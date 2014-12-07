package subtitle.standalone;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import subtitle.pig.udf.GetLanguageLevelOfTextEnByText;
import util.UtilProperties;

public class Standalone {
    public static void main(String[] args) {
        List<File> fileList = getFileList(UtilProperties.get("path.subtitles"));
        System.out.println("Loaded " + fileList.size() + " subtitles");

        int poolSize = 4;
        ExecutorService executor = Executors.newFixedThreadPool(poolSize);
        List<ConcurrentHashMap<String, AtomicInteger>> wordFrequency = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            wordFrequency.add(new ConcurrentHashMap<String, AtomicInteger>());
        }

        for (int i = 0; i < poolSize; i++) {
            Runnable worker = new WorkerThread(i, poolSize, fileList, wordFrequency);
            executor.execute(worker);
        }
        executor.shutdown();
        while (!executor.isTerminated()) {
            try {
                Thread.sleep(60l * 1000);
            } catch (InterruptedException e) {
            }
        }
        System.out.println("Finished all threads");
    }

    public static class WorkerThread implements Runnable {

        private List<File> fileList;
        private EvalFunc<Tuple> analyzer;
        private int poolSize;
        private int id;
        private CompressionCodec codec;
        private TupleFactory tf = TupleFactory.getInstance();
        private List<ConcurrentHashMap<String, AtomicInteger>> wordFrequency;
        private final String outputType;

        public WorkerThread(int id, int poolSize, List<File> fileList, List<ConcurrentHashMap<String, AtomicInteger>> wordFrequency) {
            this.fileList = fileList;
            this.poolSize = poolSize;
            this.id = id;
            this.wordFrequency = wordFrequency;
            analyzer = new GetLanguageLevelOfTextEnByText();
            outputType = "TEXT";

            codec = new CompressionCodecFactory(new Configuration()).getCodecByName("GzipCodec");
        }

        @Override
        public void run() {
            for (int counter = id; counter < fileList.size(); counter += poolSize) {
                try {
                    File f = fileList.get(counter);

                    BufferedReader br = new BufferedReader(new InputStreamReader(codec.createInputStream(new FileInputStream(f))));
                    String filmName = f.getName().replaceAll("\\.(bz2|gz)$", "");
                    Tuple input = tf.newTuple();
                    input.append(filmName);
                    input.append(getText(br));
                    Tuple output = analyzer.exec(input);

                    switch (outputType) {
                    case "TEXT":
                        printOutputForText(filmName, output);
                        break;
                    case "SENTENCE":
                        printOutputForSentence(filmName, output);
                        break;
                    case "WORD":
                        printOutputForWord(counter, output);
                        break;

                    default:
                        throw new NotImplementedException();
                    }

                } catch (IOException e) {
                    System.out.println(e);
                } catch (InterruptedException e) {
                    System.out.println(e);
                }
            }
        }

        private void printOutputForText(String filmName, Tuple output) throws ExecException {
            System.out.println("[RESULT](" + filmName + ", " +
                output.get(0) + ", " + //id

                output.get(1) + ", " + //countLevel0
                output.get(2) + ", " +
                output.get(3) + ", " +
                output.get(4) + ", " +
                output.get(5) + ", " +
                output.get(6) + ", " +
                output.get(7) + ", " + //countRest

                output.get(8) + ", " + //distinctCountLevel0
                output.get(9) + ", " +
                output.get(10) + ", " +
                output.get(11) + ", " +
                output.get(12) + ", " +
                output.get(13) + ", " +
                output.get(14) + ", " + //distinctCountLevelRest

                output.get(15) + "" + //wordCount
                ")"

                );
        }

        private void printOutputForSentence(String filmName, Tuple output) throws ExecException {
            DataBag b = (DataBag) output.get(0);

            for (Tuple t : b) {
                System.out.println("[RESULT](" + filmName + ", " +
                    t.get(0) + ", " + //id
                    t.get(1) + ", " + //sentence

                    t.get(2) + ", " + //countLevel0
                    t.get(3) + ", " +
                    t.get(4) + ", " +
                    t.get(5) + ", " +
                    t.get(6) + ", " +
                    t.get(7) + ", " +
                    t.get(8) + ", " + //countRest

                    t.get(9) + //wordcount
                    ")"
                    );
            }
        }

        private void printOutputForWord(int counter, Tuple output) throws ExecException {
            DataBag b = (DataBag) output.get(0);

            for (Tuple t : b) {
                String word = (String) t.get(1);
                Integer level = (Integer) t.get(2);

                AtomicInteger prev = wordFrequency.get(level).putIfAbsent(word, new AtomicInteger(1));
                if (prev != null) {
                    prev.incrementAndGet();
                }
            }
            if (counter % 1000 == 0) {
                printWords(counter);
            }
            if (counter % 10 == 0) {
                System.out.print(".");
                if (counter % 100 == 0) {
                    System.out.print(" ");
                }
            }
        }

        private void printWords(int iteration) {
            for (int i = 0; i < wordFrequency.size(); i++) {
                for (Entry<String, AtomicInteger> e : wordFrequency.get(i).entrySet()) {
                    if (e.getValue().get() > 1) {
                        System.out.println("[FREQ][" + iteration + "][" + (i + 1) + "]" + e.getValue().get() + "," + e.getKey());
                    }
                }
            }
        }

        public String getText(BufferedReader br) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line + " ");
            }
            return sb.toString().replaceAll("\uFEFF?", "")
                .replaceAll("(\\s*\\d+\\s*)?\\d\\d:\\d\\d:\\d\\d([,\\.]\\d+)?\\s*-->\\s*\\d\\d:\\d\\d:\\d\\d([,\\.]\\d+)?", " ") //srt
                .replaceAll("\\s*\\{\\d+\\}\\{\\d+\\}\\s*", " "); //sub
        }
    }

    private static List<File> getFileList(String path) {
        List<File> fileList = new ArrayList<File>(262144);
        File folder = new File(path);
        for (File fileEntry : folder.listFiles()) {
            if (!fileEntry.isDirectory()) {
                fileList.add(fileEntry);
            }
        }
        return fileList;
    }
}
