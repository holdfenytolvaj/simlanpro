package subtitle.hadoop.input;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.regex.Pattern;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class SrtRecordReader extends RecordReader<Text, Text> {
    //private static final Log LOGGER = LogFactory.getLog(SrtRecordReader.class);

    private FileSplit fileSplit;
    private BufferedReader br;

    private FileSystem fs;
    private TaskAttemptContext taskAttemptContext;

    private String filmName;
    int frameCounter;
    private Text currentKey;
    private Text currentValue;

    private static Pattern number = Pattern.compile("\uFEFF?\\d+");

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        fileSplit = (FileSplit) inputSplit;
        this.taskAttemptContext = taskAttemptContext;
        fs = fileSplit.getPath().getFileSystem(taskAttemptContext.getConfiguration());
        InputStream is;

        if (fileSplit.getPath().getName().endsWith(".bz2") || fileSplit.getPath().getName().endsWith(".gz")) {
            CompressionCodec codec = new CompressionCodecFactory(taskAttemptContext.getConfiguration()).getCodec(fileSplit.getPath());
            is = codec.createInputStream(fs.open(fileSplit.getPath()));
            filmName = fileSplit.getPath().getName().replaceAll("\\.(bz2|gz)$", "");
        } else if (fileSplit.getPath().getName().endsWith(".srt")) {
            is = fs.open(fileSplit.getPath());
            filmName = fileSplit.getPath().getName().replaceAll("\\.srt$", "");
        } else {
            throw new NotImplementedException("Not expected file format for " + fileSplit.getPath().getName() + " only srt are supported.");
        }

        br = new BufferedReader(new InputStreamReader(is));
        frameCounter = 1;

        //is.skip(fileSplit.getStart());
        System.out.println("0 reading file " + filmName);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (currentKey == null) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line + " ");
            }
            String text = sb.toString().replaceAll("\uFEFF?", "")
                .replaceAll("(\\s*\\d+\\s*)?\\d\\d:\\d\\d:\\d\\d([,\\.]\\d+)?\\s*-->\\s*\\d\\d:\\d\\d:\\d\\d([,\\.]\\d+)?", " ") //srt
                .replaceAll("\\s*\\{\\d+\\}\\{\\d+\\}\\s*", " "); //sub

            currentKey = new Text(filmName);
            currentValue = new Text(text);
            return true;
        } else {
            currentKey = null;
            currentValue = null;
            System.out.println("Finished file " + filmName);
            return false;
        }
    }

    //@Override
    public boolean nextKeyValuePerLine() throws IOException, InterruptedException {
        try {
            StringBuilder sb = new StringBuilder();
            String line;

            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }
                break;
            }

            if (line == null) {
                currentKey = null;
                currentValue = null;
                System.out.println("Finished file " + filmName);
                return false;
            }

            //--- frame number -------------------------------
            while (!number.matcher(line).matches()) {
                //throw new NotImplementedException("Not expected line: " + line);
                //simply ignore it instead
                System.out.println("Not expected line: " + line + " in file: " + filmName);

                line = br.readLine();
                if (line == null) {
                    currentKey = null;
                    currentValue = null;
                    System.out.println("Finished file " + filmName);
                    return false;
                }
            }
            if (frameCounter == 1) {
                line = line.replaceAll("\uFEFF?", "");//some file starts with BOM
            }

            //--- timing -------------------------------------
            br.readLine();//we can ignore this

            //--- text ---------------------------------------
            while ((line = br.readLine()) != null) {
                line = line.trim();

                if (line.isEmpty()) {
                    break;
                }
                sb.append(line + " ");
            }

            if (sb.length() == 0) {
                currentKey = null;
                currentValue = null;
                System.out.println("Finished file " + filmName);
                return false;
            }

            frameCounter++;

            currentKey = new Text(filmName);
            currentValue = new Text(sb.toString());

            //if (frameCounter % 1000 == 0) {
            //	System.out.println(filmName + " position " + frameCounter);
            //}

            return true;
        } catch (Exception e) {
            System.out.println("The file has some problem (maybe not srt?): " + filmName);
            System.out.println(e.getMessage());
            System.out.println(e.getStackTrace());
            return false;
        }
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

    @Override
    public void close() throws IOException {
        taskAttemptContext.setStatus("Closed RecordReader");
        br.close();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0; //we dont know...
    }
}
