package dictionary.frequency.en;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;

import util.UtilProperties;

import com.google.common.collect.MinMaxPriorityQueue;

public class Transformer {
    private WordFilter filter = new WordFilter();
    private WordLemmatizer lemmatizer = new WordLemmatizer();

    /**
     * Transforms the English frequency list, downloaded from:
     * http://ucrel.lancs.ac.uk/bncfreq/
     * to a list of top 10K words with frequency and alternatives.
     * The output format will be (tab separated)
     * [frequency] [main word] [alternatives...]
     */
    public static void main(String[] args) throws FileNotFoundException {
        Transformer transformer = new Transformer();

        String pathToFrequencyList = UtilProperties.get("dictionary.frequencyList.input");
        String pathToOutput = UtilProperties.get("dictionary.frequencyList.output");
        transformer.save(transformer.load(pathToFrequencyList), pathToOutput);
    }

    private MinMaxPriorityQueue<WordWithFrequency> load(String input) throws FileNotFoundException {

        try (Scanner sc = new Scanner(new FileReader(input))) {
            MinMaxPriorityQueue<WordWithFrequency> top10K = MinMaxPriorityQueue.maximumSize(10000).create();

            sc.nextLine();
            String word = sc.next();
            String pos = sc.next();
            String wordVariantOrMark = sc.next();
            Integer frequency = sc.nextInt();

            /* 
             * Example records:
             *  analyzable  Adj :   0   2   0.26
             *  analyzer    NoC %   0   4   0.38
             *   @   @   analyzer    0   4   0.43
             *   @   @   analyzers   0   1   0.00
             */
            while (sc.hasNext()) {
                WordWithFrequency wwf = new WordWithFrequency(word, frequency, pos);
                switch (wordVariantOrMark) {
                case "%":
                    while (true) {
                        //read next
                        sc.nextLine();
                        if (sc.hasNext()) {
                            word = sc.next();
                            pos = sc.next();
                            wordVariantOrMark = sc.next();
                            frequency = sc.nextInt();
                        }
                        if ("@".equals(word)) {
                            wwf.addAlternative(wordVariantOrMark);
                        } else {
                            break;
                        }
                    }

                    break;
                case ":":
                    //read next
                    sc.nextLine();
                    if (sc.hasNext()) {
                        word = sc.next();
                        pos = sc.next();
                        wordVariantOrMark = sc.next();
                        frequency = sc.nextInt();
                    }
                    break;
                default:
                    throw new NotImplementedException();
                }

                if (wwf.isGood) {
                    top10K.add(wwf);
                }
            }
            return top10K;
        }
    }

    private void save(MinMaxPriorityQueue<WordWithFrequency> wordList, String output) throws FileNotFoundException {
        try (PrintWriter pw = new PrintWriter(output)) {
            while (!wordList.isEmpty()) {
                pw.println(wordList.pollFirst());
            }
            pw.flush();
        }
    }

    class WordWithFrequency implements Comparable<WordWithFrequency> {
        Set<String> wordListOfVariants = new HashSet<>();
        final Integer frequency;
        final boolean isGood;
        final String pos;

        WordWithFrequency(String word, Integer frequency, String pos) {
            this.pos = pos;
            isGood = filter.isGoodWord(word);
            String lemma = (!isGood ? "" : lemmatizer.getLemma(word.replace(".", ""), pos).toLowerCase());
            this.frequency = frequency;
            wordListOfVariants.add(lemma);
        }

        void addAlternative(String alternativeWord) {
            if (isGood) {
                String lemma = lemmatizer.getLemma(alternativeWord.replace(".", ""), pos).toLowerCase();
                wordListOfVariants.add(lemma);
            }
        }

        @Override
        public int compareTo(WordWithFrequency o) {
            return o.frequency.compareTo(this.frequency);
        }

        @Override
        public String toString() {
            return "" + frequency + "\t" + StringUtils.join(wordListOfVariants.toArray(), "\t");
        }
    }

}
