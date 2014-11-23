package subtitle.pig.udf;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import dictionary.DictionaryEn;
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

public class GetLanguageLevelOfTextEn extends EvalFunc<Tuple> {
    private Map<String, Integer> levelByWord;
    private static Pattern number = Pattern.compile("\\d+");
    private static Pattern punctuation = Pattern.compile("\\p{Punct}*\\p{Sc}*");
    private static Pattern xmlpart = Pattern.compile("<[\\]?\\s*(i|font .*|u|b)\\s*>");
    private StanfordCoreNLP nlpHelper;
    private Set<String> miscWords;

    public enum OutputType {
        OUTPUT_BY_WORD, OUTPUT_BY_SENTENCE, OUTPUT_BY_TEXT
    };

    private OutputType outputType = OutputType.OUTPUT_BY_TEXT;

    private TupleFactory tf = TupleFactory.getInstance();

    public GetLanguageLevelOfTextEn() throws IOException {
        this(OutputType.OUTPUT_BY_TEXT);
    }

    public GetLanguageLevelOfTextEn(String outputType) throws IOException {
        this(OutputType.valueOf(outputType));
    }

    public GetLanguageLevelOfTextEn(OutputType outputType) throws IOException {
        levelByWord = DictionaryEn.loadWordsPerLevel();
        miscWords = DictionaryEn.loadMiscWordList();
        System.out.println("Dictionaries loaded.");

        Properties props = new Properties();
        props.put("annotators", "tokenize, ssplit, pos, lemma, ner");
        nlpHelper = new StanfordCoreNLP(props);
        this.outputType = outputType;
        System.out.println("NLP loaded.");
    }

    @Override
    public Tuple exec(Tuple input) throws IOException {

        //--- init ----------------------------
        Tuple output = tf.newTuple();
        if (outputType == OutputType.OUTPUT_BY_SENTENCE || outputType == OutputType.OUTPUT_BY_WORD) {
            BagFactory bf = BagFactory.getInstance();
            output.append(bf.newDefaultBag());
        }
        if (input == null || input.size() == 0) {
            return null;
        }

        Integer[] numberOfWordsPerLevel = new Integer[] { 0, 0, 0, 0, 0, 0 };
        int wordCount = 0;

        //--- cleanup -------------------------
        String text = ((String) input.get(1))
            .replaceAll("([a-zA-z])\\.([a-zA-Z])", "\1. \2");
        //.replaceAll("[\\p{Punct}\\p{Sc}]+", " ");

        //--- process -------------------------
        Annotation document = new Annotation(text);
        nlpHelper.annotate(document);
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);

        for (CoreMap sentence : sentences) {
            for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
                //String word = token.get(TextAnnotation.class);
                String ne = token.get(NamedEntityTagAnnotation.class);
                String lemma = token.get(LemmaAnnotation.class).toLowerCase();

                if (lemma.trim().isEmpty() || punctuation.matcher(lemma).matches() || number.matcher(lemma).matches() || xmlpart.matcher(lemma).matches()) {
                    continue;
                }

                Integer levelOfWord = levelByWord.get(lemma);
                if ("PERSON".equals(ne) || "LOCATION".equals(ne) || "NATIONALITY".equals(ne) || "NUMBER".equals(ne) || "ORDINAL".equals(ne)) {
                    levelOfWord = 1;
                }
                else if (miscWords.contains(lemma)) {
                    levelOfWord = 1;
                }

                levelOfWord = (levelOfWord != null) ? levelOfWord : numberOfWordsPerLevel.length;
                numberOfWordsPerLevel[levelOfWord - 1]++;
                wordCount++;

                if (outputType == OutputType.OUTPUT_BY_WORD) {
                    Tuple t = getResultTuple(input, numberOfWordsPerLevel, wordCount);
                    t.append(lemma);
                    ((DataBag) output.get(0)).add(t);

                    numberOfWordsPerLevel = new Integer[] { 0, 0, 0, 0, 0, 0 };
                    wordCount = 0;
                }
            }
            if (outputType == OutputType.OUTPUT_BY_SENTENCE) {
                Tuple t = getResultTuple(input, numberOfWordsPerLevel, wordCount);
                //t.append(sentence.get());
                ((DataBag) output.get(0)).add(t);

                numberOfWordsPerLevel = new Integer[] { 0, 0, 0, 0, 0, 0 };
                wordCount = 0;
            }
        }

        //--- prepare output ------------------
        if (outputType == OutputType.OUTPUT_BY_TEXT) {
            output = getResultTuple(input, numberOfWordsPerLevel, wordCount);
        }

        return output;
    }

    private Tuple getResultTuple(Tuple input, Integer[] numberOfWordsPerLevel, Integer wordCount) throws ExecException {
        Tuple output = tf.newTuple();
        output.append(input.get(0)); //id
        for (Integer numOfWordsPerLevel : numberOfWordsPerLevel) {
            output.append(numOfWordsPerLevel);
        }
        output.append(wordCount);
        return output;
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
            Schema tupleSchema = new Schema();
            tupleSchema.add(new FieldSchema("id", DataType.CHARARRAY));
            tupleSchema.add(new FieldSchema("l1", DataType.INTEGER));
            tupleSchema.add(new FieldSchema("l2", DataType.INTEGER));
            tupleSchema.add(new FieldSchema("l3", DataType.INTEGER));
            tupleSchema.add(new FieldSchema("l4", DataType.INTEGER));
            tupleSchema.add(new FieldSchema("l5", DataType.INTEGER));
            tupleSchema.add(new FieldSchema("rest", DataType.INTEGER));
            tupleSchema.add(new FieldSchema("wordcount", DataType.INTEGER));

            if (outputType == OutputType.OUTPUT_BY_SENTENCE || outputType == OutputType.OUTPUT_BY_WORD) {
                tupleSchema.add(new FieldSchema("content", DataType.CHARARRAY));
                Schema bagSchema = tupleSchema;

                tupleSchema = new Schema();
                tupleSchema.add(new FieldSchema("levelByContent", bagSchema, DataType.BAG));
            }

            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
                tupleSchema, DataType.TUPLE));

        } catch (Exception e) {
            return null;
        }
    }
}