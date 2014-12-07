package subtitle.pig.udf;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class GetLanguageLevelOfTextEnByText extends EvalFunc<Tuple> implements Callbackable {
    private Tuple output;
    private TupleFactory tf = TupleFactory.getInstance();
    private Integer[] numberOfWordsPerLevel;
    private Integer[] numberOfUniqueWordsPerLevel;
    private Set<String> uniqWords;
    private Integer wordCount;
    private NlpProcessor nlpProcessor = new NlpProcessor();

    public GetLanguageLevelOfTextEnByText() {
        super();
    }

    @Override
    public Tuple exec(Tuple input) throws IOException {
        numberOfWordsPerLevel = new Integer[] { 0, 0, 0, 0, 0, 0, 0 };
        numberOfUniqueWordsPerLevel = new Integer[] { 0, 0, 0, 0, 0, 0, 0 };
        uniqWords = new HashSet<>();
        wordCount = 0;

        if (input == null || input.size() == 0) {
            return null;
        }
        nlpProcessor.process(this, ((String) input.get(1)));

        output = tf.newTuple();
        output.append(input.get(0)); //id

        for (Integer numOfWordsPerLevel : numberOfWordsPerLevel) {
            output.append(numOfWordsPerLevel);
        }

        for (Integer numOfWordsPerLevel : numberOfUniqueWordsPerLevel) {
            output.append(numOfWordsPerLevel);
        }

        output.append(wordCount);

        return output;
    }

    @Override
    public void callBackByWord(String word, String pos, String lemma, Integer levelOfWord) {
        numberOfWordsPerLevel[levelOfWord]++;

        String key = lemma; //should be + pos
        if (uniqWords.add(key)) {
            numberOfUniqueWordsPerLevel[levelOfWord]++;
        }

        wordCount++;
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
            Schema tupleSchema = new Schema();
            tupleSchema.add(new FieldSchema("id", DataType.CHARARRAY));

            tupleSchema.add(new FieldSchema("countLevel0", DataType.INTEGER));
            tupleSchema.add(new FieldSchema("countLevel1", DataType.INTEGER));
            tupleSchema.add(new FieldSchema("countLevel2", DataType.INTEGER));
            tupleSchema.add(new FieldSchema("countLevel3", DataType.INTEGER));
            tupleSchema.add(new FieldSchema("countLevel4", DataType.INTEGER));
            tupleSchema.add(new FieldSchema("countLevel5", DataType.INTEGER));
            tupleSchema.add(new FieldSchema("countRest", DataType.INTEGER));

            tupleSchema.add(new FieldSchema("distinctCountLevel0", DataType.INTEGER));
            tupleSchema.add(new FieldSchema("distinctCountLevel1", DataType.INTEGER));
            tupleSchema.add(new FieldSchema("distinctCountLevel2", DataType.INTEGER));
            tupleSchema.add(new FieldSchema("distinctCountLevel3", DataType.INTEGER));
            tupleSchema.add(new FieldSchema("distinctCountLevel4", DataType.INTEGER));
            tupleSchema.add(new FieldSchema("distinctCountLevel5", DataType.INTEGER));
            tupleSchema.add(new FieldSchema("distinctCountRest", DataType.INTEGER));

            tupleSchema.add(new FieldSchema("wordcount", DataType.INTEGER));

            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
                tupleSchema, DataType.TUPLE));

        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public void callBackEndOfSentence() {
        // dont use
    }

}
