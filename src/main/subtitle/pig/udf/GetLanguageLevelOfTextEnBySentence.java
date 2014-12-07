package subtitle.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class GetLanguageLevelOfTextEnBySentence extends EvalFunc<Tuple> implements Callbackable {
    private Tuple output;
    private TupleFactory tf = TupleFactory.getInstance();
    private BagFactory bf = BagFactory.getInstance();

    private Integer[] numberOfWordsPerLevel;
    private Integer wordCount;
    private String sentenceWithLevels;
    private String id;
    private NlpProcessor nlpProcessor = new NlpProcessor();

    public GetLanguageLevelOfTextEnBySentence() {
        super();
    }

    @Override
    public Tuple exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }

        numberOfWordsPerLevel = new Integer[] { 0, 0, 0, 0, 0, 0, 0 };
        sentenceWithLevels = "";
        wordCount = 0;
        id = (String) input.get(0);
        output = tf.newTuple();
        output.append(bf.newDefaultBag());

        nlpProcessor.process(this, ((String) input.get(1)));

        return output;
    }

    @Override
    public void callBackByWord(String word, String pos, String lemma, Integer levelOfWord) {
        numberOfWordsPerLevel[levelOfWord]++;
        sentenceWithLevels += lemma + "-[" + levelOfWord + ", " + pos + "] ";

        wordCount++;
    }

    @Override
    public void callBackEndOfSentence() {
        try {
            Tuple t = tf.newTuple();
            t.append(id);
            for (Integer numOfWordsPerLevel : numberOfWordsPerLevel) {
                output.append(numOfWordsPerLevel);
            }
            t.append(sentenceWithLevels);
            t.append(wordCount);

            ((DataBag) output.get(0)).add(t);

            //reset
            numberOfWordsPerLevel = new Integer[] { 0, 0, 0, 0, 0, 0, 0 };
            sentenceWithLevels = "";
            wordCount = 0;
        } catch (ExecException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
            Schema bagSchema = new Schema();
            bagSchema.add(new FieldSchema("id", DataType.CHARARRAY));
            bagSchema.add(new FieldSchema("sentence", DataType.CHARARRAY));
            bagSchema.add(new FieldSchema("countLevel0", DataType.INTEGER));
            bagSchema.add(new FieldSchema("countLevel1", DataType.INTEGER));
            bagSchema.add(new FieldSchema("countLevel2", DataType.INTEGER));
            bagSchema.add(new FieldSchema("countLevel3", DataType.INTEGER));
            bagSchema.add(new FieldSchema("countLevel4", DataType.INTEGER));
            bagSchema.add(new FieldSchema("countLevel5", DataType.INTEGER));
            bagSchema.add(new FieldSchema("countRest", DataType.INTEGER));
            bagSchema.add(new FieldSchema("wordcount", DataType.INTEGER));

            Schema tupleSchema = new Schema();
            tupleSchema.add(new FieldSchema("levelBySentence", bagSchema, DataType.BAG));

            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
                tupleSchema, DataType.TUPLE));

        } catch (Exception e) {
            return null;
        }
    }
}
