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

public class GetLanguageLevelOfTextEnByWord extends EvalFunc<Tuple> implements Callbackable {
    private String id;
    private Tuple output;
    private TupleFactory tf = TupleFactory.getInstance();
    private BagFactory bf = BagFactory.getInstance();
    private NlpProcessor nlpProcessor = new NlpProcessor();

    public GetLanguageLevelOfTextEnByWord() {
        super();
    }

    @Override
    public Tuple exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        id = (String) input.get(0);
        output = tf.newTuple();
        output.append(bf.newDefaultBag());

        nlpProcessor.process(this, ((String) input.get(1)));

        return output;
    }

    @Override
    public void callBackByWord(String word, String pos, String lemma, Integer levelOfWord) {
        try {
            Tuple t = tf.newTuple();
            t.append(id);
            t.append(lemma);
            t.append(levelOfWord);

            ((DataBag) output.get(0)).add(t);
        } catch (ExecException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
            Schema bagSchema = new Schema();
            bagSchema.add(new FieldSchema("id", DataType.CHARARRAY));
            bagSchema.add(new FieldSchema("word", DataType.CHARARRAY));
            bagSchema.add(new FieldSchema("level", DataType.INTEGER));

            Schema tupleSchema = new Schema();
            tupleSchema.add(new FieldSchema("levelByWord", bagSchema, DataType.BAG));

            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
                tupleSchema, DataType.TUPLE));

        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public void callBackEndOfSentence() {
        // not used
    }
}
