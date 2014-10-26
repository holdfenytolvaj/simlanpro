package wiki.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import wiki.hadoop.input.WikipediaPageXmlParser;
import edu.jhu.nlp.wikipedia.WikiPage;

public class ParseXmlWikiPage extends EvalFunc<Tuple> {

	public ParseXmlWikiPage() {
	}

	@Override
	public Tuple exec(Tuple input) throws IOException {

		if (input == null || input.size() == 0) {
			return null;
		}

		String xmlWikiPage = (String) input.get(0);
		WikiPage wp = WikipediaPageXmlParser.parse(xmlWikiPage);

		TupleFactory tf = TupleFactory.getInstance();
		Tuple t = tf.newTuple();
		t.append(wp.getTitle());
		t.append(wp.getID());
		t.append(wp.getWikiText());
		t.append(wp.isRedirect() || wp.isSpecialPage() || wp.isDisambiguationPage());
		return t;
	}

	@Override
	public Schema outputSchema(Schema input) {
		try {
			Schema tupleSchema = new Schema();
			tupleSchema.add(new FieldSchema("title", DataType.CHARARRAY));
			tupleSchema.add(new FieldSchema("id", DataType.CHARARRAY));
			tupleSchema.add(new FieldSchema("text", DataType.CHARARRAY));
			tupleSchema.add(new FieldSchema("isSpecialPage", DataType.BOOLEAN));

			return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
					tupleSchema, DataType.TUPLE));

		} catch (Exception e) {
			return null;
		}
	}
}