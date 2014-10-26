package wiki.pig.udf;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.commons.lang.NotImplementedException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class GetSentencesFromChineseText extends EvalFunc<Tuple> {

	@Override
	public Tuple exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return null;
		}

		TupleFactory tf = TupleFactory.getInstance();
		DataBag bag = BagFactory.getInstance().newDefaultBag();
		String xmlWikiPage = (String) input.get(0);
		for (String sentence : new StringTokenizerWithPunctuation(xmlWikiPage)) {
			bag.add(tf.newTuple(sentence));
		}

		Tuple t = tf.newTuple();
		t.append(bag);
		return t;
	}

	@Override
	public Schema outputSchema(Schema input) {
		try {
			Schema tupleSchema = new Schema();
			tupleSchema.add(new FieldSchema("sentences", DataType.BAG));

			return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
					tupleSchema, DataType.TUPLE));

		} catch (Exception e) {
			return null;
		}
	}

	private class StringTokenizerWithPunctuation implements Iterable<String> {
		private final StringTokenizer st;
		private final String deliminators = "\u3002\uFF1F\uFF01"; //.?!

		public StringTokenizerWithPunctuation(String text) {
			st = new StringTokenizer(text, deliminators, true);
		}

		@Override
		public Iterator<String> iterator() {
			return new Iterator<String>() {
				@Override
				public boolean hasNext() {
					return st.hasMoreElements();
				}

				@Override
				public String next() {
					String s = st.nextToken();
					if (s.length() == 1 && deliminators.contains(s)) {
						return s;
					}
					return s + (st.hasMoreTokens() ? st.nextToken() : "");
				}

				@Override
				public void remove() {
					throw new NotImplementedException();
				}
			};
		}
	}

}
