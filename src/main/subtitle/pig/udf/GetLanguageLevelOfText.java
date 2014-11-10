package subtitle.pig.udf;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import dictionary.DictionaryEn;

public class GetLanguageLevelOfText extends EvalFunc<Tuple> {
	private Map<String, Integer> levelByWord;
	private static Pattern number = Pattern.compile("\\d+");

	public GetLanguageLevelOfText() throws IOException {
		levelByWord = DictionaryEn.loadWordsPerLevel();
	}

	@Override
	public Tuple exec(Tuple input) throws IOException {

		//--- init ----------------------------
		if (input == null || input.size() == 0) {
			return null;
		}

		Integer[] numberOfWordsPerLevel = new Integer[] { 0, 0, 0, 0, 0, 0 };
		int wordCount = 0;

		//--- cleanup -------------------------
		String text = ((String) input.get(1))
				.replaceAll("'d", " would")
				.replaceAll("'s", " has") //could be is as well... but doesn't matter for us at the moment
				.replaceAll("'m", " am")
				.replaceAll("n't", " not")
				.replaceAll("in' ", "ing")
				.replaceAll("[\\p{Punct}\\p{Sc}]+", " ");

		//--- process -------------------------
		StringTokenizer itr = new StringTokenizer(text);
		while (itr.hasMoreElements()) {
			String word = itr.nextToken().trim().toLowerCase();
			if (word.isEmpty()) {
				continue;
			}

			Integer levelOfWord = levelByWord.get(word);
			if (number.matcher(word).matches()) {
				levelOfWord = 1;
			}
			levelOfWord = (levelOfWord != null) ? levelOfWord : numberOfWordsPerLevel.length;
			numberOfWordsPerLevel[levelOfWord - 1]++;
			wordCount++;
		}

		//--- prepare output ------------------
		TupleFactory tf = TupleFactory.getInstance();
		Tuple t = tf.newTuple();
		t.append(input.get(0)); //id
		for (Integer numOfWordsPerLevel : numberOfWordsPerLevel) {
			t.append(numOfWordsPerLevel);
		}
		t.append(wordCount);

		return t;
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

			return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
					tupleSchema, DataType.TUPLE));

		} catch (Exception e) {
			return null;
		}
	}

}