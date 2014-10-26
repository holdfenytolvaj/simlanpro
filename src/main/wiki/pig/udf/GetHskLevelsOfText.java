package wiki.pig.udf;

import static java.util.regex.Pattern.compile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import wiki.util.UtilMisc;

public class GetHskLevelsOfText extends EvalFunc<Tuple> {
	private Map<String, Integer> hskLevelByWord = new HashMap<>(8192);
	//private static final Pattern PATTERN_TO_REPLACE = compile("[\\p{Punctuation}\\p{Symbol}\\p{Number}]+");
	private static final Pattern PATTERN_TO_REPLACE = compile("([^\\p{script=Han} ]+)");

	public GetHskLevelsOfText() {
		UtilMisc.initializeDictionaries(hskLevelByWord, new HashMap<Integer, List<String>>());
	}

	@Override
	public Tuple exec(Tuple input) throws IOException {

		if (input == null || input.size() == 0) {
			return null;
		}

		TupleFactory tf = TupleFactory.getInstance();
		Tuple t = tf.newTuple();

		Integer[] numberOfWordsPerLevel = new Integer[] { 0, 0, 0, 0, 0, 0, 0 };
		int wordCount = 0;
		String text = (String) input.get(0);

		StringTokenizer itr = new StringTokenizer(PATTERN_TO_REPLACE.matcher(text).replaceAll(" "));
		while (itr.hasMoreElements()) {
			String sequenceOfWords = itr.nextToken();
			List<String> split = getAPossibleWordSplit(sequenceOfWords);
			for (String word : split) {
				Integer levelOfWord = hskLevelByWord.get(word);
				levelOfWord = (levelOfWord != null) ? levelOfWord : numberOfWordsPerLevel.length;
				numberOfWordsPerLevel[levelOfWord - 1]++;
				wordCount++;
			}
		}

		for (int i = 0; i < numberOfWordsPerLevel.length; i++) {
			t.append(numberOfWordsPerLevel[i]);
		}
		t.append(wordCount);

		return t;
	}

	/** public for easy testability*/
	public List<String> getAPossibleWordSplitForTest(String text) {
		StringTokenizer itr = new StringTokenizer(PATTERN_TO_REPLACE.matcher(text).replaceAll(" "));
		List<String> split = new ArrayList<>();
		while (itr.hasMoreElements()) {
			String sequenceOfWords = itr.nextToken();
			split.addAll(getAPossibleWordSplit(sequenceOfWords));
		}
		return split;
	}

	private List<String> getAPossibleWordSplit(String sentence) {
		Map<String, List<String>> cache = new HashMap<>();
		return getAPossibleWordSplit(sentence, cache);
	}

	private List<String> getAPossibleWordSplit(String sentence, Map<String, List<String>> cache) {

		if (cache.containsKey(sentence)) {
			return new ArrayList<String>(cache.get(sentence));
		}

		if (hskLevelByWord.get(sentence) != null) {
			List<String> l = new ArrayList<>();
			l.add(sentence);
			return l;
		}

		if (sentence.length() == 1) {
			List<String> l = new ArrayList<>();
			l.add(sentence);
			return l;
		}

		List<String> minimalWordSplit = null;

		for (int i = sentence.length() - 1; i > 0; i--) {
			if (hskLevelByWord.get(sentence.substring(0, i)) != null || (i == 1)) {
				List<String> ws = getAPossibleWordSplit(sentence.substring(i, sentence.length()), cache);
				if (ws != null) {
					ws.add(sentence.substring(0, i));
					if (minimalWordSplit == null || minimalWordSplit.size() > ws.size()) {
						minimalWordSplit = ws;
					}
					else if (minimalWordSplit.size() == ws.size() &&
							countOfNonDictionaryWords(minimalWordSplit) > countOfNonDictionaryWords(ws)) {
						minimalWordSplit = ws;
					}
				}
			}
		}
		if (minimalWordSplit != null) {
			cache.put(sentence, minimalWordSplit);
		}

		return new ArrayList<>(minimalWordSplit);
	}

	private int countOfNonDictionaryWords(List<String> wordList) {
		int counter = 0;
		for (String word : wordList) {
			if (hskLevelByWord.get(word) == null) {
				counter++;
			}
		}
		return counter;
	}

	@Override
	public Schema outputSchema(Schema input) {
		try {
			Schema tupleSchema = new Schema();
			//tupleSchema.add(new FieldSchema("sentence", DataType.CHARARRAY));
			tupleSchema.add(new FieldSchema("hsk1", DataType.INTEGER));
			tupleSchema.add(new FieldSchema("hsk2", DataType.INTEGER));
			tupleSchema.add(new FieldSchema("hsk3", DataType.INTEGER));
			tupleSchema.add(new FieldSchema("hsk4", DataType.INTEGER));
			tupleSchema.add(new FieldSchema("hsk5", DataType.INTEGER));
			tupleSchema.add(new FieldSchema("hsk6", DataType.INTEGER));
			tupleSchema.add(new FieldSchema("nonHsk", DataType.INTEGER));
			tupleSchema.add(new FieldSchema("wordcount", DataType.INTEGER));

			return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
					tupleSchema, DataType.TUPLE));

		} catch (Exception e) {
			return null;
		}
	}

}