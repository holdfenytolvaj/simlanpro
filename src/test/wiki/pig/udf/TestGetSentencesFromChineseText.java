package wiki.pig.udf;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestGetSentencesFromChineseText {

	private static final EvalFunc<Tuple> xmlParser = new GetSentencesFromChineseText();
	private static Tuple testTuple = TupleFactory.getInstance().newTuple(1);

	/**
	 * GIVEN: sentences with different punctuation (为什么？等。谢谢！)
	 * WHEN: parsed
	 * THEN: the returning bag should contain the 3 sentences with the correct punctuation 
	 */
	@Test
	public void sentencesShouldBeSplitByChinesePunctuation() throws IOException {
		Set<String> sentences = new HashSet<>(Arrays.asList(new String[] { "\u4E3A\u4E48\u4EC0\uFF1F", "\u7B49\u3002", "\u8C22\u8C22\uFF01" }));
		testTuple.set(0, StringUtils.join(sentences, ""));
		Tuple result = xmlParser.exec(testTuple);
		DataBag bag = (DataBag) result.get(0);

		for (Tuple sentenceInATuple : bag) {
			if (!sentences.contains(sentenceInATuple.get(0))) {
				Assert.fail("Wrongly parsed sentence: " + sentenceInATuple.get(0));
			} else {
				sentences.remove(sentenceInATuple.get(0));
			}
		}

		if (sentences.size() != 0) {
			Assert.fail("One or more sentences were ignored: " + StringUtils.join(sentences, ""));
		}
	}

}
