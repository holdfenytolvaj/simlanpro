package wiki.classifier;

import static java.util.regex.Pattern.compile;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import wiki.pig.udf.GetHskLevelsOfText;
import edu.jhu.nlp.wikipedia.PageCallbackHandler;
import edu.jhu.nlp.wikipedia.WikiPage;
import edu.jhu.nlp.wikipedia.WikiTextParser;

/** 
 * Very naive approach to calculate the difficulty level of a wikipedia page.
 * Works with Chinese language on word level.
 */
public class WikiPageCallbackHandlerZhPerWord implements PageCallbackHandler {
	private final BufferedWriter bw;
	private final Map<String, Integer> dictionary;
	private static final Pattern PATTERN_TO_REPLACE = compile("([^\\p{script=Han} ]+)");

	public WikiPageCallbackHandlerZhPerWord(BufferedWriter bw, Map<String, Integer> classifier) throws IOException {
		this.bw = bw;
		this.dictionary = classifier;
	}

	@Override
	public void process(WikiPage page) {
		Integer[] numberOfWordsPerLevel = new Integer[] { 0, 0, 0, 0, 0, 0, 0 };

		GetHskLevelsOfText ghlot = new GetHskLevelsOfText();
		StringTokenizer itr = new StringTokenizer(PATTERN_TO_REPLACE.matcher(new WikiTextParser(page.getText()).getPlainText().toLowerCase()).replaceAll(" "));
		long wordCount = 0;
		while (itr.hasMoreElements()) {
			String sentence = itr.nextToken();

			for (String word : ghlot.getAPossibleWordSplitForTest(sentence)) {
				Integer levelOfWord = dictionary.get(word);
				levelOfWord = (levelOfWord != null) ? levelOfWord : numberOfWordsPerLevel.length;
				numberOfWordsPerLevel[levelOfWord - 1]++;
				wordCount++;
			}
		}

		try {
			bw.write(page.getTitle().replaceAll("[,\\n\\r\\t]", "").replaceAll("\\s+$", "") + ", " + wordCount + ", " + StringUtils.join(numberOfWordsPerLevel, ", ") + "\n");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
