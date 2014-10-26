package wiki.classifier;

import static java.util.regex.Pattern.compile;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import edu.jhu.nlp.wikipedia.PageCallbackHandler;
import edu.jhu.nlp.wikipedia.WikiPage;
import edu.jhu.nlp.wikipedia.WikiTextParser;

public class WikiPageCallbackHandler implements PageCallbackHandler {
	private final BufferedWriter bw;
	private static final Pattern PATTERN_TO_REMOVE = compile("[\\p{P}\\p{S}\\d]+");
	private final Map<String, Integer> classifier;

	public WikiPageCallbackHandler(BufferedWriter bw, Map<String, Integer> classifier) throws IOException {
		this.bw = bw;
		this.classifier = classifier;
	}

	@Override
	public void process(WikiPage page) {
		Integer[] numberOfWordsPerLevel = new Integer[] { 0, 0, 0, 0, 0, 0, 0 };

		StringTokenizer itr = new StringTokenizer(PATTERN_TO_REMOVE.matcher(new WikiTextParser(page.getText()).getPlainText().toLowerCase()).replaceAll(""));
		long wordCount = 0;
		while (itr.hasMoreElements()) {
			String word = itr.nextToken();
			Integer levelOfWord = classifier.get(word);
			levelOfWord = (levelOfWord != null) ? levelOfWord : numberOfWordsPerLevel.length;
			numberOfWordsPerLevel[levelOfWord - 1]++;
			wordCount++;
		}

		try {
			bw.write(page.getTitle().replaceAll("[,\\n\\r\\t]", "").replaceAll("\\s+$", "") + ", " + wordCount + ", " + StringUtils.join(numberOfWordsPerLevel, ", ") + "\n");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
