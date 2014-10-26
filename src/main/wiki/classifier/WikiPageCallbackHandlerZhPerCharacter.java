package wiki.classifier;

import static java.util.regex.Pattern.compile;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import edu.jhu.nlp.wikipedia.PageCallbackHandler;
import edu.jhu.nlp.wikipedia.WikiPage;
import edu.jhu.nlp.wikipedia.WikiTextParser;

public class WikiPageCallbackHandlerZhPerCharacter implements PageCallbackHandler {
	private final BufferedWriter bw;
	private static final Pattern PATTERN_TO_REMOVE = compile("[\\p{P}\\p{S}\\d]+");
	private static final Pattern LATIN_CHARACTERS = compile("[a-zA-Z]+");
	private final Map<String, Integer> dictionary;

	public WikiPageCallbackHandlerZhPerCharacter(BufferedWriter bw, Map<String, Integer> classifier) throws IOException {
		this.bw = bw;
		this.dictionary = classifier;
	}

	@Override
	public void process(WikiPage page) {
		Integer[] numberOfWordsPerLevel = new Integer[] { 0, 0, 0, 0, 0, 0, 0 };

		StringTokenizer itr = new StringTokenizer(PATTERN_TO_REMOVE.matcher(new WikiTextParser(page.getText()).getPlainText().toLowerCase()).replaceAll(""));
		long wordCount = 0;
		while (itr.hasMoreElements()) {
			String sentence = itr.nextToken();

			for (String word : getWordSplit(sentence)) {
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

	private List<String> getWordSplit(String sentence) {
		Map<String, List<String>> cache = new HashMap<>();
		List<String> wordSplit = getWordSplit(sentence, cache, false);
		if (wordSplit == null) {
			//if it is an english word leave it as it is
			if (LATIN_CHARACTERS.matcher(sentence).matches()) {
				return Arrays.asList(sentence);
			}

			//try to split still
			cache.clear();
			wordSplit = getWordSplit(sentence, cache, true);
		}
		return wordSplit;
	}

	private List<String> getWordSplit(String sentence, Map<String, List<String>> cache, boolean acceptNonDictionaryCharacter) {

		if (cache.containsKey(sentence)) {
			return cache.get(sentence);
		}

		//we dont cache single word
		if (dictionary.get(sentence) != null) {
			List<String> l = new ArrayList<>();
			l.add(sentence);
			return l;
		}

		if (sentence.length() == 1) {
			if (acceptNonDictionaryCharacter) {
				List<String> l = new ArrayList<>();
				l.add(sentence);
				return l;
			}
			return null;
		}

		List<String> minimalWordSplit = null;

		for (int i = sentence.length() - 1; i > 0; i--) {
			if (dictionary.get(sentence.substring(0, i)) != null || (i == 1 && acceptNonDictionaryCharacter)) {
				List<String> ws = getWordSplit(sentence.substring(i, sentence.length()), cache, acceptNonDictionaryCharacter);
				if (ws != null) {
					ws.add(sentence.substring(0, i));
					if (minimalWordSplit == null || minimalWordSplit.size() > ws.size()) {
						minimalWordSplit = ws;
					}
				}
			}
		}
		cache.put(sentence, minimalWordSplit);

		return minimalWordSplit;
	}

}
