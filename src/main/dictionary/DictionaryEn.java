package dictionary;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class DictionaryEn {

	public static Map<String, Integer> loadWordsPerLevel() throws IOException {
		return loadWordsPerLevel(new int[] { 500, 1000, 2500, 5000, 8000 }, "resources/en_frequency.txt");
	}

	public static Map<String, Integer> loadWordsPerLevel(int[] numberOfWordsPerLevel, String pathToFrequencyList) throws IOException {
		Map<String, Integer> levelByWord = new HashMap<>(16384);
		try (BufferedReader br = new BufferedReader(new InputStreamReader(DictionaryEn.class.getResourceAsStream(pathToFrequencyList)))) {
			String line;

			int counter = 0;
			int level = 1;
			int numberOfWordsTillNextLevel = numberOfWordsPerLevel[level - 1];

			while ((line = br.readLine()) != null) {
				if (counter++ >= numberOfWordsTillNextLevel) {
					if (level == numberOfWordsPerLevel.length) {
						break;
					}
					numberOfWordsTillNextLevel = numberOfWordsPerLevel[level++];
				}

				String[] wordList = line.split("\t");
				for (int i = 1; i < wordList.length; i++) {
					levelByWord.put(wordList[i], level);
				}
			}
		}
		return levelByWord;
	}
}
