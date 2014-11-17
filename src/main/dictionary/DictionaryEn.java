package dictionary;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DictionaryEn {

	public static Map<String, Integer> loadWordsPerLevel() throws IOException {
		//return loadWordsPerLevel(new int[] { 500, 1000, 2000, 4000, 8000 }, "resources/en_frequency.txt");
		return loadWordsPerLevelFromFrequency5000(new int[] { 500, 1000, 2000, 3500, 5000 }, "resources/en_frequency5000.txt");
	}

	public static Map<String, Integer> loadWordsPerLevel(int[] numberOfWordsPerLevel, String pathToFrequencyList) throws IOException {
		Map<String, Integer> levelByWord = new HashMap<>(16384);

		try (BufferedReader br = new BufferedReader(new InputStreamReader(DictionaryEn.class.getResourceAsStream(pathToFrequencyList)))) {
			String line;

			int counter = 0;
			int level = 1;
			int numberOfWordsTillNextLevel = numberOfWordsPerLevel[level - 1];

			while ((line = br.readLine()) != null) {
				String[] wordList = line.split("\t");

				//--- check whether we need to inc level ----
				if (counter++ >= numberOfWordsTillNextLevel) {
					if (level == numberOfWordsPerLevel.length) {
						break;
					}
					numberOfWordsTillNextLevel = numberOfWordsPerLevel[level++];
				}

				//--- add words -----------------------------
				for (int i = 1; i < wordList.length; i++) {
					levelByWord.put(wordList[i], level);
				}
			}
		}
		return levelByWord;
	}

	public static Map<String, Integer> loadWordsPerLevelFromFrequency5000(int[] numberOfWordsPerLevel, String pathToFrequencyList) throws IOException {
		Map<String, Integer> levelByWord = new HashMap<>(16384);

		try (BufferedReader br = new BufferedReader(new InputStreamReader(DictionaryEn.class.getResourceAsStream(pathToFrequencyList)))) {
			String line = br.readLine();//ignore first line

			int counter = 0;
			int level = 1;
			int numberOfWordsTillNextLevel = numberOfWordsPerLevel[level - 1];

			while ((line = br.readLine()) != null) {
				String[] wordList = line.split("\t");

				//--- check whether we need to inc level ----
				if (counter++ >= numberOfWordsTillNextLevel) {
					if (level == numberOfWordsPerLevel.length) {
						break;
					}
					numberOfWordsTillNextLevel = numberOfWordsPerLevel[level++];
				}

				//--- add words -----------------------------
				if (levelByWord.put(wordList[1], level) != null) { 
					throw new IllegalStateException("The word '" + wordList[1] + "' exists multiple times!");
				}
			}
		}
		return levelByWord;
	}

	/**
	 * Miscallenaous words like "ha-ha, Uh, em, Argh"
	 */
	public static Set<String> loadMiscWordList() throws IOException {
		return loadMiscWordList("resources/en_misc.txt");
	}

	private static Set<String> loadMiscWordList(String pathToFrequencyList) throws IOException {
		Set<String> miscWords = new HashSet<>();

		try (BufferedReader br = new BufferedReader(new InputStreamReader(DictionaryEn.class.getResourceAsStream(pathToFrequencyList)))) {
			String line = br.readLine(); //the first line is comment
			while ((line = br.readLine()) != null) {
				for (String name : line.split(" ")) {
					miscWords.add(name.toLowerCase());
				}
			}
		}
		return miscWords;
	}
}
