package dictionary;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DictionaryCh {

	private static String PATH_TO_HSK = "resources/ch_hsk.txt";

	public static void initializeDictionaries(Map<String, Integer> hskLevelByWord, Map<Integer, List<String>> hskWordsByLevel) {
		initializeDictionaries(hskLevelByWord, hskWordsByLevel, PATH_TO_HSK);
	}

	public static void initializeDictionaries(Map<String, Integer> hskLevelByWord, Map<Integer, List<String>> hskWordsByLevel, String path) {

		try (BufferedReader br = new BufferedReader(new InputStreamReader(DictionaryCh.class.getResourceAsStream(path)))) {
			String line;

			while ((line = br.readLine()) != null) {
				List<String> wordsByLevel = new ArrayList<>();

				String[] words = line.split(", ");
				Integer hskLevel = Integer.parseInt(words[0]);
				for (int i = 1; i < words.length; i++) {
					Integer lowerLevelOfWord = hskLevelByWord.put(words[i], hskLevel);
					wordsByLevel.add(words[i]);
					if (lowerLevelOfWord != null) {
						hskWordsByLevel.get(lowerLevelOfWord).remove(words[i]);
					}
				}
				hskWordsByLevel.put(hskLevel, wordsByLevel);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
