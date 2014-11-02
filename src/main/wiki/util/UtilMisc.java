package wiki.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UtilMisc {
	private static String PATH_TO_HSK = "resources/hsk.txt";

	public static void initializeDictionaries(Map<String, Integer> hskLevelByWord, Map<Integer, List<String>> hskWordsByLevel) {

		try (BufferedReader br = new BufferedReader(new InputStreamReader(UtilMisc.class.getResourceAsStream(PATH_TO_HSK)))) {
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

	public static <T> T getRandomElement(List<T> list) {
		if (list == null || list.size() == 0) {
			return null;
		}

		return list.get((int) (Math.random() * list.size()));
	}

	public static <T> T removeRandomElement(List<T> list) {
		if (list == null || list.size() == 0) {
			return null;
		}

		return list.remove((int) (Math.random() * list.size()));
	}

	/** 
	 * Similar to List.removeAll() but considers the number of occurences
	 */
	public static <T extends Comparable<T>> List<T> getDifferenceList(List<T> list, List<T> listToRemove) {
		List<T> result = new ArrayList<>();
		List<T> tmpToRemove = new ArrayList<>(listToRemove);
		for (T element : list) {
			if (tmpToRemove.contains(element)) {
				tmpToRemove.remove(element);
			} else {
				result.add(element);
			}
		}
		return result;
	}
}
