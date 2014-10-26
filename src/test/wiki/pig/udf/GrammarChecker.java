package wiki.pig.udf;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import wiki.util.UtilMisc;

public class GrammarChecker {
	private Map<String, Integer> hskLevelByWord = new HashMap<>(8192);
	private Map<Integer, List<String>> hskWordsByLevel = new HashMap<>();

	public static void main(String[] args) {
		new GrammarChecker();
	}

	public GrammarChecker() {
		UtilMisc.initializeDictionaries(hskLevelByWord, hskWordsByLevel);
		checkIfWordExistsWithDifferentLevelCharacter();
		checkIfWordExistsInMultipleHskLevel();
	}

	private void checkIfWordExistsWithDifferentLevelCharacter() {
		for (int currentLevel = hskWordsByLevel.size(); currentLevel > 0; currentLevel--) {
			for (String word : hskWordsByLevel.get(currentLevel)) {
				if (word.length() > 1) {
					for (int i = 0; i < word.length(); i++) {
						Integer levelOfCharacter = hskLevelByWord.get(word.subSequence(i, i + 1));
						if (levelOfCharacter != null && levelOfCharacter != currentLevel) {
							System.out.println(word + " (" + currentLevel + ") -> " + word.charAt(i) + "(" + levelOfCharacter + ")");
						}
					}
				}
			}
		}
	}

	private void checkIfWordExistsInMultipleHskLevel() {
		for (int currentLevel = hskWordsByLevel.size(); currentLevel > 0; currentLevel--) {
			for (String word : hskWordsByLevel.get(currentLevel)) {
				Integer levelOfCharacter = hskLevelByWord.get(word);
				if (levelOfCharacter != null && levelOfCharacter != currentLevel) {
					System.out.println(word + " exists in two levels: " + currentLevel + "/" + levelOfCharacter);
				}
			}
		}
	}

}
