package wiki.pig.udf;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import wiki.util.UtilMisc;

@RunWith(JUnit4.class)
public class TestGetHskLevelsOfText {

	private final Map<String, Integer> hskLevelByWord = new HashMap<>(8192);
	private final Map<Integer, List<String>> hskWordsByLevel = new HashMap<>();

	private static final GetHskLevelsOfText hskCalculator = new GetHskLevelsOfText();
	private static Tuple testTuple = TupleFactory.getInstance().newTuple(1);

	@Before
	public void setup() {
		UtilMisc.initializeDictionaries(hskLevelByWord, hskWordsByLevel);
	}

	@Test
	public void shouldHaveOneWordFromEachDifficulty() throws IOException {
		String sentence = "";
		for (int level = 1; level <= hskWordsByLevel.size(); level++) {
			sentence += hskWordsByLevel.get(level).get(0);
		}
		testTuple.set(0, sentence);
		Tuple result = hskCalculator.exec(testTuple);

		for (int level = 0; level < hskWordsByLevel.size(); level++) {
			if (((Integer) result.get(level)) != 1) {
				System.out.println("Wrongly split or ambigious sentence: " + sentence);
				Assert.fail("There should be only one word for all level! (" + sentence + ")");
			}
		}

		Integer nonHskWordcount = ((Integer) result.get(6));
		if (nonHskWordcount != 0) {
			Assert.fail("There should be no nonHsk word! (" + sentence + ")");
		}

		Integer wordCount = ((Integer) result.get(7));
		if (wordCount != hskWordsByLevel.size()) {
			Assert.fail("There should be no more words then we put in! (" + sentence + ")");
		}
	}

	@Test
	public void testAmbigiousSplitWithNonDictionarySplit() throws IOException {
		List<String> split = hskCalculator.getAPossibleWordSplitForTest("开发明对手表");
		assertTrue(split.contains("对"));
		assertTrue(split.contains("手表"));
		assertTrue(split.contains("开"));
		assertTrue(split.contains("发明"));
	}

	@Test
	public void testRandomizedSentenceConstruction() throws IOException {
		int counter = 0;
		while (counter++ <= 10) {
			int[] wordCountPerLevel = new int[] { 0, 0, 0, 0, 0, 0, 0 };
			List<String> words = new ArrayList<>();

			String sentence = buildSentence(wordCountPerLevel, words);

			testTuple.set(0, sentence);
			Tuple result = hskCalculator.exec(testTuple);

			for (int level = 0; level < hskWordsByLevel.size(); level++) {
				if (((int) result.get(level)) != wordCountPerLevel[level]) {
					if (!isRealAmbiguity(sentence, words)) {
						printSentenceWordAndSplit(sentence, words);
						Assert.fail("Test failed for level " + (level + 1));
					}
				}
			}

			Integer nonHskWordcount = ((Integer) result.get(6));
			if (nonHskWordcount != wordCountPerLevel[6]) {
				printSentenceWordAndSplit(sentence, words);
				Assert.fail("Number of non-HSK words doesnt match!");
			}

			Integer wordCount = ((Integer) result.get(7));
			int wordCountActual = 0;
			for (int i = 0; i < wordCountPerLevel.length; i++) {
				wordCountActual += wordCountPerLevel[i];
			}
			if (wordCount != wordCountActual) {
				if (!isRealAmbiguity(sentence, words)) {
					printSentenceWordAndSplit(sentence, words);
					Assert.fail("Number of words doesnt match!");
				}
			}
		}
	}

	private String buildSentence(int[] wordCountPerLevel, List<String> words) {
		int MAX_NUMBER_OF_WORDS_PER_LEVEL = 5;
		double PROBABILITY_OF_NONHSKWORDS = 0.1;
		double PROBABILITY_OF_PUNCTUATION = 0.1;

		return buildSentence(wordCountPerLevel, words, MAX_NUMBER_OF_WORDS_PER_LEVEL, PROBABILITY_OF_NONHSKWORDS, PROBABILITY_OF_PUNCTUATION);
	}

	private String buildSentence(int[] wordCountPerLevel, List<String> words, int MAX_NUMBER_OF_WORDS_PER_LEVEL, double PROBABILITY_OF_NONHSKWORDS, double PROBABILITY_OF_PUNCTUATION) {
		StringBuffer sentence = new StringBuffer();
		String nonHskWords = "\u9580\u6C55";//門 (door) 汕 (bamboo fish trap) 
		String punctuation = " \t\u3002\uFF1F\uFF01\uFF08\uFF09\uFF0C "; //.?!(),

		//--- create a list of random words --------------------------------
		for (int level = 1; level <= hskWordsByLevel.size(); level++) {
			int numberOfWords = (int) (Math.random() * MAX_NUMBER_OF_WORDS_PER_LEVEL) + 1;
			for (int i = 0; i < numberOfWords; i++) {
				words.add(UtilMisc.getRandomElement(hskWordsByLevel.get(level)));
				wordCountPerLevel[level - 1]++;

				if (Math.random() < PROBABILITY_OF_NONHSKWORDS) {
					int index = (int) (Math.random() * nonHskWords.length());
					words.add(nonHskWords.substring(index, index + 1));
					wordCountPerLevel[6]++;
				}
				if (Math.random() < PROBABILITY_OF_PUNCTUATION) {
					int index = (int) (Math.random() * punctuation.length());
					words.add(punctuation.substring(index, index + 1));
				}
			}
		}

		//--- build a sentence from the given words ------------------------
		List<String> tmp = new ArrayList<>(words);
		while (tmp.size() > 0) {
			sentence.append(UtilMisc.removeRandomElement(tmp));
		}

		return sentence.toString();
	}

	private void printSentenceWordAndSplit(String sentence, List<String> words) {
		System.out.println("Wrongly split or ambigious sentence: ");
		System.out.println(sentence);
		List<String> split = hskCalculator.getAPossibleWordSplitForTest(sentence);
		printWordListWithHskLevel(words);
		printWordListWithHskLevel(split);
		printWordListWithHskLevel(UtilMisc.getDifferenceList(words, split));
		printWordListWithHskLevel(UtilMisc.getDifferenceList(words, split));
	}

	private void printWordListWithHskLevel(List<String> wordList) {
		List<Word> sortedWords = new ArrayList<>();
		for (String word : wordList) {
			sortedWords.add(new Word(word));
		}
		Collections.sort(sortedWords);
		System.out.println(Arrays.deepToString(sortedWords.toArray()));
	}

	private class Word implements Comparable<Word> {
		final String word;
		final String hskLevel;

		Word(String word) {
			this.word = word;
			this.hskLevel = (hskLevelByWord.get(word) == null ? "N" : "" + hskLevelByWord.get(word));
		}

		@Override
		public int compareTo(Word o) {
			return (hskLevel.compareTo(o.hskLevel) != 0 ? hskLevel.compareTo(o.hskLevel) : word.compareTo(o.word));
		}

		public String toString() {
			return word + " (" + hskLevel + ")";
		}
	}

	private boolean isRealAmbiguity(String sentence, List<String> words) {
		List<String> split = hskCalculator.getAPossibleWordSplitForTest(sentence);
		return getCharCountOfDictionaryWords(UtilMisc.getDifferenceList(words, split)) == getCharCountOfDictionaryWords(UtilMisc.getDifferenceList(split, words));
	}

	private int getCharCountOfDictionaryWords(List<String> wordList) {
		int counter = 0;
		for (String word : wordList) {
			if (hskLevelByWord.get(word) != null) {
				counter += word.length();
			}
		}
		return counter;
	}
}
