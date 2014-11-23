package dictionary;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

public class DictionaryEn {
	private static Set<String> misc;
	private static Set<String> names;
	private static Map<String, Integer> levelByWord;

	private static int[] numberOfWordsPerLevel = new int[] { 500, 1000, 2000, 4000, 8000 };
	private static int counter = 0;
	private static int level = 1;
	private static int numberOfWordsTillNextLevel = numberOfWordsPerLevel[0];
	private static StanfordCoreNLP pipeline;

	public static synchronized Map<String, Integer> loadWordsPerLevel() throws IOException {
		//return 
		loadMiscWordList();
		loadNameList();

		if (levelByWord == null) {
			levelByWord = new HashMap<>();
			Properties props = new Properties();
			props.put("annotators", "tokenize, ssplit, pos, lemma, ner");
			pipeline = new StanfordCoreNLP(props);

			//loadWordsPerLevelFromFrequency5000("resources/en_frequency5000.txt");
			//System.out.println(levelByWord.size());
			loadWordsPerLevel("resources/en_frequency.txt");
			System.out.println(levelByWord.size());
		}

		return levelByWord;

	}

	private static Map<String, Integer> loadWordsPerLevel(String pathToFrequencyList) throws IOException {
		try (BufferedReader br = new BufferedReader(new InputStreamReader(DictionaryEn.class.getResourceAsStream(pathToFrequencyList)))) {
			String line;

			while ((line = br.readLine()) != null) {
				String[] wordList = line.split("\t");

				//--- check whether we need to inc level ----
				if (counter >= numberOfWordsTillNextLevel) {
					if (level == numberOfWordsPerLevel.length) {
						break;
					}
					numberOfWordsTillNextLevel = numberOfWordsPerLevel[level++];
				}

				//--- add words -----------------------------
				addWordToDictionary(levelByWord, wordList[1]);
				//for (int i = 1; i < wordList.length; i++) {
				//	addWordToDictionary(levelByWord, wordList[1]);
				//}
			}
		}
		return levelByWord;
	}

	private static Map<String, Integer> loadWordsPerLevelFromFrequency5000(String pathToFrequencyList) throws IOException {
		try (BufferedReader br = new BufferedReader(new InputStreamReader(DictionaryEn.class.getResourceAsStream(pathToFrequencyList)))) {
			String line = br.readLine();//ignore first line

			while ((line = br.readLine()) != null) {
				String[] wordList = line.split("\t");

				//--- check whether we need to inc level ----
				if (counter >= numberOfWordsTillNextLevel) {
					if (level == numberOfWordsPerLevel.length) {
						break;
					}
					numberOfWordsTillNextLevel = numberOfWordsPerLevel[level++];
				}

				//--- add words -----------------------------
				addWordToDictionary(levelByWord, wordList[1]);
			}
		}
		return levelByWord;
	}

	private static void addWordToDictionary(Map<String, Integer> levelByWord, String word) {
		word = word.replaceAll("'$", "g").replace(".", "");
		if (levelByWord.containsKey(word)) {
			//no need to readd
		} else if (misc.contains(word) || names.contains(word)) {
			//ignore, these words does not increase difficulty
		} else {
			Annotation document = new Annotation(word);
			pipeline.annotate(document);
			List<CoreMap> sentences = document.get(SentencesAnnotation.class);

			for (CoreMap sentence : sentences) {
				for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
					String ne = token.get(NamedEntityTagAnnotation.class);
					String lemma = token.get(LemmaAnnotation.class);

					if (levelByWord.containsKey(lemma)) {
						//no need to readd
					} else if ("PERSON".equals(ne) || "LOCATION".equals(ne) || "NATIONALITY".equals(ne) || "NUMBER".equals(ne) || "ORDINAL".equals(ne)) {
						//ignore, these words does not increase difficulty all should be level 1			
					} else {
						counter++;
						levelByWord.put(lemma, level);
					}
				}
			}
		}
	}

	/**
	 * Miscallenaous words like "ha-ha, Uh, em, Argh"
	 */
	public static synchronized Set<String> loadMiscWordList() throws IOException {
		if (misc == null) {
			misc = loadSimpleWordList("resources/en_misc.txt");
		}

		return misc;
	}

	public static Set<String> loadNameList() throws IOException {
		if (names == null) {
			names = loadSimpleWordList("resources/en_names.txt");
		}

		return names;
	}

	private static Set<String> loadSimpleWordList(String pathToSimpleList) throws IOException {
		Set<String> wordList = new HashSet<>();

		try (BufferedReader br = new BufferedReader(new InputStreamReader(DictionaryEn.class.getResourceAsStream(pathToSimpleList)))) {
			String line = br.readLine(); //the first line is comment
			while ((line = br.readLine()) != null) {
				for (String name : line.split(" ")) {
					wordList.add(name.toLowerCase());
				}
			}
		}
		return wordList;
	}

}
