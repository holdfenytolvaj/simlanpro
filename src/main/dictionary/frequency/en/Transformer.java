package dictionary.frequency.en;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;

import com.google.common.collect.MinMaxPriorityQueue;

public class Transformer {

	/**
	 * Transforms the English frequency list, downloaded from:
	 * http://ucrel.lancs.ac.uk/bncfreq/
	 * to a list of top 10K words with frequency and alternatives.
	 * The output format will be (tab separated)
	 * [frequency] [main word] [alternatives...]
	 */
	public static void main(String[] args) throws FileNotFoundException {
		Transformer transformer = new Transformer();
		transformer.save(transformer.load(args[0]), args[1]);
	}

	private MinMaxPriorityQueue<WordWithFrequency> load(String input) throws FileNotFoundException {

		try (Scanner sc = new Scanner(new FileReader(input))) {
			MinMaxPriorityQueue<WordWithFrequency> top10K = MinMaxPriorityQueue.maximumSize(10000).create();

			sc.nextLine();
			String word = sc.next();
			sc.next(); //word type (not used)
			String wordVariantOrMark = sc.next();
			Integer frequency = sc.nextInt();

			while (sc.hasNext()) {
				WordWithFrequency wwf = new WordWithFrequency(word, frequency);
				switch (wordVariantOrMark) {
				case "%":
					while (true) {
						//read next
						sc.nextLine();
						if (sc.hasNext()) {
							word = sc.next();
							sc.next(); //word type (not used)
							wordVariantOrMark = sc.next();
							frequency = sc.nextInt();
						}
						if ("@".equals(word)) {
							wwf.addAlternative(wordVariantOrMark);
						} else {
							break;
						}
					}

					break;
				case ":":
					//read next
					sc.nextLine();
					if (sc.hasNext()) {
						word = sc.next();
						sc.next(); //word type (not used)
						wordVariantOrMark = sc.next();
						frequency = sc.nextInt();
					}
					break;
				default:
					throw new NotImplementedException();
				}

				if (wwf.isGood) {
					top10K.add(wwf);
				}
			}
			return top10K;
		}
	}

	private void save(MinMaxPriorityQueue<WordWithFrequency> wordList, String output) throws FileNotFoundException {
		try (PrintWriter pw = new PrintWriter(output)) {
			while (!wordList.isEmpty()) {
				pw.println(wordList.pollFirst());
			}
			pw.flush();
		}
	}

	static class WordWithFrequency implements Comparable<WordWithFrequency> {
		static Pattern containsNumber = Pattern.compile(".*\\d+.*");
		static Pattern isRomanNumber = Pattern.compile("(v|x|l|c|d|m|ii|iii|iv|vi|vii|viii|ix|xi|xii|xiii|xiv|xv)");
		static Pattern miscalenaous = Pattern.compile("([^a^o^i^u]|(.*[_&].*))");

		Set<String> wordListOfVariants = new HashSet<>();
		final Integer frequency;
		final boolean isGood;

		WordWithFrequency(String word, Integer frequency) {
			this.frequency = frequency;
			wordListOfVariants.add(word.toLowerCase());
			this.isGood = !containsNumber.matcher(word).matches() && !isRomanNumber.matcher(word).matches() && !miscalenaous.matcher(word).matches();
		}

		void addAlternative(String alternativeWord) {
			wordListOfVariants.add(alternativeWord.toLowerCase());
		}

		@Override
		public int compareTo(WordWithFrequency o) {
			return o.frequency.compareTo(this.frequency);
		}

		@Override
		public String toString() {
			return "" + frequency + "\t" + StringUtils.join(wordListOfVariants.toArray(), "\t");
		}
	}

}
