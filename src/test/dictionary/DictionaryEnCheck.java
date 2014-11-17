package dictionary;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

public class DictionaryEnCheck {

	public static void main(String[] args) throws IOException {
		Map<String, Integer> dictionary = DictionaryEn.loadWordsPerLevel();
		for (Entry<String, Integer> e : dictionary.entrySet()) {
			System.out.println(e.getKey() + "->" + e.getValue());
		}
	}
}
