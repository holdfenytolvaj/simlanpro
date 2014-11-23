package dictionary;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

public class DictionaryEnCheck {

    public static void main(String[] args) {
        Map<String, Integer> dictionary;
        try {
            dictionary = DictionaryEn.loadWordsPerLevel();

            for (Entry<String, Integer> e : dictionary.entrySet()) {
                //System.out.println(e.getKey() + "->" + e.getValue());
            }
            System.out.println(dictionary.size());
        } catch (IOException e1) {
            throw new RuntimeException(e1);
        }
    }

}
