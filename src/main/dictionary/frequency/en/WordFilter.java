package dictionary.frequency.en;

import java.util.Set;
import java.util.regex.Pattern;

import dictionary.DictionaryEn;

public class WordFilter {
    //private static Pattern containsNumber = Pattern.compile(".*\\d+.*");
    private static Pattern isRomanNumber = Pattern.compile("(v|x|l|c|d|m|ii|iii|iv|vi|vii|viii|ix|xi|xii|xiii|xiv|xv)");
    private static Pattern oneCharacter = Pattern.compile("[^a^o^i^u]");
    private static Pattern useOnlyLatinCharacters = Pattern.compile("[a-zA-Z']*");

    private Set<String> nameList = DictionaryEn.loadNameList();
    private Set<String> miscList = DictionaryEn.loadMiscWordList();

    WordFilter() {
    }

    /**
     * only very basic filtering
     */
    public boolean isGoodWord(String word) {
        if (//containsNumber.matcher(word).matches() ||
        !useOnlyLatinCharacters.matcher(word).matches() ||

            isRomanNumber.matcher(word).matches() ||
            oneCharacter.matcher(word).matches() ||
            nameList.contains(word.toLowerCase()) ||
            miscList.contains(word.toLowerCase())) {
            return false;
        }
        return true;
    }

}
