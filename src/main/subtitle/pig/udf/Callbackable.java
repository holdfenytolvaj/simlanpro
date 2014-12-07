package subtitle.pig.udf;

public interface Callbackable {
    public void callBackByWord(String word, String pos, String lemma, Integer levelOfWord);

    public void callBackEndOfSentence();
}
