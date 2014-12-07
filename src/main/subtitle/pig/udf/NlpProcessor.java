package subtitle.pig.udf;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import dictionary.DictionaryEn;
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

public class NlpProcessor {
    private Map<String, Integer> levelByWord;
    private static Pattern number = Pattern.compile("\\d+");
    private static Pattern punctuation = Pattern.compile("\\p{Punct}*\\p{Sc}*");
    private static Pattern xmlpart = Pattern.compile("<[/]?\\s*(i|font .*|u|b)\\s*(color=[\"=#a-zA-Z]*)?\\s*>");
    private StanfordCoreNLP nlpHelper;
    private Set<String> miscWords;
    private Set<String> nameList;
    private WordNetLemmatizer wnl;
    protected int levelOfRest = 6; //could be calculated

    protected NlpProcessor() {
        try {
            levelByWord = DictionaryEn.loadWordsPerLevel();
            miscWords = DictionaryEn.loadMiscWordList();
            nameList = DictionaryEn.loadNameList();
            System.out.println("Dictionaries loaded.");

            Properties props = new Properties();
            props.put("annotators", "tokenize, ssplit, pos, lemma, ner");
            nlpHelper = new StanfordCoreNLP(props);
            System.out.println("NLP loaded.");

            wnl = new WordNetLemmatizer();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void process(Callbackable reporter, String text) throws IOException {
        //--- cleanup -------------------------
        text = text
            .replaceAll("([a-zA-z]{2,100})\\.([a-zA-Z]{2,100})", "\1 . \2")
            .replaceAll("\u00a0", " "); //non breakable space

        //--- process -------------------------
        Annotation document = new Annotation(text);
        nlpHelper.annotate(document);
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);

        for (CoreMap sentence : sentences) {
            for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
                String word = token.get(TextAnnotation.class);
                String pos = token.get(PartOfSpeechAnnotation.class);
                String ne = token.get(NamedEntityTagAnnotation.class);
                String lemma = token.get(LemmaAnnotation.class).toLowerCase();

                if (lemma.trim().isEmpty() ||
                    punctuation.matcher(lemma).matches() ||
                    number.matcher(lemma).matches() ||
                    xmlpart.matcher(lemma).matches()) {
                    continue;
                }

                String lemma2 = wnl.getLemmaByStandfordPos(word, pos);
                lemma = (lemma2 == null) ? lemma : lemma2;
                lemma = lemma.toLowerCase();

                Integer levelOfWord = levelByWord.get(lemma);
                //"PERSON".equals(ne) || "LOCATION".equals(ne) .... doesnt work completly 
                if ("NATIONALITY".equals(ne) || "NUMBER".equals(ne) || "ORDINAL".equals(ne)) {
                    levelOfWord = 0;
                }
                else if (miscWords.contains(lemma) || nameList.contains(lemma)) {
                    levelOfWord = 0;
                }

                levelOfWord = (levelOfWord != null) ? levelOfWord : levelOfRest;

                reporter.callBackByWord(word, pos, lemma, levelOfWord);
            }
            reporter.callBackEndOfSentence();
        }
    }

}