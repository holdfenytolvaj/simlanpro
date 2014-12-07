package subtitle;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import subtitle.pig.udf.WordNetLemmatizer;
import util.UtilProperties;
import edu.mit.jwi.IRAMDictionary;
import edu.mit.jwi.RAMDictionary;
import edu.mit.jwi.data.ILoadPolicy;
import edu.mit.jwi.item.IIndexWord;
import edu.mit.jwi.item.IWord;
import edu.mit.jwi.item.IWordID;
import edu.mit.jwi.item.POS;
import edu.mit.jwi.morph.WordnetStemmer;
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

@RunWith(JUnit4.class)
public class LemmalizerTest {

    //@Ignore
    @Test
    public void testSimpleLemma() throws IOException {
        //String text = "ONE SECOND 'cause occasionally the strangest tallest and strongest better, worse bunny has tryed to jump, but he'd jump again hey dan, what are you doing? Lets meet at the cinema. Fox, Geza and Jim come with me to the kitchen!";
        String text = "ONE SECOND 'CAUSE OCCASIONALLY THE STRANGEST TALLEST AND STRONGEST BETTER WORSE BUNNY HAS TRYED TO JUMP, BUT HE'D JUMP AGAIN HEY DAN, WHAT ARE YOU DOIN'? LETS MEET AT THE CINEMA. FOX GEZA AND  JIM COME WITH ME TO THE KITCHEN TO SEE MYSELF! ITS MINE!";

        //--- wordnet dictionary -------------------------------------------
        WordNetLemmatizer wnl = new WordNetLemmatizer();

        //--- Stanford CoreNLP ---------------------------------------------- 
        Properties props = new Properties();
        props.put("annotators", "tokenize, ssplit, pos, lemma, ner");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        Annotation document = new Annotation(text);
        pipeline.annotate(document);
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);

        for (CoreMap sentence : sentences) {
            // traversing the words in the current sentence
            // a CoreLabel is a CoreMap with additional token-specific methods
            for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
                // this is the text of the token
                String word = token.get(TextAnnotation.class);
                // this is the NER label of the token
                String ne = token.get(NamedEntityTagAnnotation.class);
                String pos = token.get(PartOfSpeechAnnotation.class);
                String lemma = token.get(LemmaAnnotation.class);

                String lemma2 = wnl.getLemmaByStandfordPos(word, pos);

                System.out.println(word + " > " + ne + " > " + lemma + " > " + lemma2 + " > " + pos);
            }
        }
    }

    @Ignore
    @Test
    public void testSimpleLemmaWordnet() throws IOException {
        String text = "ONE SECOND 'cause the strangest tallest and strongest better, worse bunny has tryed to jump, but he'd jump again hey dan, what are you doing? Lets meet at the cinema. Fox, Geza and Jim come with me to the kitchen!";

        IRAMDictionary dict = new RAMDictionary(new File(UtilProperties.get("wordnet.dictionary")), ILoadPolicy.NO_LOAD);
        dict.open();

        WordnetStemmer stemmer = new WordnetStemmer(dict);

        String[] sentences = text.split("\\.");
        for (String sentence : sentences) {
            String[] words = sentence.split(" ");
            for (String word : words) {
                if (word.isEmpty())
                    continue;

                for (POS pos : POS.values()) {
                    System.out.println(word + " " + pos);
                    List<String> stemList = stemmer.findStems(word, null);
                    if (stemList.size() > 0) {
                        System.out.println(Arrays.deepToString(stemList.toArray()));
                        IIndexWord idxWord = dict.getIndexWord(stemList.get(0), pos);
                        if (idxWord != null && idxWord.getWordIDs() != null && idxWord.getWordIDs().size() > 0) {
                            IWordID wordID = idxWord.getWordIDs().get(0);
                            IWord wnword = dict.getWord(wordID);
                            System.out.println("Id = " + wordID);
                            System.out.println(" pos = " + pos);
                            System.out.println(" Lemma = " + wnword.getLemma());
                            System.out.println(" Gloss = " + wnword.getSynset().getGloss());
                            //break;
                        }
                    }
                }
            }
        }
    }
}
