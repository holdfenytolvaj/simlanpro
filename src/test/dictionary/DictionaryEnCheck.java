package dictionary;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

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

public class DictionaryEnCheck {

    public static void main(String[] args) {
        Map<String, Integer> dictionary;
        try {
            dictionary = DictionaryEn.loadWordsPerLevel();

            //--- wordnet dictionary -------------------------------------------
            IRAMDictionary dict = new RAMDictionary(new File(UtilProperties.get("wordnet.dictionary")), ILoadPolicy.NO_LOAD);
            dict.open();
            WordnetStemmer stemmer = new WordnetStemmer(dict);

            //--- Stanford CoreNLP ---------------------------------------------- 
            Properties props = new Properties();
            props.put("annotators", "tokenize, ssplit, pos, lemma, ner");
            StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

            for (Entry<String, Integer> e : dictionary.entrySet()) {
                Annotation document = new Annotation(e.getKey());
                pipeline.annotate(document);
                List<CoreMap> sentences = document.get(SentencesAnnotation.class);

                for (CoreMap sentence : sentences) {
                    for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
                        String word = token.get(TextAnnotation.class);
                        String ner = token.get(NamedEntityTagAnnotation.class);
                        String pos = token.get(PartOfSpeechAnnotation.class);
                        String lemma = token.get(LemmaAnnotation.class);

                        String lemma2 = null;
                        for (POS posType : POS.values()) {
                            List<String> stemList = stemmer.findStems(word, posType);
                            if (stemList.size() > 0) {
                                IIndexWord idxWord = dict.getIndexWord(stemList.get(0), posType);
                                if (idxWord != null) {
                                    IWordID wordID = idxWord.getWordIDs().get(0);
                                    IWord wnword = dict.getWord(wordID);
                                    lemma2 = wnword.getLemma().toLowerCase();
                                    break;
                                }
                            }
                        }

                        if (lemma2 != null && !lemma.equals(lemma2)) {
                            System.out.println(word + " > " + lemma + " > " + lemma2 + " < " + pos);
                        }
                    }
                }

            }
            System.out.println(dictionary.size());
        } catch (IOException e1) {
            throw new RuntimeException(e1);
        }
    }

}
