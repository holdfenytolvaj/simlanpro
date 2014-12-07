package dictionary.frequency.en;

import java.io.File;
import java.io.IOException;
import java.util.List;
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
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

public class WordLemmatizer {
    IRAMDictionary dict;
    WordnetStemmer stemmer;
    StanfordCoreNLP pipeline;

    WordLemmatizer() {
        //--- wordnet dictionary -------------------------------------------
        dict = new RAMDictionary(new File(UtilProperties.get("wordnet.dictionary")), ILoadPolicy.NO_LOAD);
        try {
            dict.open();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        stemmer = new WordnetStemmer(dict);

        //--- Stanford CoreNLP ---------------------------------------------- 
        Properties props = new Properties();
        props.put("annotators", "tokenize, ssplit, pos, lemma, ner");
        pipeline = new StanfordCoreNLP(props);
    }

    public String getLemma(String word, String pos) {
        String lemmaStandford = null;
        String lemmaWordnet = null;

        Annotation document = new Annotation(word);
        pipeline.annotate(document);
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);

        for (CoreMap sentence : sentences) {
            for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
                //String pos = token.get(PartOfSpeechAnnotation.class);
                lemmaStandford = token.get(LemmaAnnotation.class);

                POS posType = null;
                switch (pos) {
                case "Adj":
                    posType = POS.ADJECTIVE;
                    break;
                case "Verb":
                    posType = POS.VERB;
                    break;
                case "NoC":
                    posType = POS.NOUN;
                    break;
                case "Adv":
                    posType = POS.ADVERB;
                    break;
                }

                if (posType != null) {
                    List<String> stemList = stemmer.findStems(word, posType);
                    if (stemList.size() > 0) {
                        IIndexWord idxWord = dict.getIndexWord(stemList.get(0), posType);
                        if (idxWord != null) {
                            IWordID wordID = idxWord.getWordIDs().get(0);
                            IWord wnword = dict.getWord(wordID);
                            lemmaWordnet = wnword.getLemma().toLowerCase();
                            break;
                        }
                    }
                }

                if (lemmaWordnet != null && !lemmaStandford.equals(lemmaWordnet)) {
                    System.out.println(word + " > " + lemmaStandford + " > " + lemmaWordnet + " < " + pos);
                }
            }
        }

        return (lemmaWordnet != null && "'".equals(lemmaWordnet) ? lemmaWordnet : (lemmaStandford != null ? lemmaStandford : word));
    }

}
