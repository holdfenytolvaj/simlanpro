package subtitle.pig.udf;

import java.io.File;
import java.io.IOException;
import java.util.List;

import util.UtilProperties;
import edu.mit.jwi.IRAMDictionary;
import edu.mit.jwi.RAMDictionary;
import edu.mit.jwi.data.ILoadPolicy;
import edu.mit.jwi.item.IIndexWord;
import edu.mit.jwi.item.IWord;
import edu.mit.jwi.item.IWordID;
import edu.mit.jwi.item.POS;
import edu.mit.jwi.morph.WordnetStemmer;

public class WordNetLemmatizer {
    IRAMDictionary dict;
    WordnetStemmer stemmer;

    public WordNetLemmatizer() {
        dict = new RAMDictionary(new File(UtilProperties.get("wordnet.dictionary")), ILoadPolicy.NO_LOAD);
        try {
            dict.open();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        stemmer = new WordnetStemmer(dict);
    }

    public String getLemmaByStandfordPos(String word, String pos) {
        POS posType = null;
        switch (pos) {
        case "JJ":
        case "JJS":
        case "JJR":
            posType = POS.ADJECTIVE;
            break;
        case "VBN":
            posType = POS.VERB;
            break;
        case "NN":
        case "NNS":
        case "NNP":
        case "NNPS":
            posType = POS.NOUN;
            break;
        //case "RB":
        //posType = POS.ADVERB;
        //break;
        }

        if (posType != null) {
            List<String> stemList = stemmer.findStems(word, posType);
            if (stemList.size() > 0) {
                IIndexWord idxWord = dict.getIndexWord(stemList.get(0), posType);
                if (idxWord != null) {
                    IWordID wordID = idxWord.getWordIDs().get(0);
                    IWord wnword = dict.getWord(wordID);
                    return wnword.getLemma();
                }
            }
        }
        return null;
    }
}
