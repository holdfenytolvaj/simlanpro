package wiki.classifier;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import edu.jhu.nlp.wikipedia.WikiXMLParser;
import edu.jhu.nlp.wikipedia.WikiXMLParserFactory;

/**
 * Reads the wikipedia xml file,
 * Reads the word frequency
 * 
 * Create an output that can be read with the Analyzer
 *
 * Word frequency can be downloaded from
 * http://expsy.ugent.be/subtlex-ch/
 * or generate it with the hadoop wordcount but that doesnt work for 
 * the Chinese language.  
 */
public class DifficultyClassifier {

	public static void main(String[] args) throws Exception {
		String inputFilepath = args[0];
		String outputFilepath = args[1];
		String classifierFilepath = args[2];

		WikiXMLParser wxsp = WikiXMLParserFactory.getSAXParser(inputFilepath);
		BufferedWriter bw = new BufferedWriter(new FileWriter(outputFilepath));

		wxsp.setPageCallback(new WikiPageCallbackHandlerZhPerWord(bw, loadClassifier(classifierFilepath)));
		wxsp.parse();

		bw.close();
	}

	/**
	 * Hardcoded:
	 * level 1:    0 -  500
	 * level 2:  500 - 1000
	 * level 3: 1000 - 2000
	 * level 4: 2000 - 3000
	 * level 5: 3000 - 4000
	 * level 6: 4000 - 5000
	 * level 7: 5000 -
	 */
	private static Map<String, Integer> loadClassifier(String classifierFilepath) throws FileNotFoundException, IOException {
		Map<String, Integer> classifier = new HashMap<>(8192);

		int counter = 0;
		try (BufferedReader br = new BufferedReader(new FileReader(classifierFilepath))) {
			String line;
			while ((line = br.readLine()) != null) {
				String[] wordPerFrequency = line.split("\t");
				if (wordPerFrequency.length > 0) {
					classifier.put(wordPerFrequency[0],
							(counter <= 500) ? 1 :
									(counter <= 1000) ? 2 :
											(counter <= 2000) ? 3 :
													(counter <= 3000) ? 4 :
															(counter <= 4000) ? 5 :
																	(counter <= 5000) ? 6 : 7);
				}
				counter++;
			}
		}
		return classifier;
	}
}
