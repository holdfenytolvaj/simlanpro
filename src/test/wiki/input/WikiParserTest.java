package wiki.input;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import edu.jhu.nlp.wikipedia.WikiPage;

public class WikiParserTest {

	//@Test
	public static void testParsingSimple() {
		WikiPage wp = new WikiPage();
		wp.setWikiText(getContent("wikisample_raw.txt"));

		//TODO now using a python script for this, need to setup using jython
	}

	public static String getContent(String fileName) {
		StringBuilder sb = new StringBuilder();
		try (BufferedReader br = new BufferedReader(new InputStreamReader(WikiParserTest.class.getResourceAsStream(fileName)))) {
			String line;
			while ((line = br.readLine()) != null) {
				sb.append(line);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return sb.toString();
	}
}
