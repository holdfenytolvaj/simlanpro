package wiki.hadoop.input;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.sun.org.apache.xerces.internal.parsers.DOMParser;

import edu.jhu.nlp.wikipedia.WikiPage;

public class WikipediaPageXmlParser {

	private static String FEATURE_URI = "http://apache.org/xml/features/dom/defer-node-expansion";
	private static final Log LOGGER = LogFactory.getLog(WikipediaPageXmlParser.class);

	/*
	public static String getPlainText(String wikiText) {
		String ANYTHING_EXCEPT_SQBRKT_COLUMN = "([^\\[\\]:]*)";
		String ANYTHING_EXCEPT_SQBRKT_COLUMN_PIPE = "([^\\[\\]:|]*)";
		String ANYTHING_EXCEPT_CLYBRKT = "([^\\{\\}]*)";
		String ANYTHING_EXCEPT_EQUAL = "([^=]*)";
		int MAX_DEPTH = 5;

		String text = wikiText.replaceAll("&gt;", ">")
				.replaceAll("&lt;", "<")
				.replaceAll("&quot;", "")
				.replaceAll("<ref(?:[^>/]|[^>]*[^>/])>[^<>]*</ref\\s*>", " ")
				.replaceAll("<[^<>]*(/|--)>", " ")
				.replaceAll("\\'+", "")
				.replaceAll("=+\\s*" + ANYTHING_EXCEPT_EQUAL + "\\s*=+", "$1");

		int depth = 0;
		while (text.contains("{{") && depth < MAX_DEPTH) {
			text = text.replaceAll("\\{\\{" + ANYTHING_EXCEPT_CLYBRKT + "\\}\\}", " ");
			depth++;
		}
		depth = 0;
		while (text.contains("[[") && depth < MAX_DEPTH) {
			text = text.replaceAll("\\[\\[" + ANYTHING_EXCEPT_SQBRKT_COLUMN_PIPE + "\\]\\]", "$1")
					.replaceAll("\\[\\[" + ANYTHING_EXCEPT_SQBRKT_COLUMN + "\\|" + ANYTHING_EXCEPT_SQBRKT_COLUMN_PIPE + "\\]\\]", "$2")
					.replaceAll("\\[\\[(" + ANYTHING_EXCEPT_SQBRKT_COLUMN + ":)+" + ANYTHING_EXCEPT_SQBRKT_COLUMN + "\\]\\]", " ");
			depth++;
		}
		return text;
	}
	*/

	public static WikiPage parse(String pageXmlString) {
		try {
			DOMParser domParser = new DOMParser();
			domParser.setFeature(FEATURE_URI, true);

			InputStream stream = new ByteArrayInputStream(pageXmlString.trim().getBytes(StandardCharsets.UTF_8));
			domParser.parse(new InputSource(stream));

			Document doc = domParser.getDocument();
			NodeList pages = doc.getElementsByTagName("page");
			Node pageNode = pages.item(0);

			WikiPage wpage = new WikiPage();

			NodeList childNodes = pageNode.getChildNodes();
			boolean isTextFound = false;
			for (int j = 0; j < childNodes.getLength(); j++) {
				Node child = childNodes.item(j);
				switch (child.getNodeName()) {
				case "title":
					wpage.setTitle(child.getFirstChild().getNodeValue());
					break;
				case "id":
					wpage.setID(child.getFirstChild().getNodeValue());
					break;
				case "revision":
					NodeList revchilds = child.getChildNodes();
					for (int k = 0; k < revchilds.getLength(); k++) {
						switch (revchilds.item(k).getNodeName()) {
						case "text":
							Node textNode = revchilds.item(k).getFirstChild();
							if (textNode != null) {
								isTextFound = true;
								wpage.setWikiText(textNode.getNodeValue());
							}
							break;
						}
					}
					break;
				}
			}

			if (!isTextFound) {
				wpage.setWikiText("");
				LOGGER.error("There is no text found for '" + wpage.getTitle() + "' with id " + wpage.getID());
			}

			return wpage;

		} catch (SAXException | IOException e) {
			System.out.println(pageXmlString);
			throw new RuntimeException(e);
		}
	}

}
