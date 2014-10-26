package wiki.hadoop.wordcount;

import static java.util.regex.Pattern.compile;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import wiki.hadoop.input.WikipediaPageXmlParser;
import edu.jhu.nlp.wikipedia.WikiPage;
import edu.jhu.nlp.wikipedia.WikiTextParser;

/** based on https://github.com/sagen/hadoop-wiki-indexer */
public class WordsInWikiPageMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private final Text word = new Text();
	private static final Pattern CONTAINS_DIGIT = compile("\\d+");
	private static final Pattern PATTERN_TO_REMOVE = compile("[\\p{P}\\p{S}]+");

	@Override
	public void map(LongWritable lineNumber, Text xmlWikiPage, Context context) throws IOException, InterruptedException {

		WikiPage wp = WikipediaPageXmlParser.parse(xmlWikiPage.toString());

		if (!StringUtils.isBlank(wp.getText()) && !wp.isRedirect() && !wp.isSpecialPage() && !wp.isDisambiguationPage()) {
			String parsedContent = getPreparedPlainContent(wp);

			StringTokenizer itr = new StringTokenizer(parsedContent);
			while (itr.hasMoreTokens()) {
				String nextWord = itr.nextToken();
				if (CONTAINS_DIGIT.matcher(nextWord).find()) {
					continue;
				}

				//we dont have stemmer, so just pass in the word as it is
				word.set(nextWord);
				context.write(word, one);
			}

			String status = "Done mapping ";// + wp.getTitle();
			context.setStatus(status);
		}
	}

	private static String getPreparedPlainContent(WikiPage page) throws UnsupportedEncodingException {
		//remove all punctuation, numbers, and math symbols
		return PATTERN_TO_REMOVE.matcher(new WikiTextParser(page.getText()).getPlainText().toLowerCase()).replaceAll(" ");
	}

}
