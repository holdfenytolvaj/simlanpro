package crawler;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.BinaryParseData;
import edu.uci.ics.crawler4j.url.WebURL;
import edu.uci.ics.crawler4j.util.IO;

public class SubtitleCrawler extends WebCrawler {
	private final static Pattern DOWNLOAD = Pattern.compile("\\d+)");
	private final static Pattern DOWNLOAD_ALTERNATIVE = Pattern.compile(".*");
	private final static Pattern LAST_DIGITS = Pattern.compile("(\\d+)$");

	private static File storageFolder;

	public static void configure(String storageFolderName) {
		storageFolder = new File(storageFolderName);
		if (!storageFolder.exists()) {
			storageFolder.mkdirs();
		}
	}

	@Override
	public boolean shouldVisit(WebURL url) {
		String href = url.getURL().toLowerCase();

		boolean shouldVisit = DOWNLOAD.matcher(href).matches();

		if (shouldVisit) {
			System.out.println(shouldVisit + ":" + url);
			Matcher m = LAST_DIGITS.matcher(href);
			m.find();
			url.setURL("" + m.group(1)); //hack
			System.out.println("rewrite:" + url);
		}
		shouldVisit |= DOWNLOAD_ALTERNATIVE.matcher(href).matches();
		System.out.println(shouldVisit + ":" + url);
		return shouldVisit;
	}

	@Override
	public void visit(Page page) {
		String url = page.getWebURL().getURL();
		System.out.println("visit: " + url);

		if (!(page.getParseData() instanceof BinaryParseData)) {
			return;
		}

		if (!DOWNLOAD_ALTERNATIVE.matcher(url).matches()) {
			return;
		}

		IO.writeBytesToFile(page.getContentData(), storageFolder.getAbsolutePath() + "/" + url.replace("http://", "").replace("/", "_"));
		System.out.println("Stored: " + url);
	}
}
