package crawler;

import java.io.File;
import java.util.regex.Pattern;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.BinaryParseData;
import edu.uci.ics.crawler4j.url.WebURL;
import edu.uci.ics.crawler4j.util.IO;

public class BookCrawler extends WebCrawler {
	private final static Pattern DOWNLOAD = Pattern.compile("http://.*/web/down.*(zip|rar)");
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

		if (!DOWNLOAD.matcher(url).matches()) {
			return;
		}

		IO.writeBytesToFile(page.getContentData(), storageFolder.getAbsolutePath() + "/" + url.replace("http://", "").replace("/", "_"));
		System.out.println("Stored: " + url);
	}

}
