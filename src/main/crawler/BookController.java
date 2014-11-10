package crawler;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

public class BookController {

	public static void main(String[] args) throws Exception {
		String crawlStorageFolder = "/tmp/crawler/books";
		int numberOfCrawlers = 2;

		CrawlConfig config = new CrawlConfig();
		config.setCrawlStorageFolder(crawlStorageFolder);
		config.setIncludeBinaryContentInCrawling(true);
		config.setFollowRedirects(true);
		config.setConnectionTimeout(1000 * 60 * 5);
		config.setUserAgentString("Language experiment (http://github.com/holdfenytolvaj/simlanpro)");

		PageFetcher pageFetcher = new PageFetcher(config);
		RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
		robotstxtConfig.setEnabled(false);
		RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
		CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);

		for (int i = 100; i <= 999; i++) {
			controller.addSeed("http://.../down_15" + i + ".html");
		}

		BookCrawler.configure(config.getCrawlStorageFolder());
		controller.start(BookCrawler.class, numberOfCrawlers);

	}
}
