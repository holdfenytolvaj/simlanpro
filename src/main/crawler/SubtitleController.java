package crawler;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

public class SubtitleController {

	public static void main(String[] args) throws Exception {
		String crawlStorageFolder = "/tmp/crawler/subtitles/en";
		int numberOfCrawlers = 2;

		CrawlConfig config = new CrawlConfig();
		config.setCrawlStorageFolder(crawlStorageFolder);
		config.setIncludeBinaryContentInCrawling(true);
		config.setFollowRedirects(true);
		config.setConnectionTimeout(1000 * 60 * 5);
		config.setPolitenessDelay(10000);
		config.setUserAgentString("Language experiment (http://github.com/holdfenytolvaj/simlanpro)");

		/*
		 * Instantiate the controller for this crawl.
		 */
		PageFetcher pageFetcher = new PageFetcher(config);
		RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
		robotstxtConfig.setEnabled(false);
		RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
		CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);

		/*
		 * For each crawl, you need to add some seed urls. These are the first
		 * URLs that are fetched and then the crawler starts following links
		 * which are found in these pages
		 */

		//controller.addSeed("http://www.opensubtitles.org/en/search/sublanguageid-eng/moviename-u");
		for (int i = 40; i <= 40; i += 40) {//1914
			controller.addSeed("http://www.opensubtitles.org/en/search/sublanguageid-eng/moviename-u/offset-" + i + "");
		}

		/*
		 * Start the crawl. This is a blocking operation, meaning that your code
		 * will reach the line after this only when crawling is finished.
		 */
		SubtitleCrawler.configure(config.getCrawlStorageFolder());
		controller.start(SubtitleCrawler.class, numberOfCrawlers);

	}
}
