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

		PageFetcher pageFetcher = new PageFetcher(config);
		RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
		robotstxtConfig.setEnabled(false);
		RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
		CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);

		//url removed
		for (int i = 40; i <= 40; i += 40) {//1914
			controller.addSeed("http://" + i);
		}

		SubtitleCrawler.configure(config.getCrawlStorageFolder());
		controller.start(SubtitleCrawler.class, numberOfCrawlers);

	}
}
