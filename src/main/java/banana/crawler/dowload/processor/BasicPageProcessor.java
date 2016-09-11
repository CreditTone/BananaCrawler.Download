package banana.crawler.dowload.processor;

import java.util.List;

import banana.core.modle.CrawlData;
import banana.core.processor.PageProcessor;
import banana.core.request.HttpRequest;
import banana.core.request.StartContext;
import banana.core.response.Page;

public class BasicPageProcessor implements PageProcessor {

	@Override
	public void process(Page page, StartContext context, List<HttpRequest> queue, List<CrawlData> objectContainer)
			throws Exception {
	}

}
