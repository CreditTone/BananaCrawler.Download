package banana.dowloader.processor;

import java.util.List;
import java.util.Map;

import banana.core.modle.CrawlData;
import banana.core.processor.PageProcessor;
import banana.core.request.HttpRequest;
import banana.core.request.StartContext;
import banana.core.response.Page;
import banana.dowloader.impl.DownloadServer;
import banana.dowloader.impl.DownloadTracker;

public class ProcessorForwarder extends BasicPageProcessor {
	
	private DownloadTracker downloadTracker;
	
	private banana.core.protocol.Task.ProcessorForwarder forwarderConfig;
	
	public ProcessorForwarder(String taskId, banana.core.protocol.Task.ProcessorForwarder forwarderConfig,
			DownloadTracker downloadTracker) {
		super(taskId, forwarderConfig, DownloadServer.getInstance().extractor);
		this.downloadTracker = downloadTracker;
		this.forwarderConfig = forwarderConfig;
	}

	@Override
	public RuntimeContext process(Page page, StartContext context, List<HttpRequest> queue, List<CrawlData> objectContainer)
			throws Exception {
		RuntimeContext runtimeContext = super.process(page, context, queue, objectContainer);
		if (runtimeContext == null){
			return null;
		}
		for (Map<String,String> selector : forwarderConfig.selector){
			String condition = selector.get("condition");
			if (runtimeContext.parse(condition).equals("true")){
				String index = selector.get("target");
				PageProcessor result = downloadTracker.findPageProcessor(index);
				result.process(page, context, queue, objectContainer);
				break;
			}
		}
		return runtimeContext;
	}

}
