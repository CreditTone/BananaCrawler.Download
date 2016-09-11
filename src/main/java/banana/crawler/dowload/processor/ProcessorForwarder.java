package banana.crawler.dowload.processor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


import com.alibaba.fastjson.JSON;

import banana.core.modle.CrawlData;
import banana.core.processor.PageProcessor;
import banana.core.request.HttpRequest;
import banana.core.request.StartContext;
import banana.core.response.Page;
import banana.crawler.dowload.impl.DownloadTracker;

public class ProcessorForwarder implements PageProcessor {
	
	private String taskId;
	
	private DownloadTracker downloadTracker;
	
	private banana.core.protocol.Task.ProcessorForwarder forwarderConfig;
	
	private Map<String,String> page_context_define;
	
	public ProcessorForwarder(String taskId, banana.core.protocol.Task.ProcessorForwarder forwarderConfig,
			DownloadTracker downloadTracker) {
		this.taskId = taskId;
		this.downloadTracker = downloadTracker;
		this.forwarderConfig = forwarderConfig;
		if (forwarderConfig.getPage_context() != null){
			page_context_define = new HashMap<String,String>();
			for (Entry<String, Object> entry : forwarderConfig.getPage_context().entrySet()) {
				if (entry.getValue() instanceof String){
					page_context_define.put(entry.getKey(), (String) entry.getValue());
				}else{
					page_context_define.put(entry.getKey(), JSON.toJSONString(entry.getValue()));
				}
			}
		}
	}

	@Override
	public void process(Page page, StartContext context, List<HttpRequest> queue, List<CrawlData> objectContainer)
			throws Exception {
		RuntimeContext runtimeContext = RuntimeContext.create(page.getRequest(), context);
		runtimeContext.put("_page_content", page.getContent());
		for (Map<String,String> selector : forwarderConfig.getSelector()){
			String condition = selector.get("condition");
			if (runtimeContext.parse(condition).equals("true")){
				String index = selector.get("target");
				PageProcessor result = downloadTracker.findPageProcessor(index);
				result.process(page, context, queue, objectContainer);
				break;
			}
		}
	}

}
