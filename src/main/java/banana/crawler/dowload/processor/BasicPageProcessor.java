package banana.crawler.dowload.processor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;

import banana.core.modle.CrawlData;
import banana.core.processor.Extractor;
import banana.core.processor.PageProcessor;
import banana.core.protocol.Task.BasicProcessor;
import banana.core.request.HttpRequest;
import banana.core.request.StartContext;
import banana.core.response.Page;

public class BasicPageProcessor implements PageProcessor {
	
	private static Logger logger = Logger.getLogger(BasicPageProcessor.class);

	protected String taskId;

	protected Extractor extractor;

	protected String direct;

	protected String define;

	protected Map<String, String> page_context_define;

	protected Map<String, String> task_context_define;

	protected BasicPageProcessor(String taskId, BasicProcessor proConfig, Extractor extractor) {
		this.taskId = taskId;
		this.extractor = extractor;
		if (proConfig.content_prepare != null) {
			if (proConfig.content_prepare.direct != null) {
				direct = JSON.toJSONString(proConfig.content_prepare.direct);
			}
			if (proConfig.content_prepare.define != null) {
				define = JSON.toJSONString(proConfig.content_prepare.define);
			}
		}

		if (proConfig.page_context != null) {
			page_context_define = new HashMap<String, String>();
			for (Entry<String, Object> entry : proConfig.page_context.entrySet()) {
				if (entry.getValue() instanceof String) {
					page_context_define.put(entry.getKey(), (String) entry.getValue());
				} else {
					page_context_define.put(entry.getKey(), JSON.toJSONString(entry.getValue()));
				}
			}
		}

		if (proConfig.task_context != null) {
			task_context_define = new HashMap<String, String>();
			for (Entry<String, Object> entry : proConfig.task_context.entrySet()) {
				if (entry.getValue() instanceof String) {
					task_context_define.put(entry.getKey(), (String) entry.getValue());
				} else {
					task_context_define.put(entry.getKey(), JSON.toJSONString(entry.getValue()));
				}
			}
		}
	}

	@Override
	public RuntimeContext process(Page page, StartContext context, List<HttpRequest> queue,
			List<CrawlData> objectContainer) throws Exception {
		RuntimeContext runtimeContext = RuntimeContext.create(page, context);
		if (direct != null) {
			String content = extractor.parseData(direct, page.getContent());
			if (content == null){
				logger.warn(String.format("content prepare error %s", direct));
				return null;
			}
			page.setContent(content);
		} else if (define != null) {
			String content = extractor.parseData(define, page.getContent());
			if (content == null){
				logger.warn(String.format("content prepare parse error %s", define));
				return null;
			}
			page.setContent(content);
		}

		if (page_context_define != null) {
			for (Entry<String, String> entry : page_context_define.entrySet()) {
				String value = extractor.parseData(entry.getValue(), page.getContent());
				if (value == null) {
					logger.warn(String.format("page context parse error %s", entry.getValue()));
					return null;
				}
				if (value.length() > 0) {
					runtimeContext.put(entry.getKey(), value);
				}
			}
		}
		return runtimeContext;
	}

}
