package banana.dowloader.processor;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import banana.core.modle.CrawlData;
import banana.core.processor.Extractor;
import banana.core.processor.PageProcessor;
import banana.core.protocol.Task.BasicProcessor;
import banana.core.request.HttpRequest;
import banana.core.request.StartContext;
import banana.core.response.Page;
import banana.dowloader.config.DataExtractorConfig;
import banana.dowloader.impl.DownloadServer;

public class BasicPageProcessor implements PageProcessor {

	private static Logger logger = Logger.getLogger(BasicPageProcessor.class);

	protected String taskId;

	protected Extractor extractor;

	protected String direct;

	protected String define;

	protected Map<String, DataExtractorConfig> page_context_define;

	protected Map<String, DataExtractorConfig> task_context_define;
	
	protected String[] logs;

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
			page_context_define = new HashMap<String, DataExtractorConfig>();
			for (Entry<String, Object> entry : proConfig.page_context.entrySet()) {
				page_context_define.put(entry.getKey(), new DataExtractorConfig(entry.getValue()));
			}
		}

		if (proConfig.task_context != null) {
			task_context_define = new HashMap<String, DataExtractorConfig>();
			for (Entry<String, Object> entry : proConfig.task_context.entrySet()) {
				task_context_define.put(entry.getKey(), new DataExtractorConfig(entry.getValue()));
			}
		}
		
		this.logs = proConfig.logs;
	}

	@Override
	public RuntimeContext process(Page page, StartContext context, List<HttpRequest> queue,
			List<CrawlData> objectContainer) throws Exception {
		if (direct != null) {
			String content = extractor.parseData(direct, page.getContent());
			if (content == null) {
				logger.warn(String.format("content prepare error %s", direct));
				return null;
			}
			page.setContent(content);
		} else if (define != null) {
			String content = extractor.parseData(define, page.getContent());
			if (content == null) {
				logger.warn(String.format("content prepare error %s", define));
				return null;
			}
			page.setContent(content);
		}
		RuntimeContext runtimeContext = RuntimeContext.create(page, context);
		if (page_context_define != null) {
			for (Entry<String, DataExtractorConfig> entry : page_context_define.entrySet()) {
				DataExtractorConfig dataExtratorConfig = entry.getValue();
				String value = extractor.parseData(dataExtratorConfig.parseConfig, page.getContent());
				if (value == null) {
					logger.warn(String.format("page context parse error %s %s", dataExtratorConfig.parseConfig, page.getRequest().getUrl()));
					continue;
				}
				if ((value.startsWith("{") && value.endsWith("}")) || (value.startsWith("[") && value.endsWith("]"))) {
					JSON jsonValue = (JSON) JSON.parse(value);
					writeTemplates(jsonValue, runtimeContext, dataExtratorConfig.templates);
					if (dataExtratorConfig.unique != null) {
						jsonValue = filter(jsonValue, dataExtratorConfig.unique);
					}
					if (jsonValue != null) {
						runtimeContext.put(entry.getKey(), jsonValue);
					}
				} else {
					runtimeContext.put(entry.getKey(), value);
				}
			}
		}
		for (int i = 0; (logs != null && i < logs.length); i++) {
			logger.info(runtimeContext.parse(logs[i]));
		}
		return runtimeContext;
	}

	public final void writeTemplates(JSON src, RuntimeContext context, HashMap<String, String> templates)
			throws IOException {
		if (templates.isEmpty()) {
			return;
		}
		if (src instanceof JSONObject) {
			JSONObject data = (JSONObject) src;
			for (Entry<String, String> entry : templates.entrySet()) {
				String result = context.parse(entry.getValue());
				if (!result.isEmpty()) {
					data.put(entry.getKey(), result);
					continue;
				}
			}
		} else {
			JSONArray dataArr = (JSONArray) src;
			for (int i = 0; i < dataArr.size(); i++) {
				JSONObject item = dataArr.getJSONObject(i);
				writeTemplates(item, context, templates);
			}
		}
	}

	public final JSON filter(JSON src, List<String> unique) {
		if (src instanceof JSONObject) {
			JSONObject data = (JSONObject) src;
			String[] fields = new String[unique.size()];
			for (int i = 0; i < fields.length; i++) {
				fields[i] = data.getString(unique.get(i));
			}
			boolean exists = DownloadServer.getInstance().getMasterServer().filterQuery(taskId, fields).get();
			if (exists) {
				return null;
			}
			return data;
		} else {
			JSONArray dataArr = (JSONArray) src;
			JSONArray ret = new JSONArray();
			for (int i = 0; i < dataArr.size(); i++) {
				JSONObject item = dataArr.getJSONObject(i);
				item = (JSONObject) filter(item, unique);
				if (item != null) {
					ret.add(item);
				}
			}
			return ret;
		}
	}

}
