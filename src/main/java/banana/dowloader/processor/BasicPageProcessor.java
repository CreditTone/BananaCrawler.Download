package banana.dowloader.processor;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import banana.core.download.HttpDownloader;
import banana.core.modle.BasicWritable;
import banana.core.modle.ContextModle;
import banana.core.modle.CrawlData;
import banana.core.modle.MasterConfig;
import banana.core.modle.TaskError;
import banana.core.modle.Task.BasicProcessor;
import banana.core.modle.Task.BasicProcessor.BlockCondition;
import banana.core.processor.Extractor;
import banana.core.processor.IProcessor;
import banana.core.request.HttpRequest;
import banana.core.response.HttpResponse;
import banana.core.response.Page;
import banana.core.response.StreamResponse;
import banana.core.util.SimpleMailSender;
import banana.core.util.SystemUtil;
import banana.dowloader.config.DataExtractorConfig;
import banana.dowloader.impl.DownloadServer;
import banana.dowloader.impl.DownloadTracker;
import banana.dowloader.impl.RemoteTaskContext;

public class BasicPageProcessor implements IProcessor {

	private static Logger logger = Logger.getLogger(BasicPageProcessor.class);
	
	public static final String PRO_RUNTIME_PREPARED_ERROR = "PRO_RUNTIME_PREPARED_ERROR";
	
	protected String index;

	protected String taskId;
	
	protected HttpDownloader downloader;

	protected Extractor extractor;

	protected String content_prepare;

	protected Map<String, DataExtractorConfig> page_context_define;

	protected Map<String, DataExtractorConfig> task_context_define;
	
	protected Map<String, DataExtractorConfig> global_context_define;
	
	protected String[] logs;
	
	protected List<BlockCondition> blockConditions;
	
	protected DownloadTracker downloadTracker;
	
	protected boolean keep_down;
	
	protected BasicPageProcessor(String taskId, BasicProcessor proConfig, Extractor extractor,HttpDownloader downloader) {
		this.index = proConfig.index;
		this.taskId = taskId;
		this.extractor = extractor;
		this.downloader = downloader;
		this.keep_down = proConfig.keep_down;
		if (proConfig.content_prepare != null) {
			if (proConfig.content_prepare instanceof String){
				content_prepare = JSON.toJSONString(Arrays.asList(proConfig.content_prepare));
			}else if (proConfig.content_prepare instanceof List){
				content_prepare = JSON.toJSONString(proConfig.content_prepare);
			}else if (proConfig.content_prepare instanceof Map){
				content_prepare = JSON.toJSONString(proConfig.content_prepare);
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
		
		if (proConfig.global_context != null) {
			global_context_define = new HashMap<String, DataExtractorConfig>();
			for (Entry<String, Object> entry : proConfig.global_context.entrySet()) {
				global_context_define.put(entry.getKey(), new DataExtractorConfig(entry.getValue()));
			}
		}
		
		this.logs = proConfig.logs;
		
		this.blockConditions = proConfig.blockConditions;
	}

	@Override
	public RuntimeContext process(HttpResponse response, Object taskContext, List<HttpRequest> queue,
			List<CrawlData> objectContainer) throws Exception {
		Page page = (Page) response;
		RuntimeContext runtimeContext = null;
		RemoteTaskContext remoteTaskContext = (RemoteTaskContext) taskContext;
		if (content_prepare != null) {
			String content = extractor.parseData(content_prepare, page.getContent());
			if (content == null) {
				runtimeContext = RuntimeContext.create(page, remoteTaskContext);
				TaskError taskError = new TaskError(taskId.split("_")[0], taskId, TaskError.PROCESSOR_ERROR_TYPE, new Exception("content prepare error " + index));
				runtimeContext.copyTo(taskError.runtimeContext);
				DownloadServer.getInstance().getMasterServer().errorStash(taskId, taskError);
				if (!keep_down){
					return null;
				}
				runtimeContext.put(PRO_RUNTIME_PREPARED_ERROR, true);
			}else{
				page.setContent(content);
			}
		}
		runtimeContext = runtimeContext == null?RuntimeContext.create(page, remoteTaskContext):runtimeContext;
		runtimeContext.setQueue(queue);
		if (page_context_define != null && !runtimeContext.get(PRO_RUNTIME_PREPARED_ERROR, false)) {
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
						FilterResult filterResult = filter(jsonValue, dataExtratorConfig.unique);
						runtimeContext.putFilterCount(filterResult.getFilterCount());
						jsonValue = filterResult.getResult();
					}
					if (jsonValue != null) {
						runtimeContext.put(entry.getKey(), jsonValue);
					}
				} else {
					runtimeContext.put(entry.getKey(), value);
				}
			}
		}
		if (task_context_define != null && !runtimeContext.get(PRO_RUNTIME_PREPARED_ERROR, false)) {
			for (Entry<String, DataExtractorConfig> entry : task_context_define.entrySet()) {
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
						FilterResult filterResult = filter(jsonValue, dataExtratorConfig.unique);
						runtimeContext.putFilterCount(filterResult.getFilterCount());
						jsonValue = filterResult.getResult();
					}
					if (jsonValue != null) {
						remoteTaskContext.put(entry.getKey(), jsonValue);
					}
				} else {
					remoteTaskContext.put(entry.getKey(), value);
				}
			}
		}
		if (global_context_define != null && !runtimeContext.get(PRO_RUNTIME_PREPARED_ERROR, false)) {
			for (Entry<String, DataExtractorConfig> entry : global_context_define.entrySet()) {
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
						FilterResult filterResult = filter(jsonValue, dataExtratorConfig.unique);
						runtimeContext.putFilterCount(filterResult.getFilterCount());
						jsonValue = filterResult.getResult();
					}
					if (jsonValue != null) {
						DownloadServer.getInstance().getMasterServer().putGlobalContext(entry.getKey(), new BasicWritable(jsonValue));
					}
				} else {
					if (entry.getKey().startsWith("@value")){
						DownloadServer.getInstance().getMasterServer().putGlobalContext(value, new BasicWritable(entry.getKey().replace("@value", "").trim()));
					}else{
						DownloadServer.getInstance().getMasterServer().putGlobalContext(entry.getKey(), new BasicWritable(value));
					}
				}
			}
		}
		for (int i = 0; (logs != null && i < logs.length); i++) {
			logger.info(runtimeContext.parseString(logs[i]));
		}
		for (int i = 0; blockConditions != null && i < blockConditions.size(); i++) {
			if (runtimeContext.parseString(blockConditions.get(i).condition).equals("true")){
				downloader.blockDriver(page.getDriverId());
				if (blockConditions.get(i).email != null){
					MasterConfig config = DownloadServer.getInstance().getMasterServer().getMasterConfig();
					if (config.email != null){
						SimpleMailSender sender = new SimpleMailSender(config.email.host, config.email.username, config.email.password);
						String content = "downloader_node:" + SystemUtil.getLocalIP() + ":" + DownloadServer.getInstance().config.listen + "</br>taskid:" +taskId +"</br>driverId" + page.getDriverId();
						sender.send(config.email.to, "CookieInvalid", content);
					}
				}
				break;
			}
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
				Object result = context.parseObject(entry.getValue());
				if (result != null) {
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

	public final FilterResult filter(JSON src, List<String> unique) {
		FilterResult fr = new FilterResult();
		if (src instanceof JSONObject) {
			JSONObject data = (JSONObject) src;
			String[] fields = new String[unique.size()];
			for (int i = 0; i < fields.length; i++) {
				fields[i] = data.getString(unique.get(i));
			}
			boolean exists = DownloadServer.getInstance().getMasterServer().filterQuery(taskId, fields).get();
			if (exists) {
				fr.setFilterCount(1);
				return fr;
			}
			fr.setResult(data);
			return fr;
		} else {
			JSONArray dataArr = (JSONArray) src;
			JSONArray ret = new JSONArray();
			for (int i = 0; i < dataArr.size(); i++) {
				JSONObject item = dataArr.getJSONObject(i);
				FilterResult filterResultItem = filter(item, unique);
				if (filterResultItem.getFilterCount() == 0) {
					ret.add(filterResultItem.getResult());
				}else{
					fr.setFilterCount(fr.getFilterCount()+1);
				}
			}
			fr.setResult(ret);
			return fr;
		}
	}

	public DownloadTracker getDownloadTracker() {
		return downloadTracker;
	}

	public void setDownloadTracker(DownloadTracker downloadTracker) {
		this.downloadTracker = downloadTracker;
	}
//
//	@Override
//	public RuntimeContext process(StreamResponse stream, Object taskContext, List<HttpRequest> queue,
//			List<CrawlData> objectContainer) throws Exception {
	// RemoteTaskContext remoteTaskContext = (RemoteTaskContext) taskContext;
	// return RuntimeContext.create(stream, remoteTaskContext);
//	}

}
