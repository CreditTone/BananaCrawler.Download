package banana.dowloader.processor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import banana.core.download.HttpDownloader;
import banana.core.modle.CrawlData;
import banana.core.modle.TaskContext;
import banana.core.modle.TaskError;
import banana.core.processor.Extractor;
import banana.core.processor.PageProcessor;
import banana.core.protocol.Task.Processor;
import banana.core.protocol.Task.Processor.Forwarder;
import banana.core.request.HttpRequest;
import banana.core.request.PageRequest;
import banana.core.request.RequestBuilder;
import banana.core.response.Page;
import banana.dowloader.config.DataExtractorConfig;
import banana.dowloader.config.RequestExtractorConfig;
import banana.dowloader.impl.DownloadServer;

public class JSONConfigPageProcessor extends BasicPageProcessor {
	
	public static final int RUN_MODE = 0;
	
	public static final int TEST_MODE = 1;
	
	public static int MODE = RUN_MODE;
	
	private static Logger logger = Logger.getLogger(JSONConfigPageProcessor.class);
	
	private DataExtractorConfig[] dataParser;
	
	private RequestExtractorConfig[] requestParser;
	
	private Forwarder[] forwarders;
	
	public JSONConfigPageProcessor(String taskId,Processor config,HttpDownloader downloader){
		this(taskId, config, DownloadServer.getInstance().extractor,downloader);
	}
	
	public JSONConfigPageProcessor(String taskId,Processor config,Extractor extractor,HttpDownloader downloader) {
		super(taskId, config, extractor,downloader);
		if (config.crawler_data != null) {
			dataParser = new DataExtractorConfig[config.crawler_data.length];
			for (int i = 0 ;i < config.crawler_data.length ;i++) {
				dataParser[i] = new DataExtractorConfig(config.crawler_data[i]);
			}
		}
		if (config.crawler_request != null){
			requestParser = new RequestExtractorConfig[config.crawler_request.length];
			for (int i = 0 ;i < config.crawler_request.length ;i++) {
				requestParser[i] = new RequestExtractorConfig(config.crawler_request[i]);
			}
		}
		this.forwarders = config.forwarders;
	}
	

	@Override
	public RuntimeContext process(Page page, TaskContext context, List<HttpRequest> queue, List<CrawlData> objectContainer) throws Exception {
		RuntimeContext runtimeContext = super.process(page, context, queue, objectContainer);
		if (runtimeContext == null){
			return null;
		}
		boolean parseable = !runtimeContext.get(PRO_RUNTIME_PREPARED_ERROR, false);
		String responseJson = null;
		PageRequest currentPageReq = (PageRequest) page.getRequest();
		if (dataParser != null){
			for (int i = 0; i < dataParser.length; i++) {
				DataExtractorConfig dataExtractorConfig = dataParser[i];
				JSONObject _data = (JSONObject) currentPageReq.getAttribute("_data");
				JSON result = null;
				if (parseable && (dataExtractorConfig.condition == null || runtimeContext.parseString(dataExtractorConfig.condition).equals("true"))){
					responseJson = extractor.parseData(dataExtractorConfig.parseConfig, page.getContent());
					if (responseJson == null){
						logger.info(String.format("parsed data is null %s", dataExtractorConfig.parseConfig));
						continue;
					}
					if ((responseJson != null) && (responseJson.startsWith("{") || responseJson.startsWith("["))){
						result = (JSON) JSON.parse(responseJson);
					}
				}
				if (result == null){
					result = new JSONObject();
				}
				merger(_data, result);
				writeTemplates(result, runtimeContext, dataExtractorConfig.templates);
				if (dataExtractorConfig.unique != null){
					result = filter(result, dataExtractorConfig.unique);
				}
				writeObject(runtimeContext, objectContainer, page.getRequest().getUrl(), result, dataExtractorConfig.update);
			}
		}
		
		if (requestParser != null && parseable) {
			for (int i = 0; i < requestParser.length; i++) {
				RequestExtractorConfig requestExtractorConfig = requestParser[i];
				if (requestExtractorConfig.dataContext != null){
					JSON contextData = (JSON) runtimeContext.get(requestExtractorConfig.dataContext.key);
					if (contextData != null){
						if (contextData instanceof JSONObject && (requestExtractorConfig.condition == null || runtimeContext.parseString(requestExtractorConfig.condition,(Map<String, Object>) contextData).equals("true"))) {
							JSONArray temp = new JSONArray();
							temp.add(contextData);
							contextData = temp;
						}
						if (contextData instanceof JSONArray) {
							JSONArray requestDataArr = (JSONArray) contextData;
							for (int j = 0; j < requestDataArr.size(); j++) {
								runtimeContext.setDataContext((Map)requestDataArr.get(j));
								if (requestExtractorConfig.condition == null || runtimeContext.parseString(requestExtractorConfig.condition).equals("true")){
									JSONObject jsonObject = new JSONObject();
									writeTemplates(jsonObject, runtimeContext, requestExtractorConfig.templates);
									if (requestExtractorConfig.unique != null){
										jsonObject = (JSONObject) filter(jsonObject, requestExtractorConfig.unique);
									}
									if (jsonObject != null){
										HttpRequest req = createRequest(runtimeContext, requestExtractorConfig, jsonObject).get(0);
										if (req != null){
											if (requestExtractorConfig.dataContext.data_flow){
												req.addAttribute("_data", requestDataArr.get(j));
											}
											queue.add(req);
										}
									}
								}
								runtimeContext.setDataContextNull();
							}
						}
					}
				}else if (requestExtractorConfig.condition == null || runtimeContext.parseString(requestExtractorConfig.condition).equals("true")){
					responseJson = extractor.parseData(requestExtractorConfig.parseConfig, page.getContent());
					if (responseJson == null){
						continue;
					}
					if (responseJson.startsWith("{") || responseJson.startsWith("[")){
						JSON data = (JSON) JSON.parse(responseJson);
						writeTemplates(data, runtimeContext, requestExtractorConfig.templates);
						if (requestExtractorConfig.unique != null){
							data = filter(data, requestExtractorConfig.unique);
						}
						if (data != null){
							List<HttpRequest> resps = createRequest(runtimeContext, requestExtractorConfig, data);
							if (resps != null){
								queue.addAll(resps);
							}
						}
					}
				}
			}
		}
		if (forwarders != null && parseable){
			for (Forwarder fwd : forwarders){
				if (runtimeContext.parseString(fwd.condition).equals("true")){
					PageProcessor result = downloadTracker.findPageProcessor(fwd.processor);
					if (result != null){
						result.process(page, context, queue, objectContainer);
						return runtimeContext;
					}
				}
			}
			TaskError taskError = new TaskError(taskId.split("_")[0], taskId, TaskError.FORWORD_ERROR_TYPE, new Exception("no forword on index " + index));
			runtimeContext.copyTo(taskError.runtimeContext);
			DownloadServer.getInstance().getMasterServer().errorStash(taskId, taskError);
		}
		return runtimeContext;
	}
	
	private DBObject buildUpdateQuery(Map<String,String> update,RuntimeContext runtimeContext) throws IOException{
		if (update == null || update.isEmpty()){
			return null;
		}
		BasicDBObject dbObject = new BasicDBObject();
		for (Entry<String,String> entry : update.entrySet()) {
			dbObject.put(entry.getKey(), runtimeContext.parseString(entry.getValue()));
		}
		return dbObject;
	}
	
	private void writeObject(RuntimeContext runtimeContext,List<CrawlData> objectContainer, String url, JSON data,Map<String,String> update) throws IOException{
		DBObject body = null;
		DBObject updateQuery = null;
		if (data instanceof JSONArray){
			JSONArray dataArr  = (JSONArray) data;
			for (int i = 0; i < dataArr.size(); i++) {
				Map<String,Object> map = fixData(dataArr.getJSONObject(i));
				if (!map.isEmpty()){
					body = new BasicDBObject(map);
					updateQuery = buildUpdateQuery(update, runtimeContext);
					objectContainer.add(new CrawlData(taskId, url, body, updateQuery));
				}
			}
		}else{
			Map<String, Object> map = fixData((JSONObject) data);
			if (!map.isEmpty()){
				body = new BasicDBObject(map);
				updateQuery = buildUpdateQuery(update, runtimeContext);
				objectContainer.add(new CrawlData(taskId, url, body, updateQuery));
			}
		}
	}

	private final Map<String, Object> fixData(JSONObject data) {
		Map<String,Object> map = (Map<String, Object>) data;
		Set<String> keySet = new HashSet<String>(map.keySet());
		for (String key : keySet) {
			if (map.get(key) == null){
				map.remove(key);
			}
		}
		return map;
	}
	
	private List<HttpRequest> createRequest(RuntimeContext runtimeContext,RequestExtractorConfig requestExtractorConfig,JSON data) throws IOException{
		if (data instanceof JSONObject){
			JSONObject dataObj = (JSONObject) data;
			if (dataObj.getString("url") == null && dataObj.getString("download") == null) {
				return null;
			}
			HttpRequest req = null;
			if (dataObj.getString("url") != null){
				req = RequestBuilder.custom().setUrl(dataObj.getString("url")).setProcessor(requestExtractorConfig.processor).build();
			}else{
				req = RequestBuilder.custom().setDownload(dataObj.getString("download")).setProcessor(requestExtractorConfig.processor).build();
			}
			if (dataObj.containsKey("attribute")){
				Map<String,Object> attribute = (Map<String, Object>) dataObj.get("attribute");
				for (Entry<String, Object> pair:attribute.entrySet()) {
					Object value = pair.getValue();
					if (value == null) {
						logger.warn(String.format("extractor attr error %s",req.getUrl()));
						return null;
					}else if (value instanceof String) {
						String strValue = (String)value;
						if ((strValue = strValue.trim()).isEmpty()){
							return null;
						}
					}
					req.addAttribute(pair.getKey(), value);
				}
			}
			req.setMethod(requestExtractorConfig.method);
			if (requestExtractorConfig.headers != null){
				for(Entry<String, String> pair : requestExtractorConfig.headers.entrySet()){
					String value = pair.getValue();
					req.putHeader(pair.getKey(), runtimeContext.parseString(value));
				}
			}
			if (requestExtractorConfig.params != null){
				for(Entry<String, String> pair : requestExtractorConfig.params.entrySet()){
					String value = pair.getValue();
					req.putParams(pair.getKey(), runtimeContext.parseString(value));
				}
			}
			req.setPriority(requestExtractorConfig.priority);
			return Arrays.asList(req);
		}else{
			List<HttpRequest> reqs = new ArrayList<HttpRequest>();
			JSONArray dataArr = (JSONArray) data;
			for (int i = 0; i < dataArr.size(); i++) {
				List<HttpRequest> oneReq = createRequest(runtimeContext, requestExtractorConfig, dataArr.getJSONObject(i));
				if (oneReq != null){
					reqs.add(oneReq.get(0));
				}
			}
			return reqs;
		}
	}
	
	private final static void merger(JSONObject from,JSON to){
		if (from != null && from != to){
			if (to instanceof JSONObject){
				JSONObject dest = (JSONObject) to;
				for (Entry<String,Object> entry : from.entrySet()) {
					if (dest.containsKey(entry.getKey())){
						continue;
					}
					dest.put(entry.getKey(), entry.getValue());
				}
			}else{
				JSONArray destArr = (JSONArray) to;
				for (int i = 0; i < destArr.size(); i++) {
					merger(from, destArr.getJSONObject(i));
				}
			}
		}
	}
	
}
