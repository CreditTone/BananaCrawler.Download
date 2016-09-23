package banana.crawler.dowload.processor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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

import banana.core.modle.CrawlData;
import banana.core.processor.Extractor;
import banana.core.protocol.Task;
import banana.core.protocol.Task.Processor;
import banana.core.request.HttpRequest;
import banana.core.request.PageRequest;
import banana.core.request.RequestBuilder;
import banana.core.request.StartContext;
import banana.core.response.Page;
import banana.crawler.dowload.config.DataExtractorConfig;
import banana.crawler.dowload.config.RequestExtractorConfig;
import banana.crawler.dowload.impl.DownloadServer;

public class JSONConfigPageProcessor extends BasicPageProcessor {
	
	public static final int RUN_MODE = 0;
	
	public static final int TEST_MODE = 1;
	
	public static int MODE = RUN_MODE;
	
	private static Logger logger = Logger.getLogger(JSONConfigPageProcessor.class);
	
	private Task.Processor config;
	
	private DataExtractorConfig[] dataParser;
	
	private RequestExtractorConfig[] requestParser;
	
	public JSONConfigPageProcessor(String taskId,Processor config){
		this(taskId, config, DownloadServer.getInstance().extractor);
	}
	
	public JSONConfigPageProcessor(String taskId,Processor config,Extractor extractor) {
		super(taskId, config, extractor);
		this.config = config;
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
	}
	

	@Override
	public RuntimeContext process(Page page, StartContext context, List<HttpRequest> queue, List<CrawlData> objectContainer) throws Exception {
		RuntimeContext runtimeContext = super.process(page, context, queue, objectContainer);
		if (runtimeContext == null){
			return null;
		}
		String responseJson = null;
		PageRequest currentPageReq = (PageRequest) page.getRequest();
		if (dataParser != null){
			for (int i = 0; i < dataParser.length; i++) {
				DataExtractorConfig dataExtractorConfig = dataParser[i];
				JSONObject _data = (JSONObject) currentPageReq.getAttribute("_data");
				if (dataExtractorConfig.condition == null || runtimeContext.parse(dataExtractorConfig.condition).equals("true")){
					responseJson = extractor.parseData(dataExtractorConfig.parseConfig, page.getContent());
					if (responseJson == null){
						continue;
					}
					if (responseJson.startsWith("{") || responseJson.startsWith("[")){
						JSON result = (JSON) JSON.parse(responseJson);
						writeTemplates(result, runtimeContext, dataExtractorConfig.templates);
						copy(_data, result);
						if (dataExtractorConfig.unique != null){
							result = filter(result, dataExtractorConfig.unique);
						}
						if (result != null){
							writeObject(runtimeContext, objectContainer, page.getRequest().getUrl(), result, dataExtractorConfig.update);
						}
					}
				}
			}
		}
		
		if (requestParser != null){
			for (int i = 0; i < requestParser.length; i++) {
				RequestExtractorConfig requestExtractorConfig = requestParser[i];
				if (requestExtractorConfig.dataContext != null){
					JSON contextData = (JSON) runtimeContext.get(requestExtractorConfig.dataContext.key);
					if (contextData != null){
						if (contextData instanceof JSONObject) {
							JSONArray temp = new JSONArray();
							temp.add(contextData);
							contextData = temp;
						}
						if (contextData instanceof JSONArray) {
							JSONArray requestDataArr = (JSONArray) contextData;
							for (int j = 0; j < requestDataArr.size(); j++) {
								runtimeContext.setDataContext((Map)requestDataArr.get(j));
								JSONObject jsonObject = new JSONObject();
								writeTemplates(jsonObject, runtimeContext, requestExtractorConfig.templates);
								if (requestExtractorConfig.unique != null){
									jsonObject = (JSONObject) filter(jsonObject, requestExtractorConfig.unique);
								}
								if (jsonObject != null){
									PageRequest req = createRequest(runtimeContext, requestExtractorConfig, jsonObject).get(0);
									if (req != null){
										if (requestExtractorConfig.dataContext.data_flow){
											req.addAttribute("_data", requestDataArr.get(j));
										}
										queue.add(req);
									}
								}
								runtimeContext.setDataContextNull();
							}
						}
					}
				}else if (requestExtractorConfig.condition == null || runtimeContext.parse(requestExtractorConfig.condition).equals("true")){
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
							List<PageRequest> resps = createRequest(runtimeContext, requestExtractorConfig, data);
							if (resps != null){
								queue.addAll(resps);
							}
						}
					}
				}
			}
		}
		return runtimeContext;
	}
	
	private DBObject buildUpdateQuery(Map<String,String> update,RuntimeContext runtimeContext) throws IOException{
		if (update == null || update.isEmpty()){
			return null;
		}
		BasicDBObject dbObject = new BasicDBObject();
		for (Entry<String,String> entry : update.entrySet()) {
			dbObject.put(entry.getKey(), runtimeContext.parse(entry.getValue()));
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
				body = new BasicDBObject(map);
				updateQuery = buildUpdateQuery(update, runtimeContext);
				objectContainer.add(new CrawlData(taskId, url, body, updateQuery));
			}
		}else{
			Map<String, Object> map = fixData((JSONObject) data);
			body = new BasicDBObject(map);
			updateQuery = buildUpdateQuery(update, runtimeContext);
			objectContainer.add(new CrawlData(taskId, url, body, updateQuery));
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
	
	private List<PageRequest> createRequest(RuntimeContext runtimeContext,RequestExtractorConfig requestExtractorConfig,JSON data) throws IOException{
		if (data instanceof JSONObject){
			JSONObject dataObj = (JSONObject) data;
			if (dataObj.getString("url") == null){
				return null;
			}
			PageRequest req = RequestBuilder.createPageRequest(dataObj.getString("url"), requestExtractorConfig.processor);
			if (dataObj.containsKey("attribute")){
				Map<String,Object> attribute = (Map<String, Object>) dataObj.get("attribute");
				for (Entry<String, Object> pair:attribute.entrySet()) {
					String value = (String) pair.getValue();
					if (value == null || (value = value.trim()).isEmpty()){
						logger.warn(String.format("extractor attr error %s",req.getUrl()));
						return null;
					}
					req.addAttribute(pair.getKey(), pair.getValue());
				}
			}
			req.setMethod(requestExtractorConfig.method);
			if (requestExtractorConfig.headers != null){
				for(Entry<String, String> pair : requestExtractorConfig.headers.entrySet()){
					String value = pair.getValue();
					req.putHeader(pair.getKey(), runtimeContext.parse(value));
				}
			}
			if (requestExtractorConfig.params != null){
				for(Entry<String, String> pair : requestExtractorConfig.params.entrySet()){
					String value = pair.getValue();
					req.putParams(pair.getKey(), runtimeContext.parse(value));
				}
			}
			req.setPriority(requestExtractorConfig.priority);
			return Arrays.asList(req);
		}else{
			List<PageRequest> reqs = new ArrayList<PageRequest>();
			JSONArray dataArr = (JSONArray) data;
			for (int i = 0; i < dataArr.size(); i++) {
				List<PageRequest> oneReq = createRequest(runtimeContext, requestExtractorConfig, dataArr.getJSONObject(i));
				if (oneReq != null){
					reqs.add(oneReq.get(0));
				}
			}
			return reqs;
		}
	}
	
	private final static void copy(JSONObject from,JSON to){
		if (from != null){
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
					copy(from, destArr.getJSONObject(i));
				}
			}
		}
	}
	
}
