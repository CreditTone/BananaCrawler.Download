package banana.crawler.dowload.processor;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.log4j.Logger;

import java.util.Set;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import banana.core.modle.CrawlData;
import banana.core.processor.Extractor;
import banana.core.processor.PageProcessor;
import banana.core.protocol.Task;
import banana.core.protocol.Task.CrawlerData;
import banana.core.protocol.Task.CrawlerRequest;
import banana.core.protocol.Task.ExpandableHashMap;
import banana.core.protocol.Task.Processor;
import banana.core.request.HttpRequest;
import banana.core.request.HttpRequest.Method;
import banana.core.request.PageRequest;
import banana.core.request.RequestBuilder;
import banana.core.request.StartContext;
import banana.core.response.Page;
import banana.crawler.dowload.impl.DownloadServer;

public class JSONConfigPageProcessor implements PageProcessor {
	
	public static final int RUN_MODE = 0;
	
	public static final int TEST_MODE = 1;
	
	public static int MODE = RUN_MODE;
	
	private static Logger logger = Logger.getLogger(JSONConfigPageProcessor.class);
	
	private Task.Processor config;
	
	private String direct;
	
	private String define;
	
	private Map<String,String> page_context_define;
	
	private Map<String,String> task_context_define;
	
	private ExtractorParseConfig[] dataParser;
	
	private ExtractorParseConfig[] requestParser;
	
	private Extractor extractor;
	
	private String taskId;
	
	public JSONConfigPageProcessor(String taskId,Processor config,Extractor extractor) {
		this.config = config;
		this.extractor = extractor;
		this.taskId = taskId;
		
		if (config.getContent() != null){
			if (config.getContent().direct != null) {
				direct = JSON.toJSONString(config.getContent().direct);
			}
			if (config.getContent().define != null) {
				define = JSON.toJSONString(config.getContent().define);
			}
		}
		
		if (config.getPage_context() != null){
			page_context_define = new HashMap<String,String>();
			for (Entry<String, Object> entry : config.getPage_context().entrySet()) {
				if (entry.getValue() instanceof String){
					page_context_define.put(entry.getKey(), (String) entry.getValue());
				}else{
					page_context_define.put(entry.getKey(), JSON.toJSONString(entry.getValue()));
				}
			}
		}
		
		if (config.getTask_context() != null){
			task_context_define = new HashMap<String,String>();
			for (Entry<String, Object> entry : config.getTask_context().entrySet()) {
				if (entry.getValue() instanceof String){
					task_context_define.put(entry.getKey(), (String) entry.getValue());
				}else{
					task_context_define.put(entry.getKey(), JSON.toJSONString(entry.getValue()));
				}
			}
		}
		
		if (config.getCrawler_data() != null) {
			this.dataParser = new ExtractorParseConfig[config.getCrawler_data().length];
			HashMap<String,Object> dataParseConfig = new HashMap<String,Object>();
			for (int i = 0 ;i < config.getCrawler_data().length ;i++) {
				ExpandableHashMap item = config.getCrawler_data()[i];
				ExtractorParseConfig epc = new ExtractorParseConfig();
				Set<String> keys = item.keySet();
				for (String key : keys) {
					if (key.equals("_condition")){
						epc.condition = (String) item.get("_condition");
					}else{
						dataParseConfig.put(key, config.getCrawler_data()[i].get(key));
					}
				}
				epc.body = JSON.toJSONString(dataParseConfig);
				dataParser[i] = epc;
				dataParseConfig.clear();
			}
		}
		if (config.getCrawler_request() != null){
			this.requestParser = new ExtractorParseConfig[config.getCrawler_request().length];
			HashMap<String,Object> urlParseConfig = new HashMap<String,Object>();
			for (int i = 0 ;i < config.getCrawler_request().length ;i++) {
				ExpandableHashMap item = config.getCrawler_request()[i];
				ExtractorParseConfig epc = new ExtractorParseConfig();
				if (item.containsKey("_condition")){
					epc.condition = (String) item.get("_condition");
				}
				String urlXpath = (String) item.get("url");
				if (urlXpath != null){
					urlParseConfig.put("url",urlXpath);
				}
				if (item.containsKey("attribute")){
					Object attributeDefine = item.get("attribute");
					urlParseConfig.put("attribute",attributeDefine);
				}
				if (item.containsKey("_root")){
					Object _root = item.get("_root");
					urlParseConfig.put("_root", _root);
				}
				if (item.containsKey("_type")){
					Object _type = item.get("_type");
					urlParseConfig.put("_type", _type);
				}
				epc.body = JSON.toJSONString(urlParseConfig);
				requestParser[i] = epc;
				urlParseConfig.clear();
			}
		}
	}
	

	@Override
	public void process(Page page, StartContext context, List<HttpRequest> queue, List<CrawlData> objectContainer)
			throws Exception {
		RuntimeContext runtimeContext = RuntimeContext.create(page.getRequest(), context);
		if (direct != null){
			String content = extractor.parseData(direct, page.getContent());
			if (content == null)return;
			page.setContent(content);
		}else if (define != null){
			String content = extractor.parseData(define, page.getContent());
			if (content == null)return;
			page.setContent(content);
		}
		
		if (page_context_define != null){
			for (Entry<String, String> entry : page_context_define.entrySet()) {
				String value = extractor.parseData(entry.getValue(), page.getContent());
				if (value == null){
					return;
				}
				if (value.length() > 0){
					runtimeContext.put(entry.getKey(), value);
					logger.info(String.format("output page_context %s = %s", entry.getKey(),value));
				}
			}
		}
		
		Map<String,JSON> sendRequestData = new HashMap<String,JSON>();
		String responseJson = null;
		PageRequest currentPageReq = (PageRequest) page.getRequest();
		if (dataParser != null){
			for (int i = 0; i < dataParser.length; i++) {
				ExtractorParseConfig epc = dataParser[i];
				CrawlerData cite = config.getCrawler_data()[i];
				JSONObject _data = (JSONObject) currentPageReq.getAttribute("_data");
				if (epc.condition == null || runtimeContext.parse(epc.condition).equals("true")){
					responseJson = extractor.parseData(epc.body, page.getContent());
					if (responseJson == null){
						continue;
					}
					if (responseJson.startsWith("{")){
						JSONObject json = JSON.parseObject(responseJson);
						dataFollowYingYong(runtimeContext, cite, json);
						copy(_data, json);
						if (!dataCrawled(cite, json)){
							if (cite.getSendRequest() != null){
								for (String sendTag : cite.getSendRequest()) {
									sendRequestData.put(sendTag, json);
								}
							}else{
								objectContainer.add(new CrawlData(taskId, page.getRequest().getUrl(), json.toJSONString()));
							}
						}
					}else if(responseJson.startsWith("[")){
						JSONArray jsonArray = JSON.parseArray(responseJson);
						JSONArray filtedArray = new JSONArray();
						JSONObject json = null;
						for (int j = 0; j < jsonArray.size(); j++) {
							json = jsonArray.getJSONObject(j);
							dataFollowYingYong(runtimeContext, cite, json);
							copy(_data, json);
							if (!dataCrawled(cite, json)){
								filtedArray.add(json);
							}
						}
						if (cite.getSendRequest() != null){
							for (String sendTag : cite.getSendRequest()) {
								sendRequestData.put(sendTag, filtedArray);
							}
						}else{
							for (int j = 0; j < filtedArray.size(); j++) {
								json = filtedArray.getJSONObject(j);
								objectContainer.add(new CrawlData(taskId, page.getRequest().getUrl(), json.toJSONString()));
							}
						}
					}
				}
			}
		}
		
		if (requestParser != null){
			PageRequest req = null;
			for (int i = 0; i < requestParser.length; i++) {
				ExtractorParseConfig epc = requestParser[i];
				CrawlerRequest cite = config.getCrawler_request()[i];
				if (cite.getTag() != null && sendRequestData.containsKey(cite.getTag())){
					String urlDefine = (String) cite.getCite().get("url");
					JSON requestData = sendRequestData.get(cite.getTag());
					if (requestData != null){
						if (requestData instanceof JSONObject) {
							runtimeContext.setDataContext((Map)requestData);
							String url = runtimeContext.parse(urlDefine);
							JSONObject jsonObject = new JSONObject();
							jsonObject.put("url", url);
							req = createRequest(runtimeContext, cite, jsonObject);
							if (req != null){
								req.addAttribute("_data", requestData);
								queue.add(req);
							}
							runtimeContext.setDataContextNull();
						}else if (requestData instanceof JSONArray) {
							JSONArray requestDataArr = (JSONArray) requestData;
							for (int j = 0; j < requestDataArr.size(); j++) {
								runtimeContext.setDataContext((Map)requestDataArr.get(j));
								String url = runtimeContext.parse(urlDefine);
								JSONObject jsonObject = new JSONObject();
								jsonObject.put("url", url);
								req = createRequest(runtimeContext, cite, jsonObject);
								if (req != null){
									req.addAttribute("_data", requestDataArr.get(j));
									queue.add(req);
								}
								runtimeContext.setDataContextNull();
							}
						}
					}else{
						String url = runtimeContext.parse(urlDefine);
						JSONObject jsonObject = new JSONObject();
						jsonObject.put("url", url);
						req = createRequest(runtimeContext, cite, jsonObject);
						if (req != null){
							queue.add(req);
						}
					}
				}else if (epc.condition == null || runtimeContext.parse(epc.condition).equals("true")){
					responseJson = extractor.parseData(epc.body, page.getContent());
					if (responseJson == null){
						continue;
					}
					if (responseJson.startsWith("{")){
						JSONObject jsonObject = JSON.parseObject(responseJson);
						dataFollowYingYong(runtimeContext, cite, jsonObject);
						req = createRequest(runtimeContext, cite, jsonObject);
						if (req != null){
							queue.add(req);
						}
					}else if(responseJson.startsWith("[")){
						JSONArray jsonArray = JSON.parseArray(responseJson);
						for (int j = 0; j < jsonArray.size(); j++) {
							JSONObject jsonObject = jsonArray.getJSONObject(j);
							dataFollowYingYong(runtimeContext, cite, jsonObject);
							req = createRequest(runtimeContext, cite, jsonObject);
							if (req != null){
								queue.add(req);
							}
						}
					}
				}
			}
		}
	}
	
	private final boolean dataCrawled(ExpandableHashMap config,JSONObject jsonObject){
		if (config.getUnique() == null || MODE == TEST_MODE){
			return false;
		}
		String[] fields = new String[config.getUnique().size()];
		for (int i = 0; i < fields.length; i++) {
			fields[i] = jsonObject.getString(config.getUnique(i));
		}
		boolean exists = DownloadServer.getInstance().getMasterServer().filterQuery(taskId, fields).get();
		return exists;
	}
	
	private final void dataFollowYingYong(RuntimeContext runtimeContext,ExpandableHashMap config,JSONObject data) throws IOException{
		if (config.getCite().isEmpty()){
			return;
		}
		for (Entry<String,Object> pair : config.getCite().entrySet()) {
			String valueCite = pair.getValue().toString();
			String result = runtimeContext.parse(valueCite);
			if (!result.isEmpty()){
				data.put(pair.getKey(), result);
				continue;
			}
		}
	}
	
	private PageRequest createRequest(RuntimeContext runtimeContext,CrawlerRequest config,JSONObject jsonObject) throws IOException{
		if (jsonObject.getString("url") == null){
			return null;
		}
		String processor = (String) config.get("processor");
		PageRequest req = RequestBuilder.createPageRequest(jsonObject.getString("url"), processor);
		if (jsonObject.containsKey("attribute")){
			Map<String,Object> attribute = (Map<String, Object>) jsonObject.get("attribute");
			for (Entry<String, Object> pair:attribute.entrySet()) {
				String value = (String) pair.getValue();
				if (value == null || (value = value.trim()).isEmpty()){
					logger.warn(String.format("extractor attr error %s",req.getUrl()));
					return null;
				}
				req.addAttribute(pair.getKey(), pair.getValue());
			}
		}
		if (config.containsKey("method") && "POST".equalsIgnoreCase((String) config.get("method"))){
			req.setMethod(Method.POST);
		}
		if (config.containsKey("headers")){
			Map<String,String> headers = (Map<String, String>) config.get("headers");
			for(Entry<String, String> pair:headers.entrySet()){
				String value = pair.getValue();
				req.putHeader(pair.getKey(), runtimeContext.parse(value));
			}
		}
		if (config.containsKey("params")){
			Map<String,String> params = (Map<String, String>) config.get("params");
			for(Entry<String, String> pair:params.entrySet()){
				String value = pair.getValue();
				req.putParams(pair.getKey(), runtimeContext.parse(value));
			}
		}
		if (config.containsKey("priority")){
			req.setPriority((int) config.get("priority"));
		}
		return req;
	}
	
	private final static void copy(JSONObject from,JSONObject to){
		if (from != null){
			for (Entry<String,Object> entry : from.entrySet()) {
				if (to.containsKey(entry.getKey())){
					continue;
				}
				to.put(entry.getKey(), entry.getValue());
			}
		}
	}
	
	private final class ExtractorParseConfig{
		
		public String condition;
		
		public String body;
		
	}

}
