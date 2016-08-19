package banana.crawler.dowload.processor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import banana.core.modle.CrawlData;
import banana.core.processor.PageProcessor;
import banana.core.protocol.Extractor;
import banana.core.protocol.Task;
import banana.core.protocol.Task.CrawlerData;
import banana.core.protocol.Task.CrawlerRequest;
import banana.core.protocol.Task.Processor;
import banana.core.request.HttpRequest;
import banana.core.request.HttpRequest.Method;
import banana.core.request.PageRequest;
import banana.core.request.StartContext;
import banana.core.response.Page;

public class JSONConfigPageProcessor implements PageProcessor {
	
	private Task.Processor config;
	
	private String[] dataParser;
	
	private String[] requestParser;
	
	private Extractor extractor;
	
	private String taskId;
	
	public JSONConfigPageProcessor(String taskId,Processor config,Extractor extractor) {
		this.config = config;
		this.extractor = extractor;
		this.taskId = taskId;
		if (config.getCrawler_data() != null){
			this.dataParser = new String[config.getCrawler_data().length];
			HashMap<String,Object> dataParseConfig = new HashMap<String,Object>();
			for (int i = 0 ;i < config.getCrawler_data().length ;i++) {
				Set<String> keys = config.getCrawler_data()[i].keySet();
				for (String key : keys) {
					dataParseConfig.put(key, config.getCrawler_data()[i].get(key));
				}
				dataParser[i] = JSON.toJSONString(dataParseConfig);
				dataParseConfig.clear();
			}
		}
		if (config.getCrawler_request() != null){
			this.requestParser = new String[config.getCrawler_request().length];
			HashMap<String,Object> urlParseConfig = new HashMap<String,Object>();
			for (int i = 0 ;i < config.getCrawler_request().length ;i++) {
				urlParseConfig.put("url",(String) config.getCrawler_request()[i].get("url"));
				if (config.getCrawler_request()[i].containsKey("attribute")){
					Object attributeDefine = config.getCrawler_request()[i].get("attribute");
					urlParseConfig.put("attribute",attributeDefine);
				}
				requestParser[i] = JSON.toJSONString(urlParseConfig);
				urlParseConfig.clear();
			}
		}
	}


	@Override
	public void process(Page page, StartContext context, List<HttpRequest> queue, List<CrawlData> objectContainer)
			throws Exception {
		String responseJson = null;
		PageRequest currentPageReq = (PageRequest) page.getRequest();
		if (dataParser != null){
			for (int i = 0; i < dataParser.length; i++) {
				responseJson = extractor.parseData(dataParser[i], page.getContent());
				if (responseJson.startsWith("{")){
					JSONObject json = JSON.parseObject(responseJson);
					dataFollowAttribute(currentPageReq, context, config.getCrawler_data()[i], json);
					objectContainer.add(new CrawlData(taskId, page.getRequest().getUrl(), json.toJSONString()));
				}else if(responseJson.startsWith("[")){
					JSONArray jsonArray = JSON.parseArray(responseJson);
					JSONObject json = null;
					for (int j = 0; j < jsonArray.size(); j++) {
						json = jsonArray.getJSONObject(j);
						dataFollowAttribute(currentPageReq, context, config.getCrawler_data()[i], json);
						objectContainer.add(new CrawlData(taskId, page.getRequest().getUrl(), json.toJSONString()));
					}
				}
			}
		}
		
		if (requestParser != null){
			PageRequest req = null;
			for (int i = 0; i < requestParser.length; i++) {
				responseJson = extractor.parseData(requestParser[i], page.getContent());
				if (responseJson.startsWith("{")){
					JSONObject jsonObject = JSON.parseObject(responseJson);
					req = convertRequest(context, config.getCrawler_request()[i], jsonObject);
					if (req != null){
						fixUrlAndFollowAttribute(currentPageReq, req);
						queue.add(req);
					}
				}else if(responseJson.startsWith("[")){
					JSONArray jsonArray = JSON.parseArray(responseJson);
					for (int j = 0; j < jsonArray.size(); j++) {
						req = convertRequest(context, config.getCrawler_request()[i], jsonArray.getJSONObject(j));
						if (req != null){
							fixUrlAndFollowAttribute(currentPageReq, req);
							queue.add(req);
						}
					}
				}
			}
		}
		
	}
	
	private void dataFollowAttribute(PageRequest currentPageReq,StartContext context,CrawlerData config,JSONObject data){
		for (Entry<String,Object> pair : config.getCite().entrySet()) {
			String valueCite = pair.getValue().toString();
			Object value = null;
			if (valueCite.startsWith("$request.")){
				value = currentPageReq.getAttribute(valueCite.substring(9, valueCite.length()));
			}else if (valueCite.startsWith("$context.")){
				value = context.getContextAttribute(valueCite.substring(9, valueCite.length()));
			}else{
				continue;
			}
			data.put(pair.getKey(), value);
		}
	}
	
	private void fixUrlAndFollowAttribute(PageRequest currentPageReq,PageRequest newReq){
		for (String key : currentPageReq.enumAttributeNames()) {
			newReq.addAttribute(key, currentPageReq.getAttribute(key));
		}
		if (newReq.getUrl().startsWith("http"))
			return;
		if (newReq.getUrl().startsWith("?")){
			String baseUrl = currentPageReq.getUrl().split("\\?", 2)[0];
			newReq.setUrl(baseUrl + newReq.getUrl());
		}else if (newReq.getUrl().startsWith("//")){
			newReq.setUrl("https:" + newReq.getUrl());
		}else if(newReq.getUrl().startsWith("/")){
			int index = currentPageReq.getUrl().indexOf("/",7);
			String baseUrl = currentPageReq.getUrl().substring(0, index);
			newReq.setUrl(baseUrl + newReq.getUrl());
		}else{
			newReq.setUrl(currentPageReq.getUrl() + newReq.getUrl());
		}
	}
	
	private PageRequest convertRequest(StartContext context,CrawlerRequest config,JSONObject jsonObject){
		if (jsonObject.getString("url") == null){
			return null;
		}
		String processor = (String) config.get("processor");
		PageRequest req = context.createPageRequest(jsonObject.getString("url"), processor);
		if (config.containsKey("method") && "POST".equalsIgnoreCase((String) config.get("method"))){
			req.setMethod(Method.POST);
		}
		if (config.containsKey("headers")){
			Map<String,String> headers = (Map<String, String>) config.get("headers");
			for(Entry<String, String> pair:headers.entrySet()){
				req.putHeader(pair.getKey(), (String) pair.getValue());
			}
		}
		if (config.containsKey("params")){
			Map<String,String> params = (Map<String, String>) config.get("params");
			for(Entry<String, String> pair:params.entrySet()){
				req.putParams(pair.getKey(), (String) pair.getValue());
			}
		}
		if (config.containsKey("priority")){
			req.setPriority((int) config.get("priority"));
		}
		if (jsonObject.containsKey("attribute")){
			Map<String,Object> attribute = (Map<String, Object>) jsonObject.get("attribute");
			for (Entry<String, Object> pair:attribute.entrySet()) {
				req.addAttribute(pair.getKey(), pair.getValue());
			}
		}
		return req;
	}


}
