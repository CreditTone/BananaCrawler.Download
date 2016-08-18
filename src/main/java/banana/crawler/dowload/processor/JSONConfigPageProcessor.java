package banana.crawler.dowload.processor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import banana.core.modle.CrawlData;
import banana.core.processor.PageProcessor;
import banana.core.protocol.Extractor;
import banana.core.protocol.Task;
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
			for (int i = 0 ;i < config.getCrawler_data().length ;i++) {
				dataParser[i] = JSON.toJSONString(config.getCrawler_data()[i]);
			}
		}
		if (config.getCrawler_request() != null){
			this.requestParser = new String[config.getCrawler_request().length];
			HashMap<String,String> urlParseConfig = new HashMap<String,String>();
			for (int i = 0 ;i < config.getCrawler_request().length ;i++) {
				urlParseConfig.put("url",(String) config.getCrawler_request()[i].get("url"));
				requestParser[i] = JSON.toJSONString(urlParseConfig);
			}
		}
		
	}


	@Override
	public void process(Page page, StartContext context, List<HttpRequest> queue, List<CrawlData> objectContainer)
			throws Exception {
		String responseJson = null;
		
		if (dataParser != null){
			for (int i = 0; i < dataParser.length; i++) {
				responseJson = extractor.parseData(dataParser[i], page.getContent());
				if (responseJson.startsWith("{")){
					objectContainer.add(new CrawlData(taskId, page.getRequest().getUrl(), responseJson));
				}else if(responseJson.startsWith("[")){
					JSONArray jsonArray = JSON.parseArray(responseJson);
					JSONObject json = null;
					for (int j = 0; j < jsonArray.size(); j++) {
						json = jsonArray.getJSONObject(j);
						objectContainer.add(new CrawlData(taskId, page.getRequest().getUrl(), json.toJSONString()));
					}
				}
			}
		}
		
		if (requestParser != null){
			PageRequest currentPageReq = (PageRequest) page.getRequest();
			PageRequest req = null;
			for (int i = 0; i < requestParser.length; i++) {
				responseJson = extractor.parseData(requestParser[i], page.getContent());
				if (responseJson.startsWith("{")){
					JSONObject jsonObject = JSON.parseObject(responseJson);
					req = convertRequest(context, config.getCrawler_request()[i], jsonObject);
					if (req != null){
						fixUrl(currentPageReq, req);
						queue.add(req);
					}
				}else if(responseJson.startsWith("[")){
					JSONArray jsonArray = JSON.parseArray(responseJson);
					for (int j = 0; j < jsonArray.size(); j++) {
						req = convertRequest(context, config.getCrawler_request()[i], jsonArray.getJSONObject(j));
						if (req != null){
							fixUrl(currentPageReq, req);
							queue.add(req);
						}
					}
				}
			}
		}
		
	}
	
	private void fixUrl(PageRequest currentPageReq,PageRequest newReq){
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
		return req;
	}


}
