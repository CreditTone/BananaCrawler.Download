package banana.crawler.dowload.processor;

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
import banana.core.request.BasicRequest;
import banana.core.request.HttpRequest.Method;
import banana.core.request.PageRequest;
import banana.core.request.StartContext;
import banana.core.response.Page;

public class JSONConfigPageProcessor implements PageProcessor {
	
	private Task.Processor config;
	
	private String[] dataParser;
	
	private String[] requestParser;
	
	private Extractor extractor;
	
	public JSONConfigPageProcessor(Processor config,Extractor extractor) {
		this.config = config;
		this.extractor = extractor;
		this.dataParser = new String[config.getCrawler_data().length];
		for (int i = 0 ;i < config.getCrawler_data().length ;i++) {
			dataParser[i] = JSON.toJSONString(config.getCrawler_data()[i]);
		}
		this.requestParser = new String[config.getCrawler_request().length];
		for (int i = 0 ;i < config.getCrawler_request().length ;i++) {
			requestParser[i] = JSON.toJSONString(config.getCrawler_request()[i]);
		}
	}


	@Override
	public void process(Page page, StartContext context, List<BasicRequest> queue, List<CrawlData> objectContainer)
			throws Exception {
		PageRequest currentPageReq = (PageRequest) page.getRequest();
		String responseJson = null;
		for (int i = 0; i < dataParser.length; i++) {
			responseJson = extractor.parseData(dataParser[i], page.getContent());
			System.out.println("parse data:" + responseJson);
		}
		
		PageRequest req = null;
		for (int i = 0; i < requestParser.length; i++) {
			responseJson = extractor.parseData(requestParser[i], page.getContent());
			if (responseJson.startsWith("{")){
				JSONObject jsonObject = JSON.parseObject(responseJson);
				req = convertRequest(context, config.getCrawler_request()[i], jsonObject);
				fixUrl(currentPageReq, req);
				queue.add(req);
			}else if(responseJson.startsWith("[")){
				JSONArray jsonArray = JSON.parseArray(responseJson);
				for (int j = 0; j < jsonArray.size(); j++) {
					req = convertRequest(context, config.getCrawler_request()[i], jsonArray.getJSONObject(j));
					fixUrl(currentPageReq, req);
					queue.add(req);
				}
			}
		}
		
	}
	
	private void fixUrl(PageRequest currentPageReq,PageRequest newReq){
		if (newReq.getUrl().startsWith("?")){
			String baseUrl = currentPageReq.getUrl().split("\\?", 2)[0];
			newReq.setUrl(baseUrl + newReq.getUrl());
		}else if (newReq.getUrl().startsWith("//")){
			newReq.setUrl("https:" + newReq.getUrl());
		}else if(newReq.getUrl().startsWith("/")){
			int lastIndex = currentPageReq.getUrl().lastIndexOf("/");
			String baseUrl = currentPageReq.getUrl().substring(0, lastIndex);
			newReq.setUrl(baseUrl + newReq.getUrl());
		}else if(!newReq.getUrl().startsWith("http")){
			newReq.setUrl(currentPageReq.getUrl() + newReq.getUrl());
		}
	}
	
	private PageRequest convertRequest(StartContext context,CrawlerRequest config,JSONObject jsonObject){
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
		return req;
	}

}
