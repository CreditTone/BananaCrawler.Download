package banana.crawler.dowload.processor;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.github.jknack.handlebars.Handlebars;
import com.github.jknack.handlebars.Helper;
import com.github.jknack.handlebars.Options;
import com.github.jknack.handlebars.Template;

import banana.core.modle.CrawlData;
import banana.core.processor.PageProcessor;
import banana.core.protocol.Extractor;
import banana.core.protocol.Task;
import banana.core.protocol.Task.CrawlerData;
import banana.core.protocol.Task.CrawlerRequest;
import banana.core.protocol.Task.ExpandableHashMap;
import banana.core.protocol.Task.Processor;
import banana.core.request.HttpRequest;
import banana.core.request.HttpRequest.Method;
import banana.core.request.PageRequest;
import banana.core.request.StartContext;
import banana.core.response.Page;

public class JSONConfigPageProcessor implements PageProcessor {
	
	private static final Handlebars handlebars = new ExpandHandlebars();
	
	private Task.Processor config;
	
	private String direct;
	
	private String define;
	
	private Map<String,String> page_context_define;
	
	private Map<String,String> task_context_define;
	
	private String[] dataParser;
	
	private String[] requestParser;
	
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
				String urlXpath = (String) config.getCrawler_request()[i].get("url");
				if (urlXpath != null){
					urlParseConfig.put("url",urlXpath);
				}
				if (config.getCrawler_request()[i].containsKey("attribute")){
					Object attributeDefine = config.getCrawler_request()[i].get("attribute");
					urlParseConfig.put("attribute",attributeDefine);
				}
				if (config.getCrawler_request()[i].containsKey("_root")){
					Object _root = config.getCrawler_request()[i].get("_root");
					urlParseConfig.put("_root", _root);
				}
				if (config.getCrawler_request()[i].containsKey("_type")){
					Object _type = config.getCrawler_request()[i].get("_type");
					urlParseConfig.put("_type", _type);
				}
				requestParser[i] = JSON.toJSONString(urlParseConfig);
				urlParseConfig.clear();
			}
		}
	}
	

	@Override
	public void process(Page page, StartContext context, List<HttpRequest> queue, List<CrawlData> objectContainer)
			throws Exception {
		
		Map<String,Object> pageContext = new HashMap<String,Object>();
		RuntimeContext runtimeContext = new RuntimeContext(page.getRequest().getAttributes(), pageContext);
		
		if (direct != null){
			String content = extractor.parseData(direct, page.getContent());
			page.setContent(content);
		}
		
		if (define != null){
			String content = extractor.parseData(define, page.getContent());
			page.setContent(content);
		}
		
		if (page_context_define != null){
			for (Entry<String, String> entry : page_context_define.entrySet()) {
				String value = extractor.parseData(entry.getValue(), page.getContent());
				if (value.length() > 0){
					pageContext.put(entry.getKey(), value);
				}
			}
		}
		
		String responseJson = null;
		PageRequest currentPageReq = (PageRequest) page.getRequest();
		if (dataParser != null){
			for (int i = 0; i < dataParser.length; i++) {
				responseJson = extractor.parseData(dataParser[i], page.getContent());
				if (responseJson.startsWith("{")){
					JSONObject json = JSON.parseObject(responseJson);
					dataFollowAttribute(runtimeContext, config.getCrawler_data()[i], json);
					objectContainer.add(new CrawlData(taskId, page.getRequest().getUrl(), json.toJSONString()));
				}else if(responseJson.startsWith("[")){
					JSONArray jsonArray = JSON.parseArray(responseJson);
					JSONObject json = null;
					for (int j = 0; j < jsonArray.size(); j++) {
						json = jsonArray.getJSONObject(j);
						dataFollowAttribute(runtimeContext, config.getCrawler_data()[i], json);
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
					dataFollowAttribute(runtimeContext, config.getCrawler_request()[i], jsonObject);
					req = convertRequest(context, config.getCrawler_request()[i], jsonObject);
					if (req != null){
						fixUrlAndFollowAttribute(currentPageReq, req);
						queue.add(req);
					}
				}else if(responseJson.startsWith("[")){
					JSONArray jsonArray = JSON.parseArray(responseJson);
					for (int j = 0; j < jsonArray.size(); j++) {
						JSONObject jsonObject = jsonArray.getJSONObject(j);
						dataFollowAttribute(runtimeContext, config.getCrawler_request()[i], jsonObject);
						req = convertRequest(context, config.getCrawler_request()[i], jsonObject);
						if (req != null){
							fixUrlAndFollowAttribute(currentPageReq, req);
							queue.add(req);
						}
					}
				}
			}
		}
	}
	
	private void dataFollowAttribute(RuntimeContext runtimeContext,ExpandableHashMap config,JSONObject data) throws IOException{
		if (config.getCite().isEmpty()){
			return;
		}
		for (Entry<String,Object> pair : config.getCite().entrySet()) {
			String valueCite = pair.getValue().toString();
			Template template = handlebars.compileInline(valueCite);
			String result = template.apply(runtimeContext);
			if (!result.isEmpty()){
				data.put(pair.getKey(), result);
				continue;
			}
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
				String value = (String) pair.getValue();
				if (value != null && value.length() > 0){
					req.addAttribute(pair.getKey(), pair.getValue());
				}
			}
		}
		return req;
	}
	
	private final class ExtractorParseConfig{
		
		public String condition;
		
		public String body;
		
	}

}
