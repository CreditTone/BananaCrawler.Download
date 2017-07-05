package banana.dowloader.config;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.alibaba.fastjson.JSON;

import banana.core.request.HttpRequest;

public class RequestExtractorConfig {
	
	public List<String> unique;
	
	public String condition;
	
	public String parseConfig;
	
	public HashMap<String,String> templates = new HashMap<String,String>();
	
	public DataContext dataContext;
	
	public String processor;
	
	public HttpRequest.Method method = HttpRequest.Method.GET;
	
	public HashMap<String,String> params;
	
	public HashMap<String,String> headers;
	
	public int priority;
	
	public List<String> excludes;
	
	public RequestExtractorConfig(HashMap<String, Object> config) {
		condition = (String) config.get("_condition");
		processor = (String) config.get("processor");
		if (config.containsKey("method") && "POST".equalsIgnoreCase((String) config.get("method"))){
			method = HttpRequest.Method.POST;
		}
		if (config.containsKey("params")) {
			params = new HashMap<String, String>();
			Set<Entry<String, Object>> keys = ((com.alibaba.fastjson.JSONObject) config.get("params")).entrySet();
			for (Entry<String,Object> entry : keys) {
				params.put(entry.getKey(), (String)entry.getValue());
			}
		}
		if (config.containsKey("headers")){
			headers = new HashMap<String, String>();
			Set<Entry<String, Object>> keys = ((com.alibaba.fastjson.JSONObject) config.get("headers")).entrySet();
			for (Entry<String,Object> entry : keys) {
				headers.put(entry.getKey(), (String)entry.getValue());
			}
		}
		if(config.containsKey("exclude")){
			excludes = (List<String>) config.get("excludes");
		}
		if (config.containsKey("priority")){
			priority = (int) config.get("priority");
		}
		if (config.containsKey("_datacontext")){
			dataContext = new DataContext((Map<String, Object>) config.get("_datacontext"));
		}
		if (config.containsKey("_unique") && config.get("_unique") != null){
			unique = Arrays.asList("url");
		}
		Map<String,Object> bodyConfig = new HashMap<String,Object>();
		String urlXpath = (String) config.get("url");
		if (urlXpath != null){
			if (urlXpath.startsWith("http") || urlXpath.contains("{{")){
				templates.put("url", urlXpath);
			}else{
				bodyConfig.put("url", urlXpath);
			}
		}
		String download = (String) config.get("download");
		if (download != null){
			if (download.contains("{{")){
				templates.put("download", download);
			}else{
				bodyConfig.put("download", download);
			}
		}
		if (config.containsKey("attribute")){
			Object attributeDefine = config.get("attribute");
			bodyConfig.put("attribute",attributeDefine);
		}
		if (config.containsKey("_root")){
			Object _root = config.get("_root");
			bodyConfig.put("_root", _root);
		}
		if (config.containsKey("_type")){
			Object _type = config.get("_type");
			bodyConfig.put("_type", _type);
		}
		parseConfig = JSON.toJSONString(bodyConfig);
	}

	public static final class DataContext {
		
		public DataContext(Map<String,Object> config){
			key = (String) config.get("key");
			if (config.containsKey("data_flow")){
				data_flow = (boolean) config.get("data_flow");
			}
		}
		
		public String key;
		
		public boolean data_flow;
		
	}
	
}
