package banana.dowloader.config;

import java.util.HashMap;
import java.util.List;

import banana.core.extractor.ExtractorConfig;
import banana.core.request.HttpRequest;

public class RequestExtractorConfig {
	
	public String foreach;
	
	public String condition;
	
	public Object filter_field;
	
	public String processor;
	
	public HttpRequest.Method method = HttpRequest.Method.GET;
	
	public int priority;
	
	public List<String> excludes;
	
	public ExtractorConfig extractorConfig;
	
	public RequestExtractorConfig(HashMap<String, Object> config) {
		config = (HashMap<String, Object>) config.clone();
		foreach = (String) config.remove("_foreach");
		condition = (String) config.remove("_condition");
		processor = (String) config.remove("processor");
		if (config.containsKey("method") && "POST".equalsIgnoreCase((String) config.remove("method"))){
			method = HttpRequest.Method.POST;
		}
		filter_field = config.remove("_filter_field");
		excludes = (List<String>) config.remove("_excludes");
		if (config.containsKey("priority")){
			priority = (int) config.remove("priority");
		}
		extractorConfig = new ExtractorConfig(config);
//		Map<String,Object> bodyConfig = new HashMap<String,Object>();
//		String urlXpath = (String) config.get("url");
//		if (urlXpath != null){
//			if (urlXpath.startsWith("http") || urlXpath.contains("{{")){
//				templates.put("url", urlXpath);
//			}else{
//				bodyConfig.put("url", urlXpath);
//			}
//		}
//		String download = (String) config.get("download");
//		if (download != null){
//			if (download.contains("{{")){
//				templates.put("download", download);
//			}else{
//				bodyConfig.put("download", download);
//			}
//		}
//		if (config.containsKey("attribute")){
//			Object attributeDefine = config.get("attribute");
//			bodyConfig.put("attribute",attributeDefine);
//		}
	}

}
