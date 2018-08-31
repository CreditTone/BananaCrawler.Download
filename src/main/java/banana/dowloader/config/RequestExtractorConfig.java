package banana.dowloader.config;

import java.util.HashMap;
import java.util.List;

import banana.core.request.HttpRequest;

public class RequestExtractorConfig {
	
	public String foreach;
	
	public String condition;
	
	public Object filter_field;
	
	public String processor;
	
	public HttpRequest.Method method = HttpRequest.Method.GET;
	
	public int priority;
	
	public List<String> excludes;
	
	public HashMap<String, Object> extractorConfig;
	
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
		extractorConfig = config;
	}

}
