package banana.crawler.dowload.config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;

public class DataExtractorConfig {
	
	public List<String> unique;
	
	public String parseConfig;
	
	public HashMap<String,String> templates = new HashMap<String,String>();
	
	public boolean isMap;
	
	public String condition;
	
	public Update update;
	
	public static class Update{
		
		public Update(Map<String,Object> _update){
			index = (String) _update.get("index");
			value = (String) _update.get("value");
		}
		
		public String index;
		
		public String value;
		
	}

	public DataExtractorConfig(Object configObj) {
		if (configObj instanceof Map){
			isMap = true;
			Map<String,Object> config = (Map<String, Object>) configObj;
			condition = (String) config.get("_condition");
			if (config.containsKey("_update")){
				update = new Update((Map<String, Object>) config.get("_update"));
			}
			unique = (List<String>) config.get("_unique");
			Map<String,Object> bodyConfig = new HashMap<String,Object>();
			for (String key : config.keySet()) {
				if (key.equals("_type") || key.equals("_root")){
					bodyConfig.put(key, config.get(key));
				}else if (!key.startsWith("_")){
					Object value = config.get(key);
					if (value instanceof String && value.toString().contains("{{")){
						templates.put(key, (String) value);
					}else{
						bodyConfig.put(key, value);
					}
				}
			}
			parseConfig = JSON.toJSONString(bodyConfig);
		}else if (configObj instanceof List){
			parseConfig = JSON.toJSONString(configObj);
		}
	}
	
}
