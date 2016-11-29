package banana.dowloader.config;

import java.util.Arrays;
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
	
	public Map<String,String> update;
	
	public DataExtractorConfig(Object configObj) {
		if (configObj instanceof Map){
			isMap = true;
			Map<String,Object> config = (Map<String, Object>) configObj;
			condition = (String) config.get("_condition");
			update = (Map<String, String>) config.get("_update");
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
		}else if (configObj instanceof String){
			parseConfig = JSON.toJSONString(Arrays.asList(configObj));
		}
	}
	
}
