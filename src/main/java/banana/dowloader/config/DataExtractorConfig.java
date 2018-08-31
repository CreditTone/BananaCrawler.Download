package banana.dowloader.config;

import java.util.HashMap;
import java.util.Map;

public class DataExtractorConfig {
	
	public String condition;
	
	public Object filter_field;//{"or":["userid","location"]},{"and":["userid","location"]},"userid"
	
	public Map<String, Object> extractorConfig;
	
	public DataExtractorConfig(HashMap<String,Object> configObj) {
		configObj = (HashMap<String, Object>) configObj.clone();
		condition = (String) configObj.remove("_condition");
		filter_field = (Map<String, Object>) configObj.remove("_filter_field");
		extractorConfig = configObj;
	}
	
}
