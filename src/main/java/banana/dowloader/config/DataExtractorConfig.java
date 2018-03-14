package banana.dowloader.config;

import java.util.HashMap;
import java.util.Map;

import banana.core.extractor.ExtractorConfig;

public class DataExtractorConfig {
	
	public String condition;
	
	public Object filter_field;//{"or":["userid","location"]},{"and":["userid","location"]},"userid"
	
	public ExtractorConfig extractorConfig;
	
	public DataExtractorConfig(HashMap<String,Object> configObj) {
		configObj = (HashMap<String, Object>) configObj.clone();
		condition = (String) configObj.remove("_condition");
		filter_field = (Map<String, Object>) configObj.remove("_filter_field");
		extractorConfig = new ExtractorConfig(configObj);
	}
	
}
