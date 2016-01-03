package banana.crawler.dowload.impl;

import java.io.File;
import java.io.IOException;
import java.rmi.Naming;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.banana.common.master.ICrawlerMasterServer;

import net.sf.json.JSONObject;



public class Properties {

	private static Logger logger = Logger.getLogger(Properties.class);
	
	private static Map<String,Object> properties = new HashMap<String,Object>();
	
	static{
		try {
			String body = FileUtils.readFileToString(new File("config.json"));
			JSONObject json = JSONObject.fromObject(body);
			Set<String> keySet = json.keySet();
			for (String key : keySet) {
				properties.put(key, json.get(key));
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static Object getConfigPropertie(String propertie){
		return properties.get(propertie);
	}
	
	public static Object getTaskPropertie(String taskName,String propertie){
		String key = taskName + "_" + propertie;
		Object value = properties.get(key);
		if (value != null){
			return value;
		}
		try {
			value = DownloadServer.getInstance().getMasterServer().getTaskPropertie(taskName, propertie);
			properties.put(key, value);
		}catch (Exception e) {
			logger.warn("get propertie error",e);
		}
		return value;
	}
	
}
