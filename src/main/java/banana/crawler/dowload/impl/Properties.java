package banana.crawler.dowload.impl;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;


import net.sf.json.JSONObject;



public class Properties {

	private static Logger logger = Logger.getLogger(Properties.class);
	
	private static Map<String,Object> properties = new HashMap<String,Object>();
	
	static{
		try {
			URL url = Properties.class.getClassLoader().getResource("config.json");
			InputStream in = url.openStream();
			byte[] body = new byte[in.available()];
			in.read(body);
			in.close();
			JSONObject json = JSONObject.fromObject(new String(body));
			Set<String> keySet = json.keySet();
			for (String key : keySet) {
				properties.put(key, json.get(key));
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static void main(String[] args) {
		
	}
	
	public static Object getConfigPropertie(String propertie){
		return properties.get(propertie);
	}
}
