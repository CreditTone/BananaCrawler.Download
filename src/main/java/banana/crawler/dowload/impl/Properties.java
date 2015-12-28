package banana.crawler.dowload.impl;

import java.io.File;
import java.io.IOException;
import java.rmi.Naming;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.banana.common.master.ICrawlerMasterServer;

import banana.crawler.dowload.SystemPropertie;
import net.sf.json.JSONObject;


public class Properties {

	private static Logger logger = Logger.getLogger(Properties.class);
	
	private static Map<String,Object> properties = new HashMap<String,Object>();
	
	private static SystemPropertie systemPropertie ;
	
	public static String masterAddress ;
	
	static{
		try {
			String body = FileUtils.readFileToString(new File("config.json"));
			systemPropertie = (SystemPropertie) JSONObject.toBean(JSONObject.fromObject(body), SystemPropertie.class);
			masterAddress = "rmi://"+systemPropertie.getMasterHost()+":"+systemPropertie.getMasterPort()+"/MasterServer";
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static Object getPropertie(String taskName,String propertie){
		String key = taskName + "_" + propertie;
		Object value = properties.get(key);
		if (value != null){
			return value;
		}
		try {
			ICrawlerMasterServer crawlerMasterServer = (ICrawlerMasterServer) Naming.lookup(masterAddress);
			value = crawlerMasterServer.getTaskPropertie(taskName, propertie);
			properties.put(propertie, value);
		}catch (Exception e) {
			logger.warn("get propertie error",e);
		}
		return value;
	}
}
