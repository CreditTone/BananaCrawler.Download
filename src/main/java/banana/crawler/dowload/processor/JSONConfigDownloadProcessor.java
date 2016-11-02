package banana.crawler.dowload.processor;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import banana.core.processor.BinaryProcessor;
import banana.core.request.HttpRequest;
import banana.core.response.StreamResponse;

public class JSONConfigDownloadProcessor implements BinaryProcessor {
	
	private static Logger logger = Logger.getLogger(JSONConfigDownloadProcessor.class);
	
	//OSSClient client = new OSSClient("http://oss-cn-qingdao.aliyuncs.com", "LTAIFJ29bKun4hUL", "ixUaDDa8kot5wZKsdxhcSIpFVErFgO");

	private String workDir = new SimpleDateFormat("yyyyMMdd").format(new Date());
	
	private int dayOfMonth = 0;
	
	public JSONConfigDownloadProcessor() {
	}
	
	public static void main(String[] args) {
		new JSONConfigDownloadProcessor();
	}
	
	@Override
	public void process(StreamResponse stream) {
		checkDayOfMonth();
		HttpRequest binaryRequest = stream.getRequest();
		String filename = binaryRequest.getAttribute("music")+"_"+binaryRequest.getAttribute("zuozhe");
		filename = filename.replaceAll("/", "").replaceAll("\"", "").replaceAll("-", "").replaceAll("#", "");
		if (binaryRequest.getUrl().equals("mp3")){
			filename+=".mp3";
		}else{
			filename+=".aac";
		}
		//PutObjectResult result = client.putObject("japan2music", "Japan/"+workDir+"/"+filename, in);
		//logger.info("put object " +result.getRequestId());
		try {
			FileUtils.writeByteArrayToFile(new File("/data/"+workDir+"/"+filename), stream.getBody());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private final synchronized boolean checkDayOfMonth(){
		if (dayOfMonth != Calendar.getInstance().get(Calendar.DAY_OF_MONTH)){
			workDir = new SimpleDateFormat("yyyyMMdd").format(new Date());
			dayOfMonth = Calendar.getInstance().get(Calendar.DAY_OF_MONTH);
			new File("/data/"+workDir).mkdirs();
			return true;
		}
		return false;
	}

}
