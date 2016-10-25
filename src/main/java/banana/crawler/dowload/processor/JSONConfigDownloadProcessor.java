package banana.crawler.dowload.processor;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.log4j.Logger;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.PutObjectResult;

import banana.core.processor.BinaryProcessor;
import banana.core.request.BinaryRequest;
import banana.core.request.HttpRequest;
import banana.core.response.StreamResponse;

public class JSONConfigDownloadProcessor implements BinaryProcessor {
	
	private static Logger logger = Logger.getLogger(JSONConfigDownloadProcessor.class);
	
	OSSClient client = new OSSClient("http://oss-cn-qingdao.aliyuncs.com", "LTAIFJ29bKun4hUL", "ixUaDDa8kot5wZKsdxhcSIpFVErFgO");

	private String workDir = new SimpleDateFormat("yyyyMMdd").format(new Date());
	
	private int dayOfMonth = 0;
	
	public JSONConfigDownloadProcessor() {
		ObjectListing ol = client.listObjects("japan2music", "Japan/20161025/");
		System.out.println(ol.getObjectSummaries().get(0).getKey());
	}
	
	public static void main(String[] args) {
		new JSONConfigDownloadProcessor();
	}
	
	@Override
	public void process(StreamResponse stream) {
		checkDayOfMonth();
		HttpRequest binaryRequest = stream.getRequest();
		String filename = binaryRequest.getAttribute("music")+"_"+binaryRequest.getAttribute("zuozhe");
		if (binaryRequest.getUrl().equals("mp3")){
			filename+=".mp3";
		}else{
			filename+=".acc";
		}
		InputStream in = new ByteArrayInputStream(stream.getBody());
		PutObjectResult result = client.putObject("japan2music", "Japan/"+workDir+"/"+filename, in);
		logger.info("put object " +result.getRequestId());
		try {
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private final synchronized boolean checkDayOfMonth(){
		if (dayOfMonth != Calendar.getInstance().get(Calendar.DAY_OF_MONTH)){
			workDir = new SimpleDateFormat("yyyyMMdd").format(new Date());
			dayOfMonth = Calendar.getInstance().get(Calendar.DAY_OF_MONTH);
			return true;
		}
		return false;
	}

}
