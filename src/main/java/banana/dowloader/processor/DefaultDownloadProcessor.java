package banana.dowloader.processor;

import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.mongodb.BasicDBObject;

import au.com.bytecode.opencsv.CSVReader;
import banana.core.modle.ContextModle;
import banana.core.modle.CrawlData;
import banana.core.processor.IProcessor;
import banana.core.request.HttpRequest;
import banana.core.response.HttpResponse;
import banana.core.response.StreamResponse;
import banana.dowloader.impl.RemoteTaskContext;

public class DefaultDownloadProcessor implements IProcessor {
	
	private static Logger logger = Logger.getLogger(DefaultDownloadProcessor.class);
	
	private String taskId;
	
	private String rootDir;
	
	public DefaultDownloadProcessor(String taskId, String rootDir) {
		this.taskId = taskId;
		this.rootDir = rootDir.isEmpty()?null:rootDir;
	}

	@Override
	public ContextModle process(HttpResponse response, Object taskContext,List<HttpRequest> queue,List<CrawlData> objectContainer) throws Exception {
		StreamResponse stream = (StreamResponse) response;
		RemoteTaskContext remoteTaskContext = (RemoteTaskContext) taskContext;
		RuntimeContext runtimeContext = RuntimeContext.create(stream, remoteTaskContext);
		InputStream input = null;
		if (stream.getRequest().getUrl().contains("alipay_record")){
			for (int i = 0; i < 3; i++) {
				try{
					File file = new File("tmpdata/"+System.currentTimeMillis());
					FileUtils.writeByteArrayToFile(file, stream.getBody());
					Thread.sleep(1000);
					ZipFile zip = new ZipFile(file);
					Enumeration<? extends ZipEntry> enumt = zip.entries();
					input = zip.getInputStream(enumt.nextElement());
					break;
				}catch(ZipException e){
					e.printStackTrace();
				}
				Thread.sleep(1000);
			}
			Reader reader = null;
			reader = new InputStreamReader(input,"GBK");
	        CSVReader csvReader = new CSVReader(reader);
	        List<String[]> list = csvReader.readAll();  
	        for (int i = 5; i < list.size() - 7; i++) {
	        	String[] line = list.get(i);
	        	BasicDBObject dbObject = new BasicDBObject();
	        	for (int j = 0; j < line.length; j++) {
					switch (j){
					case 0:
						dbObject.put("交易号", line[j].trim());
						break;
					case 1:
						dbObject.put("商户订单号", line[j].trim());
						break;
					case 2:
						dbObject.put("交易创建时间", line[j].trim());
						break;
					case 3:
						dbObject.put("付款时间", line[j].trim());
						break;
					case 4:
						dbObject.put("最近修改时间", line[j].trim());
						break;
					case 5:
						dbObject.put("交易来源地", line[j].trim());
						break;
					case 6:
						dbObject.put("类型", line[j].trim());
						break;
					case 7:
						dbObject.put("交易对方", line[j].trim());
						break;
					case 8:
						dbObject.put("商品名称", line[j].trim());
						break;
					case 9:
						dbObject.put("金额(元)", line[j].trim());
						break;
					case 10:
						dbObject.put("收/支", line[j].trim());
						break;
					case 11:
						dbObject.put("交易状态", line[j].trim());
						break;
					case 12:
						dbObject.put("服务费(元)", line[j].trim());
						break;
					case 13:
						dbObject.put("成功退款(元)", line[j].trim());
						break;
					case 14:
						dbObject.put("备注", line[j].trim());
						break;
					case 15:
						dbObject.put("资金状态", line[j].trim());
						break;
					}
				}
	        	dbObject.put("buyerId", runtimeContext.get("buyerId"));
	        	dbObject.put("crawlerId", runtimeContext.get("crawlerId"));
	        	dbObject.put("recodeType", runtimeContext.get("recodeType"));
	        	CrawlData crawlData = new CrawlData(taskId, stream.getOwnerUrl(), dbObject);
	        	objectContainer.add(crawlData);
			}
	        csvReader.close();
		}
		
		String downloadPath = stream.getRequest().getDownloadPath();
		if (rootDir != null){
			downloadPath = rootDir + "/" + downloadPath;
		}
		FileUtils.writeByteArrayToFile(new File(downloadPath), stream.getBody());
		logger.info(String.format("store file %s", downloadPath));
		return runtimeContext;
	}

}
