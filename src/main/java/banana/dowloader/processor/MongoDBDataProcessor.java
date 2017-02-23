package banana.dowloader.processor;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.mongodb.DBCollection;

import banana.core.modle.CrawlData;
import banana.core.processor.DataProcessor;
import banana.dowloader.impl.DownloadServer;

public class MongoDBDataProcessor implements DataProcessor {
	
	private static Logger logger = Logger.getLogger(MongoDBDataProcessor.class);

	private static Set<String> collections = new HashSet<String>();

	public MongoDBDataProcessor(){}

	@Override
	public void process(List<CrawlData> objectContainer,String ... collection) {
		if (objectContainer == null){
			return;
		}
		if (!collections.contains(collection[0])){
			if (!DownloadServer.getInstance().db.collectionExists(collection[0])){
				synchronized (this) {
					if (!DownloadServer.getInstance().db.collectionExists(collection[0])){
						DownloadServer.getInstance().db.getCollection(collection[0]).createIndex("_task_name");
					}
				}
			}
			collections.add(collection[0]);
		}
		DBCollection dbCollection = DownloadServer.getInstance().db.getCollection(collection[0]);
		for (CrawlData data : objectContainer) {
			try{
				if (data.isUpdate()){
					dbCollection.update(data.getUpdateQuery(), data.getData(), true, true);
					logger.info(String.format("update %s %s",collection[0] ,data.getUpdateQuery()));
				}else{
					dbCollection.insert(data.getData());
				}
			}catch(Exception e){
				logger.warn("数据写入错误", e);
			}
		}
	}
	
}
