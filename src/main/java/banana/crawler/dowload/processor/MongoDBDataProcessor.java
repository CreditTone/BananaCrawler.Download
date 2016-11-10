package banana.crawler.dowload.processor;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import banana.core.modle.CrawlData;
import banana.core.modle.MasterConfig;
import banana.core.processor.DataProcessor;

public class MongoDBDataProcessor implements DataProcessor {
	
	private static Logger logger = Logger.getLogger(MongoDBDataProcessor.class);

	private DB db;
	
	private static Set<String> collections = new HashSet<String>();

	public MongoDBDataProcessor(MasterConfig.MongoDB mongodb) throws NumberFormatException, UnknownHostException {
		MongoClient client = null;
		ServerAddress serverAddress = new ServerAddress(mongodb.host, mongodb.port);
		List<ServerAddress> seeds = new ArrayList<ServerAddress>();
		seeds.add(serverAddress);
		MongoCredential credentials = MongoCredential.createCredential(mongodb.username, mongodb.db,
				mongodb.password.toCharArray());
		client = new MongoClient(seeds, Arrays.asList(credentials), getOptions());
		db = client.getDB(mongodb.db);
	}

	private MongoClientOptions getOptions() {
		MongoClientOptions.Builder build = new MongoClientOptions.Builder();
		// 与数据最大连接数50
		build.connectionsPerHost(50);
		// 如果当前所有的connection都在使用中，则每个connection上可以有50个线程排队等待
		build.threadsAllowedToBlockForConnectionMultiplier(50);
		build.connectTimeout(60 * 1000);
		build.maxWaitTime(60 * 1000);
		build.socketTimeout(60 * 1000);
		MongoClientOptions options = build.build();
		return options;
	}

	@Override
	public void process(List<CrawlData> objectContainer,String ... collection) {
		if (!collections.contains(collection[0])){
			if (!db.collectionExists(collection[0])){
				synchronized (this) {
					if (!db.collectionExists(collection[0])){
						db.getCollection(collection[0]).createIndex("_task_name");
					}
				}
			}
			collections.add(collection[0]);
		}
		DBCollection dbCollection = db.getCollection(collection[0]);
		for (CrawlData data : objectContainer) {
			if (data.isUpdate()){
				dbCollection.update(data.getUpdateQuery(), data.getData(), false, true);
				logger.info(String.format("update %s %s",collection[0] ,data.getUpdateQuery()));
			}else{
				dbCollection.insert(data.getData());
			}
		}
	}
	
}
