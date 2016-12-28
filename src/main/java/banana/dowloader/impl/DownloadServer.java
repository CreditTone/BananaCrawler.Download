package banana.dowloader.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.log4j.Logger;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import banana.core.NodeStatus;
import banana.core.exception.CrawlerMasterException;
import banana.core.exception.DownloadException;
import banana.core.modle.DownloaderConfig;
import banana.core.modle.MasterConfig;
import banana.core.processor.DataProcessor;
import banana.core.processor.Extractor;
import banana.core.protocol.MasterProtocol;
import banana.core.protocol.DownloadProtocol;
import banana.core.protocol.Task;
import banana.core.request.Cookies;
import banana.core.util.SystemUtil;
import banana.dowloader.processor.MongoDBDataProcessor;

public final class DownloadServer implements DownloadProtocol {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static Logger logger = Logger.getLogger(DownloadServer.class);

	public DownloaderConfig config;

	private MasterProtocol master = null;

	private Server rpcServer;

	public DB db;

	public Extractor extractor;

	public DataProcessor dataProcessor;

	private Map<String, DownloadTracker> downloadInstance = new ConcurrentHashMap<String, DownloadTracker>();

	private static DownloadServer instance = null;

	public static final DownloadServer getInstance() {
		return instance;
	}

	public DownloadServer(DownloaderConfig config) throws Exception {
		master = (MasterProtocol) RPC.getProxy(MasterProtocol.class, MasterProtocol.versionID,
				new InetSocketAddress(config.master.host, config.master.port), new Configuration());
		MasterConfig masterConfig = master.getMasterConfig();
		dataProcessor = new MongoDBDataProcessor();
		extractor = new JsonRpcExtractor(masterConfig.extractor);
		extractor.parseData("{}", "<html></html>");
		instance = this;
		this.config = config;
		MongoClient client = null;
		ServerAddress serverAddress = new ServerAddress(masterConfig.mongodb.host, masterConfig.mongodb.port);
		List<ServerAddress> seeds = new ArrayList<ServerAddress>();
		seeds.add(serverAddress);
		MongoCredential credentials = MongoCredential.createCredential(masterConfig.mongodb.username,
				masterConfig.mongodb.db, masterConfig.mongodb.password.toCharArray());
		client = new MongoClient(seeds, Arrays.asList(credentials), getOptions());
		db = client.getDB(masterConfig.mongodb.db);
	}

	@Override
	public NodeStatus healthCheck() {
		return SystemUtil.getLocalNodeStatus();
	}

	public MasterProtocol getMasterServer() {
		return master;
	}

	@Override
	public synchronized boolean startDownloadTracker(String taskId,Task config,Cookies initCookie) throws DownloadException {
		newDownloadTracker(taskId, config, initCookie);
		DownloadTracker d = downloadInstance.get(taskId);
		if (d == null) {
			throw new DownloadException(String.format("Can't find the downloader for Id %s", taskId));
		} else if (d.isRuning()) {
			throw new DownloadException("Downloader is already running");
		}
		Thread fetchLinkThread = new Thread(d);
		fetchLinkThread.start();
		return true;
	}

	@Override
	public void resubmitTaskConfig(String taskId, int thread, Task config) throws DownloadException {
		DownloadTracker d = downloadInstance.get(taskId);
		if (d == null) {
			throw new DownloadException(String.format("Can't find the downloader for Id %s", taskId));
		}
		d.updateConfig(thread, config);
	}

	private void newDownloadTracker(String taskId, Task config,Cookies initCookie) throws DownloadException {
		if (downloadInstance.keySet().contains(taskId)) {
			throw new DownloadException("DownloaderTracker is already existed");
		}
		downloadInstance.put(taskId, new DownloadTracker(taskId, config,initCookie));
		logger.info("Create a DownloadTracker under the task " + taskId);
	}

	@Override
	public void stopDownloadTracker(String taskId) throws DownloadException {
		DownloadTracker d = downloadInstance.get(taskId);
		if (d == null) {
			throw new DownloadException("Can't find the downloader");
		}
		d.stop();
		downloadInstance.remove(taskId);
	}

	@Override
	public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
		return DownloadProtocol.versionID;
	}

	@Override
	public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash)
			throws IOException {
		return new ProtocolSignature(DownloadProtocol.versionID, null);
	}

	@Override
	public boolean isWaitRequest(String taskId) throws DownloadException {
		DownloadTracker d = downloadInstance.get(taskId);
		if (d == null) {
			throw new DownloadException("Can't find the downloader");
		}
		return d.isWaitRequest();
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

	public synchronized void startDownloader()
			throws HadoopIllegalArgumentException, IOException, CrawlerMasterException {
		String localIp = SystemUtil.getLocalIP();
		rpcServer = new RPC.Builder(new Configuration()).setProtocol(DownloadProtocol.class).setInstance(this)
				.setBindAddress("0.0.0.0").setPort(config.listen).setNumHandlers(config.handlers).build();
		rpcServer.start();
		master.registerDownloadNode(localIp, config.listen);
	}

	@Override
	public synchronized void stopDownloader() throws DownloadException {
		for (String taskid : downloadInstance.keySet()) {
			stopDownloadTracker(taskid);
		}
		new Thread(){public void run() {rpcServer.stop();}}.start();
	}

	@Override
	public void injectCookies(String taskId, Cookies cookies) throws DownloadException {
		DownloadTracker d = downloadInstance.get(taskId);
		if (d == null) {
			throw new DownloadException("Can't find the downloader");
		}
		d.getHttpDownloader().injectCookies(cookies);
	}

}
