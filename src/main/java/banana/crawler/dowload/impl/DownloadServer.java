package banana.crawler.dowload.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.log4j.Logger;

import banana.core.JedisOperator;
import banana.core.NodeStatus;
import banana.core.exception.DownloadException;
import banana.core.processor.DataProcessor;
import banana.core.protocol.CrawlerMasterProtocol;
import banana.core.protocol.DownloadProtocol;
import banana.core.protocol.Extractor;
import banana.core.protocol.Task;
import banana.core.util.SystemUtil;
import banana.crawler.dowload.processor.MongoDBDataProcessor;


public final class DownloadServer implements DownloadProtocol{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private static Logger logger = Logger.getLogger(DownloadServer.class);
	
	private CrawlerMasterProtocol master = null;

	public Extractor extractor;
	
	public DataProcessor dataProcessor;
	
	private Map<String,DownloadTracker> downloadInstance = new HashMap<String,DownloadTracker>();
	
	private static DownloadServer instance = null;
	
	public static DownloadServer initInstance(String masterHost) throws Exception {
		if (instance == null){
			try {
				instance = new DownloadServer(masterHost);
			} catch (Exception e) {
				logger.warn("请确认master已经启动", e);
				throw e;
			}
		}
		String mongoAddress = instance.master.getMasterPropertie("MONGO").toString();
		instance.dataProcessor = new MongoDBDataProcessor(mongoAddress);
		String extractorAddress = instance.master.getMasterPropertie("EXTRACTOR").toString();
		instance.extractor = new JsonRpcExtractor(extractorAddress);
		instance.extractor.parseData("{}", "<html></html>");
		return instance;
	}
	
	public static final DownloadServer getInstance(){
		return instance;
	}
	
	private DownloadServer(String masterHost)throws Exception{
		int port = 8666;
		if (masterHost.contains(":")){
			port = Integer.parseInt(masterHost.split(":")[1]);
		}
		master = (CrawlerMasterProtocol) RPC.getProxy(CrawlerMasterProtocol.class,CrawlerMasterProtocol.versionID,new InetSocketAddress(masterHost,port),new Configuration());
		master.getMasterPropertie("");
		System.out.println("已经连接到master");
	}
	
	@Override
	public NodeStatus healthCheck(){
		return SystemUtil.getLocalNodeStatus();
	}
	
	public CrawlerMasterProtocol getMasterServer(){
		return master;
	}
	
	@Override
	public boolean startDownloadTracker(String taskId,int thread,Task config) throws DownloadException{
		newDownloadTracker(taskId, thread, config);
		DownloadTracker d = downloadInstance.get(taskId);
		if (d == null){
			throw new DownloadException(String.format("Can't find the downloader for Id %s", taskId));
		}else if (d.isRuning()){
			throw new DownloadException("Downloader is already running");
		}
		Thread fetchLinkThread = new Thread(d);
		fetchLinkThread.start();
		return true;
	}
	
	@Override
	public void resubmitTaskConfig(String taskId, int thread, Task config) throws DownloadException {
		DownloadTracker d = downloadInstance.get(taskId);
		if (d == null){
			throw new DownloadException(String.format("Can't find the downloader for Id %s", taskId));
		}
		d.updateConfig(thread, config);
	}

	private void newDownloadTracker(String taskId, int thread,Task config) throws DownloadException{
		if (downloadInstance.keySet().contains(taskId)){
			throw new DownloadException("TaskTracker is already existed");
		}
		downloadInstance.put(taskId, new DownloadTracker(taskId,thread,config));
		logger.info("Create a DownloadTracker under the task " + taskId);
	}
	
	@Override
	public void stopDownloadTracker(String taskId) throws DownloadException {
		DownloadTracker d = downloadInstance.get(taskId);
		if (d == null){
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
		return new ProtocolSignature(DownloadProtocol.versionID,null);
	}

	@Override
	public boolean isWaitRequest(String taskId) throws DownloadException {
		DownloadTracker d = downloadInstance.get(taskId);
		if (d == null){
			throw new DownloadException("Can't find the downloader");
		}
		return d.isWaitRequest();
	}

}
