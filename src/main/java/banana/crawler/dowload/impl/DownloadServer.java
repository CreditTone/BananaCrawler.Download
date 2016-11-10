package banana.crawler.dowload.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.log4j.Logger;

import banana.core.NodeStatus;
import banana.core.exception.DownloadException;
import banana.core.modle.DownloaderConfig;
import banana.core.modle.MasterConfig;
import banana.core.processor.DataProcessor;
import banana.core.processor.Extractor;
import banana.core.protocol.CrawlerMasterProtocol;
import banana.core.protocol.DownloadProtocol;
import banana.core.protocol.Task;
import banana.core.util.SystemUtil;
import banana.crawler.dowload.processor.MongoDBDataProcessor;


public final class DownloadServer implements DownloadProtocol{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private static Logger logger = Logger.getLogger(DownloadServer.class);
	
	public DownloaderConfig config;
	
	private CrawlerMasterProtocol master = null;

	public Extractor extractor;
	
	public DataProcessor dataProcessor;
	
	private Map<String,DownloadTracker> downloadInstance = new HashMap<String,DownloadTracker>();
	
	private static DownloadServer instance = null;
	
	public static final DownloadServer getInstance(){
		return instance;
	}
	
	public DownloadServer(DownloaderConfig config)throws Exception{
		master = (CrawlerMasterProtocol) RPC.getProxy(CrawlerMasterProtocol.class,CrawlerMasterProtocol.versionID,new InetSocketAddress(config.master.host,config.master.port),new Configuration());
		MasterConfig masterConfig = master.getMasterConfig();
		dataProcessor = new MongoDBDataProcessor(masterConfig.mongodb);
		extractor = new JsonRpcExtractor(masterConfig.extractor);
		extractor.parseData("{}", "<html></html>");
		instance = this;
		this.config = config;
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
