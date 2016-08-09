package banana.crawler.dowload.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.log4j.Logger;

import banana.core.JedisOperator;
import banana.core.NodeStatus;
import banana.core.exception.DownloadException;
import banana.core.protocol.CrawlerMasterProtocol;
import banana.core.protocol.DownloadProtocol;
import banana.core.util.SystemUtil;


public final class DownloadServer implements DownloadProtocol{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private static Logger logger = Logger.getLogger(DownloadServer.class);
	
	private CrawlerMasterProtocol master = null;

	private JedisOperator redis;
	
	private Map<String,DownloadTracker> downloadInstance = new HashMap<String,DownloadTracker>();
	
	private static DownloadServer instance = null;
	
	public static DownloadServer initInstance(String masterHost){
		if (instance == null){
			try {
				instance = new DownloadServer(masterHost);
			} catch (Exception e) {
				logger.warn("请确认master已经启动", e);
				return null;
			}
		}
		return instance;
	}
	
	public static final DownloadServer getInstance(){
		return instance;
	}
	
	private DownloadServer(String masterHost){
		try {
			int port = 8787;
			if (masterHost.contains(":")){
				port = Integer.parseInt(masterHost.split(":")[1]);
			}
			master = (CrawlerMasterProtocol) RPC.getProxy(CrawlerMasterProtocol.class,CrawlerMasterProtocol.versionID,new InetSocketAddress(masterHost,port),new Configuration());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public boolean startDownloadTracker(String trackerId) throws DownloadException{
		DownloadTracker d = downloadInstance.get(trackerId);
		if (d == null){
			throw new DownloadException("Can't find the downloader");
		}else if (d.isRuning()){
			throw new DownloadException("Downloader is already running");
		}
		Thread fetchLinkThread = new Thread(d);
		fetchLinkThread.start();
		return true;
	}

	@Override
	public NodeStatus healthCheck(){
		return SystemUtil.getLocalNodeStatus();
	}
	
	public CrawlerMasterProtocol getMasterServer(){
		return master;
	}
	
	public JedisOperator getRedis(){
		return redis;
	}

	@Override
	public String newDownloadTracker(String taskName, int thread) throws DownloadException{
		for (String key : downloadInstance.keySet()) {
			if (key.startsWith(taskName)){
				throw new DownloadException("TaskTracker is already running:"+taskName);
			}
		}
		String trackerId = taskName + "_" + new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date());
		downloadInstance.put(trackerId, new DownloadTracker(taskName,thread));
		logger.info("Create a DownloadTracker under the task " + taskName);
		return trackerId;
	}
	
	@Override
	public void stopDownloadTracker(String taskName) throws DownloadException {
		DownloadTracker d = downloadInstance.get(taskName);
		if (d == null){
			throw new DownloadException("Can't find the downloader");
		}
		d.stop();
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
}
