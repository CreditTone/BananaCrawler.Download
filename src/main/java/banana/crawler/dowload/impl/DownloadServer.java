package banana.crawler.dowload.impl;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.banana.common.JedisOperator;
import com.banana.common.NodeStatus;
import com.banana.common.PropertiesNamespace;
import com.banana.common.download.IDownload;
import com.banana.common.master.ICrawlerMasterServer;
import com.banana.common.util.SystemUtil;
import com.banana.request.BasicRequest;


public final class DownloadServer extends UnicastRemoteObject implements IDownload{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private static Logger logger = Logger.getLogger(DownloadServer.class);
	
	private ICrawlerMasterServer master = null;

	private JedisOperator redis;
	
	private Map<String,DownloadTracker> downloadInstance = new HashMap<String,DownloadTracker>();
	
	private static DownloadServer instance = null;
	
	public static DownloadServer initInstance(String masterHost){
		if (instance == null){
			try {
				instance = new DownloadServer(masterHost);
			} catch (Exception e) {
				logger.warn("请检测master是否正常启动", e);
				return null;
			}
		}
		return instance;
	}
	
	public static DownloadServer getInstance(){
		return instance;
	}
	
	private DownloadServer(String masterHost) throws RemoteException, MalformedURLException, NotBoundException {
		super();
		master = (ICrawlerMasterServer) Naming.lookup("rmi://" + masterHost + ":1099"+"/master");
		redis = JedisOperator.newInstance(master.getMasterPropertie(PropertiesNamespace.Master.REDIS_HOST).toString(), (Integer)master.getMasterPropertie(PropertiesNamespace.Master.REDIS_PORT));
	}
	
	public boolean startDownloadTracker(String taskName) throws RemoteException{	
		DownloadTracker d = downloadInstance.get(taskName);
		if (d == null){
			throw new RemoteException("Can't find the downloader");
		}else if (d.isRuning()){
			throw new RemoteException("Downloader is already running");
		}
		Thread fetchLinkThread = new Thread(d);
		fetchLinkThread.start();
		return true;
	}

	@Override
	public NodeStatus healthCheck() throws RemoteException {
		return SystemUtil.getLocalNodeStatus();
	}
	
	public ICrawlerMasterServer getMasterServer(){
		return master;
	}
	
	public JedisOperator getRedis(){
		return redis;
	}

	@Override
	public void newDownloadTracker(String taskName, int thread) throws RemoteException {
		if (downloadInstance.containsKey(taskName)){
			throw new RemoteException("这个任务已经存在:"+taskName);
		}
		downloadInstance.put(taskName, new DownloadTracker(taskName,thread, this));
		logger.info("Create a DownloadTracker under the task " + taskName);
	}
	
	@Override
	public void stopDownloadTracker(String taskName) throws RemoteException {
		DownloadTracker d = downloadInstance.get(taskName);
		if (d == null){
			throw new RemoteException("Can't find the downloader");
		}
		d.stop();
	}
}
