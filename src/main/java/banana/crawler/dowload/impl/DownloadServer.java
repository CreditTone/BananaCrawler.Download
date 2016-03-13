package banana.crawler.dowload.impl;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.banana.common.JOperator;
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

	private JOperator redis;
	
	private Map<String,Download> downloadInstance = new HashMap<String,Download>();
	
	private static DownloadServer instance = null;
	
	static{
		try {
			instance = new DownloadServer();
		} catch (Exception e) {
			logger.warn("请检测master是否正常启动", e);
		}
	}
	
	public static DownloadServer getInstance(){
		return instance;
	}
	
	private DownloadServer() throws RemoteException, MalformedURLException, NotBoundException {
		super();
		String masterHost = (String) Properties.getConfigPropertie(PropertiesNamespace.Download.MASTER_HOST);
		int masterPort = (int) Properties.getConfigPropertie(PropertiesNamespace.Download.MASTER_PORT);
		master = (ICrawlerMasterServer) Naming.lookup("rmi://" + masterHost + ":" + masterPort +"/master");
		redis = JOperator.newInstance(master.getMasterPropertie(PropertiesNamespace.Master.REDIS_HOST).toString(), (Integer)master.getMasterPropertie(PropertiesNamespace.Master.REDIS_PORT));
	}
	
	public boolean startDownload(String taskName) throws RemoteException{	
		Download d = downloadInstance.get(taskName);
		if (d == null){
			throw new RemoteException("Can't find the downloader");
		}else if (d.isRuning()){
			throw new RemoteException("Downloader is already running");
		}
		Thread fetchLinkThread = new Thread(d);
		fetchLinkThread.start();
		d.setRuning(true);
		return true;
	}

	@Override
	public NodeStatus getStatus() throws RemoteException {
		return SystemUtil.getLocalNodeStatus();
	}
	
	public ICrawlerMasterServer getMasterServer(){
		return master;
	}

	@Override
	public void newDownload(String taskName, int thread) throws RemoteException {
		if (downloadInstance.containsKey(taskName)){
			throw new RemoteException("这个任务已经存在:"+taskName);
		}
		downloadInstance.put(taskName, new Download(taskName,thread, instance));
	}
}
