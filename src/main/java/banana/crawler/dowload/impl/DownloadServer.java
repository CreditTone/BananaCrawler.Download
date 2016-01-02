package banana.crawler.dowload.impl;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.banana.CrawlTask;
import com.banana.common.NodeStatus;
import com.banana.common.PropertiesNamespace;
import com.banana.common.download.IDownload;
import com.banana.common.master.ICrawlerMasterServer;
import com.banana.common.util.SystemUtil;
import com.banana.component.PageProcessor;
import com.banana.component.PageScript;
import com.banana.downloader.JavaScriptDownloader;
import com.banana.downloader.impl.DefaultFileDownloader;
import com.banana.downloader.impl.DefaultPageDownloader;
import com.banana.model.Proccessable;
import com.banana.page.OkPage;
import com.banana.page.Page;
import com.banana.page.RetryPage;
import com.banana.request.BasicRequest;
import com.banana.request.BinaryRequest;
import com.banana.request.PageRequest;
import com.banana.request.StartContext;
import com.banana.request.TransactionRequest;
import com.banana.request.PageRequest.PageEncoding;
import com.banana.thread.CountableThreadPool;

public final class DownloadServer extends UnicastRemoteObject implements IDownload{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private static Logger logger = Logger.getLogger(DownloadServer.class);

	private Map<String,PageProcessor> pageProcessors = new HashMap<String,PageProcessor>();
	
	private URLClassLoader externalClassLoader = new URLClassLoader(new URL[]{});

	private String externalPath ;
	
	private DefaultPageDownloader defaultPageDownloader = new DefaultPageDownloader();
	
	private DefaultFileDownloader defaultFileDownloader = new DefaultFileDownloader(10);
	
	private ExecutorService offlineHandleThreadPool; //离线处理线程
	
	
	protected DownloadServer() throws RemoteException {
		super();
		externalPath = DownloadServer.class.getClassLoader().getResource("").getPath() + "externalJar";
		offlineHandleThreadPool = Executors.newCachedThreadPool();
	}
	
	public boolean dowloadLink(String taskName,BasicRequest request) throws RemoteException{	
		System.out.println("验证rmi多线程:"+Thread.currentThread().getId());
		switch(request.getType()){
		case PAGE_REQUEST:
			final PageRequest pageRequest = (PageRequest) request;
			if(pageRequest.getPageEncoding()==null){
				pageRequest.setPageEncoding(PageEncoding.UTF8);
			}
			PageProcessor pageProccess = findPageProcessor(pageRequest.getProcessorClass().getName());
			if(pageProccess == null){
				throw new RemoteException(String.format("Class Not Found Exception %s", pageRequest.getProcessorClass().getName()));
			}
			Page page = defaultPageDownloader.download(pageRequest,taskName);
			logger.info("抓取:"+pageRequest.getUrl()+"\tStatus:"+page.getStatus()+"\tCode:"+page.getStatusCode());
			offlineHandle(taskName,pageProccess, page);
			break;
		case TRANSACTION_REQUEST:
			TransactionRequest transactionRequest = (TransactionRequest) request;
			Iterator<BasicRequest> basicRequestIter = transactionRequest.iteratorChildRequest();
			while(basicRequestIter.hasNext()){
				BasicRequest child = basicRequestIter.next();
				dowloadLink(taskName, child);
			}
			break;
		case BINARY_REQUEST:
			defaultFileDownloader.downloadFile((BinaryRequest) request);
			break;
		}
		return false;
	}

	public boolean loadJar(String filename,byte[] jarBody) throws RemoteException{
		//下载到本地后load jar
		File jar = new File(externalPath +"/" + filename);
		Method addURL = null;
		try {
			FileUtils.writeByteArrayToFile(jar, jarBody);
			addURL = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
			addURL.setAccessible(true);  
			addURL.invoke(externalClassLoader, jar.toURI().toURL());
		} catch (Exception e) {
			throw new RemoteException(e.getMessage());
		}finally{
			if (addURL != null){
				addURL.setAccessible(false);
			}
		}
		return true;
	}
	
	private synchronized PageProcessor  addPageProcessor(String proccessClsName){
		if(pageProcessors.get(proccessClsName) != null){
			return pageProcessors.get(proccessClsName);
		}
		PageProcessor proccess = null;
		try {
			Class<? extends PageProcessor> proccessCls = (Class<? extends PageProcessor>) externalClassLoader.loadClass(proccessClsName);
			proccess = proccessCls.newInstance();
			if(!pageProcessors.containsKey(proccessCls)){
				pageProcessors.put(proccessClsName , proccess );
			}
		} catch (Exception e) {
			logger.warn(e.getMessage());
		}
		return proccess;
	}
	
	public PageProcessor findPageProcessor(String proccessClsName) {
		PageProcessor pageProcessor = pageProcessors.get(proccessClsName);
		if(pageProcessor == null){
			pageProcessor = addPageProcessor(proccessClsName);
		}
		return pageProcessor;
	}
	
	/**
	 * 离线处理
	 * @param pageProccess
	 * @param page
	 * @return
	 */
	private final void offlineHandle(final String taskName,final PageProcessor pageProccess ,final Page page){
		offlineHandleThreadPool.execute(new Runnable() {
			
			@Override
			public void run() {
				if(page instanceof RetryPage){
					RetryPage retryPage = (RetryPage) page;
					PageRequest retryRequest = retryPage.getRequest();
					int maxPageRetryCount = (int) Properties.getPropertie(taskName, PropertiesNamespace.Task.MAX_PAGE_RETRY_COUNT);
					if(retryRequest.getHistoryCount() < maxPageRetryCount){
						pushRequest(taskName,Arrays.asList((BasicRequest)retryRequest));
						logger.warn("重新请求URL:"+retryPage.getRequest().getUrl());
					}else{
						retryRequest.notifySelf();
						logger.error("下载次数超过"+maxPageRetryCount+":"+retryPage.getRequest().getUrl()+" 被丢弃");
					}
				}else if(page instanceof OkPage){
					try {
						List<BasicRequest> newRequests = new ArrayList<BasicRequest>();
						List<Proccessable> objectContainer = new ArrayList<Proccessable>();
					    pageProccess.process((OkPage) page,null,newRequests,objectContainer);
						handleResult(newRequests,objectContainer);
						BasicRequest basicRequest = page.getRequest();
						basicRequest.notifySelf();
					} catch (Exception e) {
						logger.error("离线处理异常URL:"+page.getRequest().getUrl(),e);
					}
				}
			}
		});
	}
	
	
	protected void handleResult(List<BasicRequest> newRequests, List<Proccessable> objectContainer) {
		
	}

	public void pushRequest(String taskName,List<BasicRequest> requests){
		try {
			ICrawlerMasterServer crawlerMasterServer = (ICrawlerMasterServer) Naming.lookup(Properties.masterAddress);
			crawlerMasterServer.pushTaskRequests(taskName, requests);
		} catch (Exception e) {
			logger.warn(e.getMessage());
		}
		
	}

	@Override
	public NodeStatus getStatus() throws RemoteException {
		return SystemUtil.getLocalNodeStatus("127.0.0.1");
	}

}
