package banana.crawler.dowload.impl;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.rmi.RemoteException;
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

import com.banana.common.JedisOperator.Command;
import com.banana.common.PrefixInfo;
import com.banana.common.PropertiesNamespace;
import com.banana.common.util.CountableThreadPool;
import com.banana.component.DataProcessor;
import com.banana.component.PageProcessor;
import com.banana.component.config.MongonDataProcessor;
import com.banana.component.config.XmlConfigPageProcessor;
import com.banana.downloader.impl.DefaultFileDownloader;
import com.banana.downloader.impl.DefaultPageDownloader;
import com.banana.model.Processable;
import com.banana.page.OkPage;
import com.banana.page.Page;
import com.banana.page.RetryPage;
import com.banana.request.BasicRequest;
import com.banana.request.BinaryRequest;
import com.banana.request.PageRequest;
import com.banana.request.TransactionRequest;

import redis.clients.jedis.Jedis;

import com.banana.request.PageRequest.PageEncoding;

public class DownloadTracker implements Runnable{
	
	private static Logger logger = Logger.getLogger(DownloadTracker.class);
	
	private final String taskName;
	
	private boolean isRuning = false;
	
	private DownloadServer downloadServer;

	private Map<String,PageProcessor> pageProcessors = new HashMap<String,PageProcessor>();
	
	private Map<String,DataProcessor> dataProcessors = new HashMap<String,DataProcessor>();
	
	private URLClassLoader externalClassLoader = new URLClassLoader(new URL[]{});

	private String externalPath ;
	
	private DefaultPageDownloader defaultPageDownloader = new DefaultPageDownloader();
	
	private DefaultFileDownloader defaultFileDownloader = new DefaultFileDownloader(10);
	
	private CountableThreadPool downloadThreadPool ;
	
	private int fetchsize;
	
	public DownloadTracker(String tname,int thread,DownloadServer ds){
		taskName = tname;
		externalPath = DownloadServer.class.getClassLoader().getResource("").getPath() + "externalJar";
		downloadServer = ds;
		downloadThreadPool = new CountableThreadPool(thread, Executors.newCachedThreadPool());
		fetchsize = thread * 3 < 10? 10 :thread * 3;
	}
	
	public boolean isRuning() {
		return isRuning;
	}

	private boolean download(BasicRequest request){
		System.out.println("验证rmi多线程:"+Thread.currentThread().getId());
		switch(request.getType()){
		case PAGE_REQUEST:
			final PageRequest pageRequest = (PageRequest) request;
			if(pageRequest.getPageEncoding()==null){
				pageRequest.setPageEncoding(PageEncoding.UTF8);
			}
			PageProcessor pageProccess = findPageProcessor(pageRequest.getProcessorClass().getName(),pageRequest.getProcessorAddress());
			if(pageProccess == null){
				logger.warn("Not Found PageProcessor Name:"+pageRequest.getProcessorClass().getName());
				return false;
			}
			Page page = defaultPageDownloader.download(pageRequest,taskName);
			logger.info("抓取:"+pageRequest.getUrl()+"\tStatus:"+page.getStatus()+"\tCode:"+page.getStatusCode());
			offlineHandle(pageProccess, page);
			break;
		case TRANSACTION_REQUEST:
			TransactionRequest transactionRequest = (TransactionRequest) request;
			Iterator<BasicRequest> basicRequestIter = transactionRequest.iteratorChildRequest();
			while(basicRequestIter.hasNext()){
				BasicRequest child = basicRequestIter.next();
				download(child);
			}
			break;
		case BINARY_REQUEST:
			defaultFileDownloader.downloadFile((BinaryRequest) request);
			break;
		}
		return true;
	}
	
	private synchronized PageProcessor addPageProcessor(String proccessClsName,String arg1){
		if(pageProcessors.get(proccessClsName) != null){
			return pageProcessors.get(proccessClsName);
		}
		PageProcessor proccess = null;
		try {
			if (proccessClsName.equals(XmlConfigPageProcessor.class.getName())){
				if (arg1 != null){
					String xmlConfig = downloadServer.getRedis().exe(new Command<String>() {

						@Override
						public String operation(Jedis jedis) throws Exception {
							String taskKey = PrefixInfo.TASK_PREFIX + taskName + PrefixInfo.TASK_CONFIG;
							return jedis.get(taskKey);
						}
					});
					String processorName = arg1;
					proccess = XmlConfigPageProcessor.newConfigPageProcessor(xmlConfig,processorName);
				}
			}else{
				Class<? extends PageProcessor> proccessCls = (Class<? extends PageProcessor>) externalClassLoader.loadClass(proccessClsName);
				proccess = proccessCls.newInstance();
				pageProcessors.put(proccessClsName , proccess );
			}
		} catch (Exception e) {
			logger.warn(e.getMessage());
		}
		return proccess;
	}
	
	public PageProcessor findPageProcessor(String proccessClsName,String arg1) {
		PageProcessor pageProcessor = pageProcessors.get(proccessClsName);
		if(pageProcessor == null){
			pageProcessor = addPageProcessor(proccessClsName,arg1);
		}
		return pageProcessor;
	}
	
	private synchronized DataProcessor  addDataProcessor(String proccessClsName){
		if(dataProcessors.get(proccessClsName) != null){
			return dataProcessors.get(proccessClsName);
		}
		DataProcessor proccess = null;
		try {
			if (proccessClsName.equals("default")){
				proccess = new MongonDataProcessor();
			}else{
				Class<? extends DataProcessor> proccessCls = (Class<? extends DataProcessor>) externalClassLoader.loadClass(proccessClsName);
				proccess = proccessCls.newInstance();
			}
			dataProcessors.put(proccessClsName , proccess );
		} catch (Exception e) {
			logger.warn(e.getMessage());
		}
		return proccess;
	}
	
	public DataProcessor findDataProcessor(String proccessClsName) {
		DataProcessor dataProcessor = dataProcessors.get(proccessClsName);
		if(dataProcessor == null){
			dataProcessor = addDataProcessor(proccessClsName);
		}
		return dataProcessor;
	}
	
	/**
	 * 离线处理
	 * @param pageProccess
	 * @param page
	 * @return
	 */
	private final void offlineHandle(final PageProcessor pageProccess ,final Page page){
		if(page instanceof RetryPage){
			RetryPage retryPage = (RetryPage) page;
			PageRequest retryRequest = retryPage.getRequest();
			int maxPageRetryCount = (int) Properties.getTaskPropertie(taskName, PropertiesNamespace.Task.MAX_PAGE_RETRY_COUNT);
			if(retryRequest.getHistoryCount() < maxPageRetryCount){
				handleResult(Arrays.asList((BasicRequest)retryRequest), null);
				logger.warn("重新请求URL:"+retryPage.getRequest().getUrl());
			}else{
				retryRequest.notifySelf();
				logger.error("下载次数超过"+maxPageRetryCount+":"+retryPage.getRequest().getUrl()+" 被丢弃");
			}
		}else if(page instanceof OkPage){
			try {
				List<BasicRequest> newRequests = new ArrayList<BasicRequest>();
				List<Processable> objectContainer = new ArrayList<Processable>();
			    pageProccess.process((OkPage) page,null,newRequests,objectContainer);
				handleResult(newRequests,objectContainer);
				BasicRequest basicRequest = page.getRequest();
				basicRequest.notifySelf();
			} catch (Exception e) {
				logger.error("离线处理异常URL:"+page.getRequest().getUrl(),e);
			}
		}
	}
	
	
	protected void handleResult(List<BasicRequest> newRequests, List<Processable> objectContainer) {
		//跟进URL加入队列
		try {
			downloadServer.getMasterServer().pushTaskRequests(taskName, newRequests);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		if (objectContainer != null){
			DataProcessor dp = findDataProcessor("default");
			dp.handleData(objectContainer);
		}
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
	
	public final List<BasicRequest> pollRequests() throws InterruptedException, RemoteException{
		while (true) {
			if(downloadThreadPool.getIdleThreadCount() == 0){
				Thread.sleep(100);
				continue;//等待有线程可以工作
			}
			return downloadServer.getMasterServer().pollTaskRequests(taskName, fetchsize);
		}
	}
	
	private final void asyncInvokeDownload(final BasicRequest request){
		downloadThreadPool.execute(new Runnable() {
			@Override
			public void run() {
				download(request);
			}
		});
	}

	@Override
	public void run() {
		logger.info("DownloadTracker Starting ");
		logger.info("DownloadTracker TaskName = " + taskName);
		logger.info("DownloadTracker thread = " + downloadThreadPool.getThreadNum());
		isRuning = true;
		defaultPageDownloader.open();//打开下载器
		List<BasicRequest> requests = null;
		while(true){
			try{
				requests = pollRequests();
				if (isRuning && requests.isEmpty()){
					logger.info("Task "+ taskName +" queue is empty");
					Thread.sleep(1000);
					continue;
				}
				for (BasicRequest request : requests) {
					asyncInvokeDownload(request);
				}
			}catch(Exception e){
				logger.error("轮询队列出错",e);
				break;
			}
			if (!isRuning){
				//销毁
				break;
			}
		}
	}
	
	public void stop(){
		isRuning = false;
		downloadThreadPool.close();
		try{
			externalClassLoader.close();
		}catch(Exception e){
			logger.warn("",e);
		}
		try {
			defaultPageDownloader.close();
		} catch (IOException e) {
			logger.warn("",e);
		}
		try {
			defaultFileDownloader.close();
		} catch (IOException e) {
			logger.warn("",e);
		}
		
	}
}
