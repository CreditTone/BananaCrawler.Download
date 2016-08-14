package banana.crawler.dowload.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import banana.core.download.impl.DefaultFileDownloader;
import banana.core.download.impl.DefaultPageDownloader;
import banana.core.exception.CrawlerMasterException;
import banana.core.modle.CrawlData;
import banana.core.processor.PageProcessor;
import banana.core.protocol.Task;
import banana.core.protocol.processor.JSONConfigPageProcessor;
import banana.core.request.BasicRequest;
import banana.core.request.BinaryRequest;
import banana.core.request.PageRequest;
import banana.core.request.PageRequest.PageEncoding;
import banana.core.request.TransactionRequest;
import banana.core.response.Page;
import banana.core.util.CountableThreadPool;

public class DownloadTracker implements Runnable,banana.core.protocol.DownloadTracker{
	
	private static Logger logger = Logger.getLogger(DownloadTracker.class);
	
	private final String taskId;
	
	private boolean isRuning = false;
	
	private Map<String,PageProcessor> pageProcessors = new HashMap<String,PageProcessor>();
	
	private DefaultPageDownloader defaultPageDownloader = new DefaultPageDownloader();
	
	private DefaultFileDownloader defaultFileDownloader = new DefaultFileDownloader(10);
	
	private CountableThreadPool downloadThreadPool ;
	
	private int fetchsize;
	
	public DownloadTracker(String tId,int thread){
		taskId = tId;
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
			PageProcessor pageProcessor = findPageProcessor(pageRequest.getProcessor());
			if(pageProcessor == null){
				logger.warn("Not Found PageProcessor Name:"+pageRequest.getProcessor());
				return false;
			}
			Page page = defaultPageDownloader.download(pageRequest);
			logger.info("抓取:"+pageRequest.getUrl()+"\tStatusCode:"+page.getStatus());
			offlineHandle(pageProcessor, page);
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
	
	private synchronized PageProcessor addPageProcessor(String processor) {
		if(pageProcessors.get(processor) != null){
			return pageProcessors.get(processor);
		}
		PageProcessor processorInstance = null;
		Task config = (Task) DownloadServer.getInstance().getMasterServer().getTaskPropertie(taskId, null);
		for (Task.Processor p : config.processors) {
			if (processor.equals(p.getIndex())){
				processorInstance = new JSONConfigPageProcessor(p);
				break;
			}
		}
		return processorInstance;
	}
	
	public PageProcessor findPageProcessor(String processor) {
		PageProcessor pageProcessor = pageProcessors.get(processor);
		if(pageProcessor == null){
			pageProcessor = addPageProcessor(processor);
		}
		return pageProcessor;
	}
	
	/**
	 * 离线处理
	 * @param pageProccess
	 * @param page
	 * @return
	 */
	private final void offlineHandle(final PageProcessor pageProccess ,final Page page){
		int ret = page.getStatus() / 100;
		PageRequest pr = (PageRequest) page.getRequest();
		if (ret == 2){
			try {
				List<BasicRequest> newRequests = new ArrayList<BasicRequest>();
				List<CrawlData> objectContainer = new ArrayList<CrawlData>();
			    pageProccess.process(page,null,newRequests,objectContainer);
				handleResult(newRequests,objectContainer);
				BasicRequest basicRequest = page.getRequest();
			    basicRequest.notify(basicRequest.getUuid());
			} catch (Exception e) {
				e.printStackTrace();
				logger.error("离线处理异常URL:"+pr.getUrl(),e);
			}
		}else{
			if(page.getRequest().getHistoryCount() < 3){
				handleResult(Arrays.asList(page.getRequest()), null);
				logger.warn("重新请求URL:"+ pr.getUrl());
			}else{
				pr.notify(pr.getUuid());
				logger.error("下载次数超过" + 3 + ":"+pr.getUrl()+" 被丢弃");
			}
		}
	}
	
	
	protected void handleResult(List<BasicRequest> newRequests, List<CrawlData> objectContainer) {
		//跟进URL加入队列
		try {
			DownloadServer.getInstance().getMasterServer().pushTaskRequests(taskId, newRequests);
		} catch (CrawlerMasterException e) {
			e.printStackTrace();
		}
		if (objectContainer != null){
			DownloadServer.getInstance().dataProcessor.handleData(objectContainer);
		}
	}

	public final List<BasicRequest> pollRequests() throws CrawlerMasterException, InterruptedException {
		while (true) {
			if(downloadThreadPool.getIdleThreadCount() == 0){
				Thread.sleep(100);
				continue;//等待有线程可以工作
			}
			return DownloadServer.getInstance().getMasterServer().pollTaskRequests(taskId, fetchsize);
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
		logger.info("DownloadTracker TaskId = " + taskId);
		logger.info("DownloadTracker thread = " + downloadThreadPool.getThreadNum());
		isRuning = true;
		defaultPageDownloader.open();//打开下载器
		List<BasicRequest> requests = null;
		while(true){
			try{
				requests = pollRequests();
				if (isRuning && requests.isEmpty()){
					logger.info("Task "+ taskId +" queue is empty");
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
	
	@Override
	public void stop(){
		isRuning = false;
		downloadThreadPool.close();
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
	
	@Override
	public Page sendRequest(PageRequest request) {
		Page page = defaultPageDownloader.download(request);
		return page;
	}

	@Override
	public void setFetchSize(int fetchsize) {
		this.fetchsize = fetchsize;
	}
}
