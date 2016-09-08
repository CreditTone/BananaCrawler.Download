package banana.crawler.dowload.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import banana.core.download.impl.DefaultPageDownloader;
import banana.core.exception.CrawlerMasterException;
import banana.core.modle.CrawlData;
import banana.core.processor.PageProcessor;
import banana.core.protocol.CrawlerMasterProtocol;
import banana.core.protocol.Task;
import banana.core.request.BasicRequest;
import banana.core.request.HttpRequest;
import banana.core.request.PageRequest;
import banana.core.request.PageRequest.PageEncoding;
import banana.core.request.TransactionRequest;
import banana.core.response.Page;
import banana.core.util.CountableThreadPool;
import banana.core.util.SystemUtil;
import banana.crawler.dowload.processor.JSONConfigPageProcessor;

public class DownloadTracker implements Runnable,banana.core.protocol.DownloadTracker{
	
	private static Logger logger = Logger.getLogger(DownloadTracker.class);
	
	private final String taskId;
	
	private Task config;
	
	private boolean runing = false;
	
	private boolean stoped = false;
	
	private boolean waitRequest = false;
	
	private Map<String,PageProcessor> pageProcessors = new HashMap<String,PageProcessor>();
	
	private DefaultPageDownloader defaultPageDownloader = new DefaultPageDownloader();
	
	private CountableThreadPool downloadThreadPool;
	
	public DownloadTracker(String tId,int thread,Task taskConfig){
		taskId = tId;
		config = taskConfig;
		downloadThreadPool = new CountableThreadPool(thread);
	}
	
	public boolean isRuning() {
		return runing;
	}
	
	public boolean isWaitRequest(){
		return waitRequest && (downloadThreadPool.getThreadAlive() == 0);
	}
	
	public void updateConfig(int thread,Task taskConfig){
		downloadThreadPool.setThread(thread);
		config = taskConfig;
		pageProcessors.clear();
		logger.info(String.format("%s downloadTracker add %d thread", taskId, thread - downloadThreadPool.getThreadNum()));
	}

	private boolean download(BasicRequest request){
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
			logger.info(pageRequest.getUrl()+" StatusCode:"+page.getStatus());
			processPage(pageProcessor, page);
			break;
		case TRANSACTION_REQUEST:
			TransactionRequest transactionRequest = (TransactionRequest) request;
			Iterator<BasicRequest> basicRequestIter = transactionRequest.iteratorChildRequest();
			while(basicRequestIter.hasNext()){
				BasicRequest child = basicRequestIter.next();
				download(child);
			}
			break;
		}
		return true;
	}
	
	private synchronized PageProcessor addPageProcessor(String processor) {
		if(pageProcessors.get(processor) != null){
			return pageProcessors.get(processor);
		}
		PageProcessor processorInstance = null;
		for (Task.Processor processorConfig : config.processors) {
			if (processor.equals(processorConfig.getIndex())){
				processorInstance = new JSONConfigPageProcessor(taskId, processorConfig, DownloadServer.getInstance().extractor);
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
	private final void processPage(final PageProcessor pageProccess ,final Page page){
		int ret = page.getStatus() / 100;
		PageRequest pr = (PageRequest) page.getRequest();
		if (ret == 2){
			try {
				List<HttpRequest> newRequests = new ArrayList<HttpRequest>();
				List<CrawlData> objectContainer = new ArrayList<CrawlData>();
			    pageProccess.process(page,null,newRequests,objectContainer);
				handleResult(newRequests,objectContainer);
			} catch (Exception e) {
				e.printStackTrace();
				logger.error("离线处理异常URL:"+pr.getUrl(),e);
			}
		}else{
			if(page.getRequest().getHistoryCount() < 3){
				handleResult(Arrays.asList(page.getRequest()), null);
				logger.warn("重新请求URL:"+ pr.getUrl());
			}else{
				logger.error("下载次数超过" + 3 + ":"+pr.getUrl()+" 被丢弃");
			}
		}
	}
	
	
	protected void handleResult(List<HttpRequest> newRequests, List<CrawlData> objectContainer) {
		//跟进URL加入队列
		try {
			CrawlerMasterProtocol master = DownloadServer.getInstance().getMasterServer();
			for (HttpRequest req : newRequests) {
				master.pushTaskRequest(taskId, req);
			}
		} catch (CrawlerMasterException e) {
			e.printStackTrace();
		}
		try {
			DownloadServer.getInstance().dataProcessor.process(objectContainer, config.collection);
		} catch (Exception e) {
			logger.info(String.format("%s write data failure", taskId),e);
		}
	}

	public final HttpRequest pollRequest() throws CrawlerMasterException, InterruptedException {
		HttpRequest newReq = null;
		while(newReq == null && runing){
			newReq = DownloadServer.getInstance().getMasterServer().pollTaskRequest(taskId);
			waitRequest = (newReq == null);
		}
		while (true) {
			if(downloadThreadPool.getIdleThreadCount() >= 0){
				break;//有线程可以工作
			}
			Thread.sleep(10);
		}
		return newReq;
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
		runing = true;
		defaultPageDownloader.open();//打开下载器
		BasicRequest request = null;
		while(runing){
			try{
				request = pollRequest();
				if (request != null){
					asyncInvokeDownload(request);
				}
			}catch(Exception e){
				logger.error("轮询队列出错",e);
				break;
			}
		}
		release();
	}
	
	@Override
	public void stop(){
		runing = false;
		while(!stoped){
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void release() {
		downloadThreadPool.close();
		try {
			defaultPageDownloader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		stoped = true;
		logger.info(String.format("%s DownloadTracker %s release", taskId, SystemUtil.getLocalIP()));
	}
	
	@Override
	public Page sendRequest(PageRequest request) {
		Page page = defaultPageDownloader.download(request);
		return page;
	}
}
