package banana.crawler.dowload.impl;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import banana.core.download.HttpDownloader;
import banana.core.download.impl.DefaultHttpDownloader;
import banana.core.download.impl.PhantomJsDownloader;
import banana.core.exception.CrawlerMasterException;
import banana.core.modle.CrawlData;
import banana.core.processor.BinaryProcessor;
import banana.core.processor.PageProcessor;
import banana.core.protocol.MasterProtocol;
import banana.core.protocol.Task;
import banana.core.protocol.Task.DownloadProcessor;
import banana.core.request.BasicRequest;
import banana.core.request.BinaryRequest;
import banana.core.request.HttpRequest;
import banana.core.request.PageRequest;
import banana.core.request.PageRequest.PageEncoding;
import banana.core.request.TransactionRequest;
import banana.core.response.Page;
import banana.core.response.StreamResponse;
import banana.core.util.CountableThreadPool;
import banana.core.util.SystemUtil;
import banana.crawler.dowload.processor.JSONConfigDownloadProcessor;
import banana.crawler.dowload.processor.JSONConfigPageProcessor;
import banana.crawler.dowload.processor.ProcessorForwarder;

public class DownloadTracker implements Runnable,banana.core.protocol.DownloadTracker{
	
	private static Logger logger = Logger.getLogger(DownloadTracker.class);
	
	private final String taskId;
	
	private Task config;
	
	private boolean runing = false;
	
	private boolean stoped = false;
	
	private boolean waitRequest = false;
	
	private Map<String,PageProcessor> pageProcessors = new HashMap<String,PageProcessor>();
	
	private Map<String,BinaryProcessor> binaryProcesors = new HashMap<String,BinaryProcessor>();
	
	private HttpDownloader httpDownloader;
	
	private CountableThreadPool downloadThreadPool;
	
	public DownloadTracker(String tId,int thread,Task taskConfig){
		taskId = tId;
		config = taskConfig;
		downloadThreadPool = new CountableThreadPool(thread);
		if (taskConfig.downloader.equals("default")){
			httpDownloader = new DefaultHttpDownloader();
		}else if (taskConfig.downloader.equals("phantomjs")){
			httpDownloader = new PhantomJsDownloader(DownloadServer.getInstance().config.phantomjs);
		}
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

	private final boolean download(BasicRequest request){
		if (request instanceof PageRequest){
			final PageRequest pageRequest = (PageRequest) request;
			if(pageRequest.getPageEncoding()==null){
				pageRequest.setPageEncoding(PageEncoding.UTF8);
			}
			PageProcessor pageProcessor = findPageProcessor(pageRequest.getProcessor());
			if(pageProcessor == null){
				logger.warn("Not Found PageProcessor Name:" + pageRequest.getProcessor());
				return false;
			}
			Page page = httpDownloader.download(pageRequest);
			if (page == null){
				return false;
			}
			if (pageRequest.getMethod() == HttpRequest.Method.POST){
				logger.info(String.format("%s Post:%s StatusCode:%s", pageRequest.getUrl(), pageRequest.getParams(), page.getStatus()));
			}else{
				logger.info(String.format("%s StatusCode:%s", pageRequest.getUrl(), page.getStatus()));
			}
			processPage(pageProcessor, page);
		}else{
			final BinaryRequest binaryRequest = (BinaryRequest) request;
			BinaryProcessor binaryProcessor = findBinaryProcessor(binaryRequest.getProcessor());
			if(binaryProcessor == null){
				logger.warn("Not Found BinaryProcessor Name:" + binaryRequest.getProcessor());
				return false;
			}
			StreamResponse stream = httpDownloader.downloadBinary(binaryRequest);
			if (binaryRequest.getMethod() == HttpRequest.Method.POST){
				logger.info(String.format("%s Post:%s StatusCode:%s", binaryRequest.getUrl(), binaryRequest.getParams(), stream.getStatus()));
			}else{
				logger.info(String.format("%s StatusCode:%s", binaryRequest.getUrl(), stream.getStatus()));
			}
			if (stream.getStatus() == 200){
				try {
					binaryProcessor.process(stream);
				} catch (Exception e) {
					e.printStackTrace();
					logger.error("离线处理异常URL:"+binaryRequest.getUrl(),e);
				}
			}
		}
		return true;
	}
	
	private BinaryProcessor findBinaryProcessor(String processor) {
		BinaryProcessor binaryProcessor = binaryProcesors.get(processor);
		if(binaryProcessor == null){
			binaryProcessor = addBinaryProcessor(processor);
		}
		return binaryProcessor;
	}

	private synchronized BinaryProcessor addBinaryProcessor(String processor) {
		if(binaryProcesors.get(processor) != null){
			return binaryProcesors.get(processor);
		}
		for (DownloadProcessor pro : config.download_processors) {
			if (pro.index.equals(processor)){
				BinaryProcessor binaryProcesor = new JSONConfigDownloadProcessor(pro);
				binaryProcesors.put(processor, binaryProcesor);
				break;
			}
		}
		return binaryProcesors.get(processor);
	}

	private synchronized PageProcessor addPageProcessor(String processor) {
		if(pageProcessors.get(processor) != null){
			return pageProcessors.get(processor);
		}
		for (Task.ProcessorForwarder forwarderConfig : config.forwarders) {
			if (processor.equals(forwarderConfig.index)){
				pageProcessors.put(processor, new ProcessorForwarder(taskId, forwarderConfig, this));
				break;
			}
		}
		for (Task.Processor processorConfig : config.processors) {
			if (processor.equals(processorConfig.index)){
				pageProcessors.put(processor, new JSONConfigPageProcessor(taskId, processorConfig));
				break;
			}
		}
		return pageProcessors.get(processor);
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
	 * @param pageProcessor
	 * @param page
	 * @return
	 */
	private final void processPage(final PageProcessor pageProcessor ,final Page page){
		int ret = page.getStatus() / 100;
		PageRequest pr = (PageRequest) page.getRequest();
		if (ret == 2){
			try {
				List<HttpRequest> newRequests = new ArrayList<HttpRequest>();
				List<CrawlData> objectContainer = new ArrayList<CrawlData>();
			    pageProcessor.process(page,null,newRequests,objectContainer);
			    for (HttpRequest request : newRequests) {
					request.baseRequest(page.getRequest());
				}
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
		try {
			MasterProtocol master = DownloadServer.getInstance().getMasterServer();
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
		httpDownloader.open();//打开下载器
		HttpRequest request = null;
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
			httpDownloader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		stoped = true;
		logger.info(String.format("%s DownloadTracker %s release", taskId, SystemUtil.getLocalIP()));
	}
	
	@Override
	public Page sendRequest(PageRequest request) {
		Page page = httpDownloader.download(request);
		return page;
	}
}
