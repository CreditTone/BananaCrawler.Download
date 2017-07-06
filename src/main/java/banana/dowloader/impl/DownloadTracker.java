package banana.dowloader.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import banana.core.download.HttpDownloader;
import banana.core.download.impl.DefaultHttpDownloader;
import banana.core.download.impl.HtmlUnitDownloader;
import banana.core.download.impl.PhantomJsDownloader;
import banana.core.exception.CrawlerMasterException;
import banana.core.modle.ContextModle;
import banana.core.modle.CrawlData;
import banana.core.modle.Task;
import banana.core.modle.TaskError;
import banana.core.processor.IProcessor;
import banana.core.protocol.MasterProtocol;
import banana.core.request.BinaryRequest;
import banana.core.request.Cookies;
import banana.core.request.HttpRequest;
import banana.core.request.PageRequest;
import banana.core.request.PageRequest.PageEncoding;
import banana.core.response.HttpResponse;
import banana.core.response.Page;
import banana.core.response.StreamResponse;
import banana.core.util.CountableThreadPool;
import banana.core.util.SystemUtil;
import banana.dowloader.processor.DefaultDownloadProcessor;
import banana.dowloader.processor.JSONConfigProcessor;

public class DownloadTracker implements Runnable,banana.core.protocol.DownloadTracker{
	
	private static Logger logger = Logger.getLogger(DownloadTracker.class);
	
	private final String taskId;
	
	private Task config;
	
	private boolean runing = false;
	
	private boolean stoped = false;
	
	private Map<String,IProcessor> pageProcessors = new HashMap<String,IProcessor>();
	
	private IProcessor defaultDownloadProcessor;
	
	private HttpDownloader httpDownloader;
	
	private CountableThreadPool downloadThreadPool;
	
	private RemoteTaskContext taskContext;
	
	private PageEncoding defaultEncoding;
	
	public DownloadTracker(String taskid,Task taskConfig,Cookies initCookies){
		taskId = taskid;
		config = taskConfig;
		downloadThreadPool = new CountableThreadPool(taskConfig.thread);
		if (taskConfig.downloader.equals("default")){
			httpDownloader = new DefaultHttpDownloader(initCookies);
		}else if (taskConfig.downloader.equals("phantomjs")){
			httpDownloader = new PhantomJsDownloader(DownloadServer.getInstance().config.phantomjs, initCookies);
		}else if (taskConfig.downloader.equals("htmlunit")){
			httpDownloader = new HtmlUnitDownloader(initCookies);
		}
		taskContext = new RemoteTaskContext(taskId);
		if (config.encoding.equalsIgnoreCase("gbk")){
			defaultEncoding = PageEncoding.GBK;
		}else if (config.encoding.equalsIgnoreCase("gb2312")){
			defaultEncoding = PageEncoding.GB2312;
		}
		defaultDownloadProcessor = new DefaultDownloadProcessor(taskId,taskConfig.download_root);
	}
	
	public HttpDownloader getHttpDownloader() {
		return httpDownloader;
	}

	public boolean isRuning() {
		return runing;
	}
	
	public boolean isWorking(){
		return downloadThreadPool.getThreadAlive() > 0;
	}
	
	public void updateConfig(int thread,Task taskConfig){
		downloadThreadPool.setThread(thread);
		config = taskConfig;
		pageProcessors.clear();
		logger.info(String.format("%s downloadTracker add %d thread", taskId, thread - downloadThreadPool.getThreadNum()));
	}
	
	private final void asyncInvokeDownload(final HttpRequest request) {
		downloadThreadPool.execute(new Runnable() {
			@Override
			public void run() {
				HttpResponse response = download(request);
				if (response == null){
					return;
				}
				if (request.getMethod() == HttpRequest.Method.POST){
					logger.info(String.format("%s %s post:%s status:%s", request.getUrl(), response.getRedirectUrl(), request.getParams(), response.getStatus()));
				}else{
					logger.info(String.format("%s %s status:%s", request.getUrl(), response.getRedirectUrl(), response.getStatus()));
				}
				
				IProcessor processor = null;
				if (response instanceof Page){
					processor = findConfigProcessor(request.getProcessor());
					if(processor == null){
						logger.warn("Not Found PageProcessor Name:" + request.getProcessor());
						return;
					}
			    }else{
			    	processor = defaultDownloadProcessor;
			    }
				processResponse(processor, response);
				
			}
		});
	}

	private final HttpResponse download(HttpRequest request){
		if (request instanceof PageRequest){
			final PageRequest pageRequest = (PageRequest) request;
			if (defaultEncoding != null){
				pageRequest.setPageEncoding(defaultEncoding);
			}else{
				pageRequest.setPageEncoding(PageEncoding.UTF8);
			}
			Page page = httpDownloader.download(pageRequest);
			return page;
		}else{
			final BinaryRequest binaryRequest = (BinaryRequest) request;
			StreamResponse stream = httpDownloader.downloadBinary(binaryRequest);
			return stream;
		}
	}
	
	private synchronized IProcessor addPageProcessor(String processor) {
		if(pageProcessors.get(processor) != null){
			return pageProcessors.get(processor);
		}
		for (Task.Processor processorConfig : config.processors) {
			if (processor.equals(processorConfig.index)){
				JSONConfigProcessor processorInstance = new JSONConfigProcessor(taskId, processorConfig,httpDownloader);
				processorInstance.setDownloadTracker(this);
				pageProcessors.put(processor, processorInstance);
				break;
			}
		}
		return pageProcessors.get(processor);
	}
	
	public IProcessor findConfigProcessor(String processor) {
		IProcessor pageProcessor = pageProcessors.get(processor);
		if(pageProcessor == null){
			pageProcessor = addPageProcessor(processor);
		}
		return pageProcessor;
	}
	
	/**
	 * @param pageProcessor
	 * @param page
	 * @return
	 */
	private final void processResponse(final IProcessor pageProcessor ,final HttpResponse response) {
		int ret = response.getStatus() / 100;
		ContextModle runtimeContext = null;
		if (ret == 2){
			try {
				List<HttpRequest> newRequests = new ArrayList<HttpRequest>();
				List<CrawlData> objectContainer = new ArrayList<CrawlData>();
			    runtimeContext = pageProcessor.process(response,taskContext,newRequests,objectContainer);
				for (HttpRequest request : newRequests) {
					request.baseRequest(response.getRequest());
				}
				handleResult(newRequests,objectContainer);
			} catch (Exception e) {
				TaskError taskError = new TaskError(taskId.split("_")[0], taskId, TaskError.PROCESSOR_ERROR_TYPE, e);
				if (runtimeContext != null){
					runtimeContext.copyTo(taskError.runtimeContext);
				}
				try {
					DownloadServer.getInstance().getMasterServer().errorStash(taskId, taskError);
				} catch (Exception e1) {}
				logger.error("离线处理异常URL:"+response.getRequest().getUrl(),e);
			}
		}else{
			if(response.getRequest().getHistoryCount() < 3){
				handleResult(Arrays.asList(response.getRequest()), null);
				logger.warn("重新请求URL:"+ response.getRequest().getUrl());
			}else{
				logger.error("下载次数超过" + 3 + ":"+response.getRequest().getUrl()+" 被丢弃");
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
		while (runing) {
			if(downloadThreadPool.getIdleThreadCount() > 0){
				break;//有线程可以工作
			}
			Thread.sleep(10);
		}
		HttpRequest newReq = null;
		while(newReq == null && runing){
			newReq = DownloadServer.getInstance().getMasterServer().pollTaskRequest(taskId);
		}
		return newReq;
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
