package banana.dowloader.processor;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.openqa.selenium.remote.RemoteWebDriver;

import banana.core.actions.Actions;
import banana.core.download.HttpDownloader;
import banana.core.download.impl.FirefoxDownloader;
import banana.core.extractor2.Extractor;
import banana.core.modle.BasicWritable;
import banana.core.modle.ContextModle;
import banana.core.modle.CrawlData;
import banana.core.modle.TaskError;
import banana.core.modle.Task.PageProcessorConfig;
import banana.core.modle.Task.PageProcessorConfig.Forwarder;
import banana.core.modle.Task.PageProcessorConfig.RetryCondition;
import banana.core.processor.IProcessor;
import banana.core.request.HttpRequest;
import banana.core.response.HttpResponse;
import banana.core.response.Page;
import banana.dowloader.config.DataExtractorConfig;
import banana.dowloader.impl.DownloadServer;
import banana.dowloader.impl.DownloadTracker;
import banana.dowloader.impl.RemoteTaskContext;

public class BasicPageProcessor implements IProcessor {

	private static Logger logger = Logger.getLogger(BasicPageProcessor.class);
	
	public static final String PRO_RUNTIME_PREPARED_ERROR = "PRO_RUNTIME_PREPARED_ERROR";
	
	protected String index;
	
	protected String taskId;
	
	protected HttpDownloader downloader;
	
	protected DataExtractorConfig page_context_define;

	protected DataExtractorConfig task_context_define;
	
	protected DataExtractorConfig global_context_define;
	
	protected String[] logs;
	
	protected DownloadTracker downloadTracker;
	
	protected RetryCondition retryCondition;
	
	private Forwarder[] forwarders;
	
	private String[] actions;
	
	protected BasicPageProcessor(String taskId, PageProcessorConfig proConfig,HttpDownloader downloader) {
		this.index = proConfig.index;
		this.taskId = taskId;
		this.downloader = downloader;
		if (proConfig.page_context != null) {
			page_context_define = new DataExtractorConfig(proConfig.page_context);
		}
		if (proConfig.task_context != null) {
			task_context_define = new DataExtractorConfig(proConfig.task_context);
		}
		if (proConfig.global_context != null) {
			global_context_define = new DataExtractorConfig(proConfig.global_context);
		}
		this.logs = proConfig.logs;
		this.actions = proConfig.actions;
		this.retryCondition = proConfig.retry_condition;
		this.forwarders = proConfig.forwarders;
	}

	@Override
	public RuntimeContext process(HttpResponse response, Object taskContext, List<HttpRequest> queue,
			List<CrawlData> objectContainer) throws Exception {
		Page page = (Page) response;
		logger.info("process page:" + page.getTitle()+" index:"+index);
		RuntimeContext runtimeContext = null;
		RemoteTaskContext remoteTaskContext = (RemoteTaskContext) taskContext;
		runtimeContext = runtimeContext == null?RuntimeContext.create(page, remoteTaskContext):runtimeContext;
		runtimeContext.setQueue(queue);
		if (page.getContent() != null && page_context_define != null && !runtimeContext.get(PRO_RUNTIME_PREPARED_ERROR, false)) {
			Map<String,Object> pageContextResult = (Map<String, Object>) Extractor.doComplex(page_context_define.extractorConfig, page.getContent(), runtimeContext);
			if (pageContextResult != null) {
				for (Entry<String, Object> entry : pageContextResult.entrySet()) {
					runtimeContext.put(entry.getKey(), entry.getValue());
				}
			}
		}
		if (page.getContent() != null && task_context_define != null && !runtimeContext.get(PRO_RUNTIME_PREPARED_ERROR, false)) {
			Map<String,Object> taskContextResult = (Map<String, Object>) Extractor.doComplex
					(task_context_define.extractorConfig, page.getContent(),  runtimeContext);
			if (taskContextResult != null) {
				for (Entry<String, Object> entry : taskContextResult.entrySet()) {
					remoteTaskContext.put(entry.getKey(), entry.getValue());
				}
			}
		}
		if (page.getContent() != null && global_context_define != null && !runtimeContext.get(PRO_RUNTIME_PREPARED_ERROR, false)) {
			Map<String,Object> globalContextResult = (Map<String, Object>) Extractor.doComplex
					(global_context_define.extractorConfig,page.getContent(),  runtimeContext);
			if (globalContextResult != null) {
				for (Entry<String, Object> entry : globalContextResult.entrySet()) {
					DownloadServer.getInstance().getMasterServer().putGlobalContext(entry.getKey(), new BasicWritable(entry.getValue()));
				}
			}
		}
		if (retryCondition != null) {
			for (int i = 0; retryCondition.or != null && i < retryCondition.or.size() ; i++) {
				String condition = retryCondition.or.get(i);
				if (runtimeContext.parseString(condition).equals("true") && response.getRequest().getHistoryCount() < retryCondition.maxRetry) {
					logger.warn("重试条件"+condition+"成立，第"+(response.getRequest().getHistoryCount()+1)+"次请求URL:" + response.getRequest().getUrl());
					queue.add(response.getRequest());
					return null;
				}
			}
			for (int i = 0; retryCondition.and != null && i < retryCondition.and.size() ; i++) {
				String condition = retryCondition.and.get(i);
				String result = runtimeContext.parseString(condition);
				if (result.equals("false")) {
					break;
				}
				if (result.equals("true") && retryCondition.and.size()-1 == i && response.getRequest().getHistoryCount() < retryCondition.maxRetry) {
					logger.warn("重试条件"+condition+"成立，第"+(response.getRequest().getHistoryCount()+1)+"次请求URL:" + response.getRequest().getUrl());
					queue.add(response.getRequest());
					return null;
				}
			}
		}
		
		if (forwarders != null) {
			logger.info("定义了forwarders");
			for (Forwarder fwd : forwarders) {
				logger.info("parseString:"+runtimeContext.parseString(fwd.condition));
				if (runtimeContext.parseString(fwd.condition).equals("true")) {
					IProcessor result = downloadTracker.findConfigProcessor(fwd.processor);
					if (result != null) {
						result.process(page, taskContext, queue, objectContainer);
						return null;
					}
				}else {
					logger.info("转发条件不成立 condition:"+fwd.condition+"->"+runtimeContext.parseString(fwd.condition));
					logger.info("_owner_url:"+runtimeContext.get("_owner_url"));
				}
			}
			TaskError taskError = new TaskError(taskId.split("_")[0], taskId, TaskError.FORWORD_ERROR_TYPE,
					new Exception("no forword on index " + index));
			runtimeContext.copyTo(taskError.runtimeContext);
			DownloadServer.getInstance().getMasterServer().errorStash(taskId, taskError);
		}
		
		if (actions != null) {
			Actions actionsObj = createActions(runtimeContext);
			actionsObj.start(actions);
		}
		
		for (int i = 0; (logs != null && i < logs.length); i++) {
			logger.info(runtimeContext.parseString(logs[i]));
		}
		return runtimeContext;
	}

	public DownloadTracker getDownloadTracker() {
		return downloadTracker;
	}

	public void setDownloadTracker(DownloadTracker downloadTracker) {
		this.downloadTracker = downloadTracker;
	}
	
	protected Actions createActions(ContextModle runtimeCotext) {
		RemoteWebDriver webDriver = null;
		if (downloader instanceof FirefoxDownloader) {
			webDriver = ((FirefoxDownloader)downloader).getRemoteWebDriver();
		}
		return new Actions(webDriver,runtimeCotext);
	}
//
//	@Override
//	public RuntimeContext process(StreamResponse stream, Object taskContext, List<HttpRequest> queue,
//			List<CrawlData> objectContainer) throws Exception {
	// RemoteTaskContext remoteTaskContext = (RemoteTaskContext) taskContext;
	// return RuntimeContext.create(stream, remoteTaskContext);
//	}

}
