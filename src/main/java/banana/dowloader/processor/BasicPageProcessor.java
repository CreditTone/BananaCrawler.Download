package banana.dowloader.processor;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.select.Elements;

import banana.core.download.HttpDownloader;
import banana.core.extractor.Extractor;
import banana.core.modle.BasicWritable;
import banana.core.modle.CrawlData;
import banana.core.modle.MasterConfig;
import banana.core.modle.TaskError;
import banana.core.modle.Task.PageProcessorConfig;
import banana.core.modle.Task.PageProcessorConfig.BlockCondition;
import banana.core.modle.Task.PageProcessorConfig.ContentPrepare;
import banana.core.modle.Task.PageProcessorConfig.RetryCondition;
import banana.core.processor.IProcessor;
import banana.core.request.HttpRequest;
import banana.core.response.HttpResponse;
import banana.core.response.Page;
import banana.core.util.SimpleMailSender;
import banana.core.util.SystemUtil;
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
	
	protected ContentPrepare content_prepare;

	protected DataExtractorConfig page_context_define;

	protected DataExtractorConfig task_context_define;
	
	protected DataExtractorConfig global_context_define;
	
	protected String[] logs;
	
	protected List<BlockCondition> blockConditions;
	
	protected DownloadTracker downloadTracker;
	
	protected RetryCondition retryCondition;
	
	protected BasicPageProcessor(String taskId, PageProcessorConfig proConfig,HttpDownloader downloader) {
		this.index = proConfig.index;
		this.taskId = taskId;
		this.downloader = downloader;
		this.content_prepare = proConfig.content_prepare;
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
		this.blockConditions = proConfig.blockConditions;
		this.retryCondition = proConfig.retry_condition;
	}

	@Override
	public RuntimeContext process(HttpResponse response, Object taskContext, List<HttpRequest> queue,
			List<CrawlData> objectContainer) throws Exception {
		Page page = (Page) response;
		if (page.getTitle() != null) {
			logger.info("process page " + page.getTitle());
		}
		RuntimeContext runtimeContext = null;
		RemoteTaskContext remoteTaskContext = (RemoteTaskContext) taskContext;
		if (content_prepare != null) {
			switch(content_prepare._option) {
			case "html":
				Elements elements = Extractor.catchExSelect(Jsoup.parse(page.getContent()), content_prepare._expression);
				page.setContent(elements.isEmpty() ? null:elements.first().outerHtml());
				break;
			case "json":
				Object jsonResult = Extractor.catchExReadJson(page.getContent(), content_prepare._expression);
				page.setContent(jsonResult == null? null:jsonResult.toString());
				break;
			case "string":
				String str = Extractor.catchExRegex(page.getContent(), content_prepare._expression);
				page.setContent(str);
				break;
			}
			if (page.getContent() == null) {
				logger.warn("内容预处理失败 " + content_prepare._option + " " + content_prepare._expression);
				return null;
			}
		}
		runtimeContext = runtimeContext == null?RuntimeContext.create(page, remoteTaskContext):runtimeContext;
		runtimeContext.setQueue(queue);
		if (page_context_define != null && !runtimeContext.get(PRO_RUNTIME_PREPARED_ERROR, false)) {
			Map<String,Object> pageContextResult = (Map<String, Object>) Extractor.doExtractor
					(page.getContent(), page_context_define.extractorConfig, runtimeContext);
			if (pageContextResult != null) {
				for (Entry<String, Object> entry : pageContextResult.entrySet()) {
					runtimeContext.put(entry.getKey(), entry.getValue());
				}
			}
		}
		if (task_context_define != null && !runtimeContext.get(PRO_RUNTIME_PREPARED_ERROR, false)) {
			Map<String,Object> pageContextResult = (Map<String, Object>) Extractor.doExtractor
					(page.getContent(), task_context_define.extractorConfig, runtimeContext);
			if (pageContextResult != null) {
				for (Entry<String, Object> entry : pageContextResult.entrySet()) {
					remoteTaskContext.put(entry.getKey(), entry.getValue());
				}
			}
		}
		if (global_context_define != null && !runtimeContext.get(PRO_RUNTIME_PREPARED_ERROR, false)) {
			Map<String,Object> pageContextResult = (Map<String, Object>) Extractor.doExtractor
					(page.getContent(), global_context_define.extractorConfig, runtimeContext);
			if (pageContextResult != null) {
				for (Entry<String, Object> entry : pageContextResult.entrySet()) {
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
		for (int i = 0; (logs != null && i < logs.length); i++) {
			logger.info(runtimeContext.parseString(logs[i]));
		}
		for (int i = 0; blockConditions != null && i < blockConditions.size(); i++) {
			if (runtimeContext.parseString(blockConditions.get(i).condition).equals("true")){
				downloader.blockDriver(page.getDriverId());
				if (blockConditions.get(i).email != null){
					MasterConfig config = DownloadServer.getInstance().getMasterServer().getMasterConfig();
					if (config.email != null){
						SimpleMailSender sender = new SimpleMailSender(config.email.host, config.email.username, config.email.password);
						String content = "downloader_node:" + SystemUtil.getLocalIP() + ":" + DownloadServer.getInstance().config.listen + "</br>taskid:" +taskId +"</br>driverId" + page.getDriverId();
						sender.send(config.email.to, "CookieInvalid", content);
					}
				}
				break;
			}
		}
		return runtimeContext;
	}

	public DownloadTracker getDownloadTracker() {
		return downloadTracker;
	}

	public void setDownloadTracker(DownloadTracker downloadTracker) {
		this.downloadTracker = downloadTracker;
	}
//
//	@Override
//	public RuntimeContext process(StreamResponse stream, Object taskContext, List<HttpRequest> queue,
//			List<CrawlData> objectContainer) throws Exception {
	// RemoteTaskContext remoteTaskContext = (RemoteTaskContext) taskContext;
	// return RuntimeContext.create(stream, remoteTaskContext);
//	}

}
