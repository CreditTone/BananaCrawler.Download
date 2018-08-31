package banana.dowloader.processor;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.mongodb.BasicDBObject;

import banana.core.download.HttpDownloader;
import banana.core.extractor2.Extractor;
import banana.core.modle.CrawlData;
import banana.core.modle.TaskError;
import banana.core.modle.Task.PageProcessorConfig;
import banana.core.modle.Task.PageProcessorConfig.Forwarder;
import banana.core.processor.IProcessor;
import banana.core.request.HttpRequest;
import banana.core.request.RequestBuilder;
import banana.core.response.HttpResponse;
import banana.core.response.Page;
import banana.dowloader.config.DataExtractorConfig;
import banana.dowloader.config.RequestExtractorConfig;
import banana.dowloader.impl.DownloadServer;

public class ConfigPageProcessor extends BasicPageProcessor {

	public static final int RUN_MODE = 0;

	public static final int TEST_MODE = 1;

	public static int MODE = RUN_MODE;

	private static Logger logger = Logger.getLogger(ConfigPageProcessor.class);

	private DataExtractorConfig[] dataParser;

	private RequestExtractorConfig[] requestParser;

	public ConfigPageProcessor(String taskId, PageProcessorConfig config, HttpDownloader downloader) {
		super(taskId, config, downloader);
		if (config.crawler_data != null) {
			dataParser = new DataExtractorConfig[config.crawler_data.length];
			for (int i = 0; i < config.crawler_data.length; i++) {
				dataParser[i] = new DataExtractorConfig(config.crawler_data[i]);
			}
		}
		if (config.crawler_request != null) {
			requestParser = new RequestExtractorConfig[config.crawler_request.length];
			for (int i = 0; i < config.crawler_request.length; i++) {
				requestParser[i] = new RequestExtractorConfig(config.crawler_request[i]);
			}
		}
	}

	@Override
	public RuntimeContext process(HttpResponse response, Object taskContext, List<HttpRequest> queue,
			List<CrawlData> objectContainer) throws Exception {
		Page page = (Page) response;
		RuntimeContext runtimeContext = super.process(page, taskContext, queue, objectContainer);
		if (runtimeContext == null) {
			return null;
		}
		for (int i = 0; dataParser != null && i < dataParser.length; i++) {
			DataExtractorConfig dataExtractorConfig = dataParser[i];
			Object dataParseResult = null;
			if (dataExtractorConfig.condition == null
					|| runtimeContext.parseString(dataExtractorConfig.condition).equals("true")) {
				dataParseResult = Extractor.doComplex(dataExtractorConfig.extractorConfig, page.getContent(), runtimeContext);
				if (dataParseResult == null) {
					logger.info(String.format("parsed data is null %s", dataExtractorConfig.extractorConfig));
					continue;
				}
			}
			writeCrawlObject(runtimeContext, objectContainer, page.getRequest().getUrl(), dataParseResult,
					dataExtractorConfig.filter_field);
		}
		
		for (int i = 0; requestParser != null && i < requestParser.length; i++) {
			RequestExtractorConfig requestExtractorConfig = requestParser[i];
			if (requestExtractorConfig.condition != null
					&& runtimeContext.parseString(requestExtractorConfig.condition).equals("false")) {
				continue;
			}
			Object requestParseResult = null;
			if (requestExtractorConfig.foreach != null) {
				List<Map<String,Object>> foreachList = (List<Map<String, Object>>) runtimeContext.get(requestExtractorConfig.foreach);
				if (foreachList == null) {
					logger.info(String.format("foreachList is null %s", requestExtractorConfig.foreach));
					continue;
				}
				for (Map<String,Object> dataContext : foreachList) {
					runtimeContext.setDataContext(dataContext);
					requestParseResult = Extractor.doComplex(requestExtractorConfig.extractorConfig,page.getContent(), runtimeContext);
					writeRequestObject(runtimeContext, queue, requestParseResult, requestExtractorConfig);
					runtimeContext.setDataContextNull();
				}
			}else {
				requestParseResult = Extractor.doComplex(requestExtractorConfig.extractorConfig, page.getContent(),runtimeContext);
				writeRequestObject(runtimeContext, queue, requestParseResult, requestExtractorConfig);
			}
		}
		return runtimeContext;
	}
	
	private boolean containsExclude(List<String> excludes,String url) {
		if (excludes == null){
			return false;
		}
		for (String exclude : excludes) {
			if (url.contains(exclude)){
				return true;
			}
		}
		return false;
	}
	
	
	private void writeRequestObject(RuntimeContext runtimeContext, List<HttpRequest> queue, Object data,
			RequestExtractorConfig requestExtractorConfig) throws Exception {
		if (data instanceof Map) {
			Map<String,Object> item = new HashMap<String,Object>((Map<String, Object>) data){
				@Override
				public Object get(Object key) {
					Object value = super.get(key);
					return value == null? runtimeContext.get(key):value;
				}
			};
			String url = (String) item.get("url");
			if (url ==  null || url.trim().isEmpty() || containsExclude(requestExtractorConfig.excludes, url)) {
				return;
			}
			if (url.startsWith("//")) {
				if ("true".equals(runtimeContext.parseString("{{hasPrefix ._owner_url 'https'}}"))) {
					url = "https:" + url; 
				}else {
					url = "http:" + url;
				}
			}
			if (requestExtractorConfig.filter_field == null || !existsFilterField(requestExtractorConfig.filter_field, item)) {
				RequestBuilder builder = RequestBuilder.custom()
						.setUrl(url)
						.setPriority(requestExtractorConfig.priority)
						.setMethod(requestExtractorConfig.method)
						.setHeaders((Map<String, Object>) item.get("headers"))
						.setParams((Map<String, Object>) item.get("params"))
						.setAttribute((Map<String, Object>) item.get("attribute"))
						.setProcessor(requestExtractorConfig.processor);
				if (runtimeContext.getDataContext() != null) {
					Map<String,Object> attr = builder.getAttribute();
					if (attr == null) {
						attr = new HashMap<String,Object>();
					}
					attr.putAll(runtimeContext.getDataContext());
					builder.setAttribute(attr);
				}
				queue.add(builder.build());
				if (requestExtractorConfig.filter_field != null) {
					addFilterField(requestExtractorConfig.filter_field, item);
				}
			}else {
				logger.info("过滤掉url"+url);
			}
		} else if (data instanceof List) {
			List<Map<String,Object>> listData = (List<Map<String, Object>>) data;
			for (Map<String,Object> item : listData) {
				writeRequestObject(runtimeContext, queue,item, requestExtractorConfig);
			}
		}
	}

	private void writeCrawlObject(RuntimeContext runtimeContext, List<CrawlData> objectContainer, String url, Object data,
			Object filter_field) throws Exception {
		if (data instanceof Map) {
			Map<String,Object> item = (Map<String, Object>) data;
			if (filter_field == null || !existsFilterField(filter_field, item)) {
				CrawlData crawlData = createCrawlData(runtimeContext, url, item);
				objectContainer.add(crawlData);
				if (filter_field != null) {
					addFilterField(filter_field, item);
				}
			}else {
				logger.info("过滤掉data "+filter_field);
			}
		} else if (data instanceof List) {
			List<Map<String,Object>> listData = (List<Map<String, Object>>) data;
			for (Map<String,Object> item : listData) {
				writeCrawlObject(runtimeContext, objectContainer, url, item, filter_field);
			}
		}
	}
	
	private CrawlData createCrawlData(RuntimeContext runtimeContext,String url,Map<String, Object> data) throws IOException {
		BasicDBObject body = new BasicDBObject(data);
		return new CrawlData(taskId, url, body);
	}
	
	private boolean existsFilterField(Object filter_field,Map<String,Object> data) throws Exception {
		if (filter_field instanceof String) {
			String fieldValue = (String) data.get(filter_field);
			return DownloadServer.getInstance().getMasterServer().filterQuery(taskId, fieldValue).get();
		}
		Map<String,Object> mapfilter_field = (Map<String, Object>) filter_field;
		List<String> or_fields = (List<String>) mapfilter_field.get("or");
		if (or_fields != null) {
			for (String fieldName : or_fields) {
				String fieldValue = (String) data.get(fieldName);
				boolean b = DownloadServer.getInstance().getMasterServer().filterQuery(taskId, fieldValue).get();
				if (b) {
					return b;
				}
			}
		}
		List<String> and_fields = (List<String>) mapfilter_field.get("and");
		if (and_fields != null) {
			for (String fieldName : and_fields) {
				String fieldValue = (String) data.get(fieldName);
				boolean b = DownloadServer.getInstance().getMasterServer().filterQuery(taskId, fieldValue).get();
				if (!b) {
					return b;
				}
			}
			return true;
		}
		return false;
	}
	
	private void addFilterField(Object filter_field,Map<String,Object> data) throws Exception {
		if (filter_field instanceof String) {
			String fieldValue = (String) data.get(filter_field);
			DownloadServer.getInstance().getMasterServer().addFilterField(taskId, fieldValue);
			return;
		}
		Map<String,Object> mapfilter_field = (Map<String, Object>) filter_field;
		List<String> or_fields = (List<String>) mapfilter_field.get("or");
		if (or_fields != null) {
			for (String fieldName : or_fields) {
				String fieldValue = (String) data.get(fieldName);
				DownloadServer.getInstance().getMasterServer().addFilterField(taskId, fieldValue);
			}
		}
		List<String> and_fields = (List<String>) mapfilter_field.get("and");
		if (and_fields != null) {
			for (String fieldName : and_fields) {
				String fieldValue = (String) data.get(fieldName);
				DownloadServer.getInstance().getMasterServer().addFilterField(taskId, fieldValue);
			}
		}
	}

}
