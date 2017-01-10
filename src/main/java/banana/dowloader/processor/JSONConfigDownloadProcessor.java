package banana.dowloader.processor;

import java.util.Map;

import org.apache.log4j.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSInputFile;

import banana.core.processor.BinaryProcessor;
import banana.core.protocol.Task.DownloadProcessor;
import banana.core.response.StreamResponse;
import banana.dowloader.impl.DownloadServer;

public class JSONConfigDownloadProcessor implements BinaryProcessor {
	
	private static Logger logger = Logger.getLogger(JSONConfigDownloadProcessor.class);
	
	private DownloadProcessor config;
	
	public JSONConfigDownloadProcessor(DownloadProcessor config) {
		this.config = config;
	}
	
	@Override
	public void process(StreamResponse stream,String ... args) throws Exception {
		GridFS tracker_status = new GridFS(DownloadServer.getInstance().db, args[0]);
		RuntimeContext runtimeContext = RuntimeContext.create(stream.getRequest(), null);
		for (Map<String,String> fileConfig : config.files) {
			String filename = fileConfig.get("filename");
			filename = runtimeContext.parseString(filename);
			String contentType = fileConfig.get("contentType");
			if (contentType != null){
				contentType = runtimeContext.parseString(contentType);
			}
			String aliases = fileConfig.get("aliases");
			if (aliases != null){
				aliases = runtimeContext.parseString(aliases);
			}
			GridFSInputFile file = tracker_status.createFile(stream.getBody());
			file.setFilename(filename);
			file.setContentType(contentType);
			file.setMetaData(new BasicDBObject("aliases", aliases));
			file.save();
		}
	}
	

}
