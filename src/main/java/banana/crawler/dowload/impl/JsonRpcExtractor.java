package banana.crawler.dowload.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.googlecode.jsonrpc4j.JsonRpcClient;

import banana.core.protocol.Extractor;

public class JsonRpcExtractor implements Extractor {
	
	private static Logger logger = Logger.getLogger(JsonRpcExtractor.class);
	
	private String remote;
	
	private BlockingQueue<Socket> socketCache = new LinkedBlockingQueue<Socket>();
	
	public JsonRpcExtractor(String remote){
		this.remote = remote;
	}

	@Override
	public String parseData(String config, String body) {
		String reply =  null;
		Socket socket = null;
		try{
			JsonRpcClient client = new JsonRpcClient();
			socket = pollConnection();
			InputStream ips = socket.getInputStream();
	        OutputStream ops = socket.getOutputStream();
	        String params = config + "######" + body;
			reply = client.invokeAndReadResponse("Extractor.RpcParse", new Object[]{params}, String.class, ops, ips);
		}catch(Throwable e){
			logger.warn(String.format("parse error %s", config), e);
		}finally{
			if (socket != null){
				try {
					pushConnection(socket);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		return reply;
	}
	
	private Socket pollConnection() throws InterruptedException, UnknownHostException, IOException{
		Socket socket = null;
		while(true){
			if (socketCache.isEmpty()){
				Socket so = new Socket(remote, 8585);
				so.setTcpNoDelay(true);
				so.setKeepAlive(true);
				so.setSoTimeout(1000 * 60);
				return so; 
			} 
			socket = socketCache.take();
			if (!socket.isClosed()){
				return socket;
			}
			System.out.println("connetion is closed " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
		}
	}
	
	private void pushConnection(Socket connetion) throws InterruptedException{
		socketCache.put(connetion);;
	}
	
}
