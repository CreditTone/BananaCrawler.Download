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

import com.googlecode.jsonrpc4j.JsonRpcClient;

import banana.core.protocol.Extractor;



public class JsonRpcExtractor implements Extractor {
	
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
			try {
				reply = client.invokeAndReadResponse("Extractor.RpcParse", new Object[]{params}, String.class, ops, ips);
			} catch (Throwable e) {
				throw new Exception(e);
			}
		}catch(Exception e){
			e.printStackTrace();
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
				return new Socket(remote, 8585); 
			} 
			socket = socketCache.take();
			if (!socket.isClosed()){
				System.out.println("connetion is closed " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
				return socket;
			}
		}
	}
	
	private void pushConnection(Socket connetion) throws InterruptedException{
		socketCache.put(connetion);;
	}
	
}
