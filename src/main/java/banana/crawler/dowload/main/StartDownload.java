package banana.crawler.dowload.main;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

import banana.core.exception.CrawlerMasterException;
import banana.core.processor.DataProcessor;
import banana.core.protocol.DownloadProtocol;
import banana.core.protocol.Extractor;
import banana.core.protocol.processor.MongoDBDataProcessor;
import banana.core.util.SystemUtil;
import banana.crawler.dowload.impl.DownloadServer;
import banana.crawler.dowload.impl.JsonRpcExtractor;

public class StartDownload {

	public static void main(String[] args) throws Exception {
		args = (args == null || args.length == 0)?new String[]{"-h"}:args;
		CommandLineParser parser = new BasicParser( );  
		Options options = new Options();
		options.addOption("h", "help", false, "Print this usage information");  
		options.addOption("m", "master", true, "Set the mater host");
		options.addOption("e", "extractor", true, "Set the extractor host");
		options.addOption("mdb", "mongodb", true, "Set the mongodb host and username/password");
		CommandLine commandLine = parser.parse( options, args );
		HelpFormatter formatter = new HelpFormatter();
		if(commandLine.hasOption('h') ) {  
		    formatter.printHelp("Downloader", options);
		    System.exit(0);  
		}
		String master = "localhost";
		if(commandLine.hasOption('m')) {
			master = commandLine.getOptionValue("m");
		}
		Extractor extractor = null;
		if(commandLine.hasOption('e')) {
			String extractorHost = commandLine.getOptionValue("e");
			extractor = new JsonRpcExtractor(extractorHost);
		}
		DataProcessor dataProcessor = null;
		if(commandLine.hasOption("mdb")){
			String mongodbUrl = commandLine.getOptionValue("mdb");
			dataProcessor = new MongoDBDataProcessor(mongodbUrl);
		}
		DownloadServer downloadServer = DownloadServer.initInstance(master);
		downloadServer.extractor = extractor;
		downloadServer.dataProcessor = dataProcessor;
		if (downloadServer != null){
			String localIp = SystemUtil.getLocalIP();
			Server server = new RPC.Builder(new Configuration()).setProtocol(DownloadProtocol.class)
	                .setInstance(downloadServer).setBindAddress("0.0.0.0").setPort(8777)
	                .setNumHandlers(5).build();
	        server.start();
	        downloadServer.getMasterServer().registerDownloadNode(localIp,8777);
		}
	}
}
