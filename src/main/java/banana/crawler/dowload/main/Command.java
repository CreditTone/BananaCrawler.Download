package banana.crawler.dowload.main;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

import com.alibaba.fastjson.JSON;

import banana.core.modle.DownloaderConfig;
import banana.core.protocol.DownloadProtocol;
import banana.core.util.SystemUtil;
import banana.crawler.dowload.impl.DownloadServer;

public class Command {

	public static void main(String[] args) throws Exception {
		args = (args == null || args.length == 0)?new String[]{}:args;
		CommandLineParser parser = new DefaultParser();  
		Options options = new Options();
		options.addOption("h", "help", false, "Print this usage information");  
		options.addOption("c", "config", true, "config file");
		CommandLine commandLine = parser.parse(options, args);
		HelpFormatter formatter = new HelpFormatter();
		if(commandLine.hasOption('h') ) {
		    formatter.printHelp("Downloader", options);
		    System.exit(0);
		}
		File configFile = new File("downloader_config.json");
		if (commandLine.hasOption("c")){
			configFile = new File(commandLine.getOptionValue("c"));
		}else if (!configFile.exists()){
			try{
				configFile = new File(Command.class.getClassLoader().getResource("").getPath() + "/downloader_config.json");
			}catch(Exception e){
				System.out.println("请指定配置文件位置");
				System.exit(0);
			}
		}
		DownloaderConfig config = JSON.parseObject(FileUtils.readFileToString(configFile),DownloaderConfig.class);
		DownloadServer downloadServer = new DownloadServer(config);
		downloadServer.startDownloader();
	}
}
