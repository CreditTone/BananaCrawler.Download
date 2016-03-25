package banana.crawler.dowload.main;

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

import banana.crawler.dowload.impl.DownloadServer;

public class StartDownload {

	public static void main(String[] args)
			throws RemoteException, MalformedURLException, UnknownHostException, ParseException {
		args = (args == null || args.length == 0)?new String[]{"-h"}:args;
		CommandLineParser parser = new BasicParser( );  
		Options options = new Options();  
		options.addOption("h", "help", false, "Print this usage information");  
		options.addOption("m", "master", true, "Set the mater host");  
		CommandLine commandLine = parser.parse( options, args );  
		HelpFormatter formatter = new HelpFormatter();
		if( commandLine.hasOption('h') ) {  
		    formatter.printHelp("Downloader", options);
		    System.exit(0);  
		}
		String master = "localhost";
		if( commandLine.hasOption('m') ) {
			master = commandLine.getOptionValue("m");
		}  
		DownloadServer downloadServer = DownloadServer.initInstance(master);
		if (downloadServer != null){
			LocateRegistry.createRegistry(1098);
			Naming.rebind("rmi://0.0.0.0:1098/downloader", downloadServer);
		}
	}

}
