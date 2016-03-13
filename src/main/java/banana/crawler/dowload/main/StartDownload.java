package banana.crawler.dowload.main;

import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

import com.banana.common.util.SystemUtil;

import banana.crawler.dowload.impl.DownloadServer;


public class StartDownload {

	public static void main(String[] args) throws RemoteException, MalformedURLException, UnknownHostException {
		 DownloadServer downloadServer = DownloadServer.getInstance();
		 LocateRegistry.createRegistry(1099);
		 Naming.rebind("rmi://localhost:1099/downloader", downloadServer);
	}

}
