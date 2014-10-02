package com.lindelit.xatransactions;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZKConexion {
    // Valores de conexion por default
    public String hosts = "localhost:2181";
    public int sessionTimeout = 50;
	
    public ZooKeeper zk;
	
    public ZKConexion(){}
	
    public ZKConexion(String hosts, int sessionTimeout) {
        this.hosts = hosts;
        this.sessionTimeout = sessionTimeout;
    }
	
    public ZooKeeper connect(Watcher watcher) throws IOException, InterruptedException{
        //final CountDownLatch connectedSignal = new CountDownLatch(1);
		
        zk = new ZooKeeper(hosts, sessionTimeout, watcher);
		
        return zk;
    }
	
    public void close() throws InterruptedException{
        this.zk.close();
    }
}
