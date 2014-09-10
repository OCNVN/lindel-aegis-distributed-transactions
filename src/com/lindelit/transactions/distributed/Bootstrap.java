/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lindelit.transactions.distributed;

import java.io.IOException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;

/**
 *
 * @author carloslucero
 */
public class Bootstrap implements Watcher{
    private final static org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(Bootstrap.class);
    
    com.lindelit.coordinator.ZKConexion zkc;

    public void init() {
        zkc = new com.lindelit.coordinator.ZKConexion();
        try {
            zkc.connect(this);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    public void execute(){
        try{
            zkc.zk.create(
                ApplicationZnodes.APP_NAMESPACE.getPath(), 
                "Root namespace".getBytes(), 
                ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                CreateMode.PERSISTENT);
        }catch (KeeperException | InterruptedException ex){}
        
        try{
            zkc.zk.create(
                ApplicationZnodes.TRANSACTIONS_NAMESPACE.getPath(), 
                "Root transactions namespace".getBytes(), 
                ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                CreateMode.PERSISTENT);
        }catch (KeeperException | InterruptedException ex){}
        
        try{
            zkc.zk.create(
                ApplicationZnodes.RESULTS_NAMESPACE.getPath(), 
                "Root tasks namespace".getBytes(), 
                ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                CreateMode.PERSISTENT);
        }catch (KeeperException | InterruptedException ex){}
        
        try{
            zkc.zk.create(
                ApplicationZnodes.WORKERS_NAMESPACE.getPath(), 
                "Root workers namespace".getBytes(), 
                ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                CreateMode.PERSISTENT);
        }catch (KeeperException | InterruptedException ex){}
        
        try{
            zkc.zk.create(
                ApplicationZnodes.ASSIGNS_NAMESPACE.getPath(), 
                "Root assigns namespace".getBytes(), 
                ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                CreateMode.PERSISTENT);
        }catch (KeeperException | InterruptedException ex){}
        
        try{
            zkc.zk.create(
                ApplicationZnodes.STATUS_NAMESPACE.getPath(), 
                "Root status namespace".getBytes(), 
                ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                CreateMode.PERSISTENT);
        }catch (KeeperException | InterruptedException ex){}
        
        try{
            zkc.zk.create(
                ApplicationZnodes.MASTERS_NAMESPACE.getPath(), 
                "Root masters namespace".getBytes(), 
                ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                CreateMode.PERSISTENT);
        }catch (KeeperException | InterruptedException ex){}
        
        try{
            zkc.zk.create(
                ApplicationZnodes.CLIENTS_NAMESPACE.getPath(), 
                "Root clients namespace".getBytes(), 
                ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                CreateMode.PERSISTENT);
        }catch (KeeperException | InterruptedException ex){}
        
        try{
            zkc.zk.create(
                ApplicationZnodes.TRANSACTION_CLIENT_NAMESPACE.getPath(), 
                "Root clients namespace".getBytes(), 
                ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                CreateMode.PERSISTENT);
        }catch (KeeperException | InterruptedException ex){}
    }

    @Override
    public void process(WatchedEvent event) {
        
    }
    
    
    public enum ApplicationZnodes {
        APP_NAMESPACE       ("/ADT"),
        
        TRANSACTIONS_NAMESPACE (APP_NAMESPACE.getPath() + "/transactions"),

        CLIENTS_NAMESPACE     (APP_NAMESPACE.getPath() + "/clients"),
        TRANSACTION_CLIENT_NAMESPACE     (APP_NAMESPACE.getPath() + "/transaction-client"),
        RESULTS_NAMESPACE     (APP_NAMESPACE.getPath() + "/results"), 
        WORKERS_NAMESPACE   (APP_NAMESPACE.getPath() + "/workers"), 
        ASSIGNS_NAMESPACE   (APP_NAMESPACE.getPath() + "/assigns"),
        STATUS_NAMESPACE   (APP_NAMESPACE.getPath() + "/status"),
        MASTERS_NAMESPACE   (APP_NAMESPACE.getPath() + "/masters");

        //MASTER_ZNODE_SUBFIX     ("master-"),
        //WORKER_ZNODE_SUBFIX     ("worker-"),
        //TASK_ZNODE_SUBFIX       ("task-"),

        //MASTER_ZNODE   (APP_NAMESPACE.getPath() + "/master"), 
        //WORKER_ZNODE (WORKERS_NAMESPACE.getPath() + "/" + WORKER_ZNODE_SUBFIX.getPath()),
        //TASK_ZNODE (TASKS_NAMESPACE.getPath() + "/" + TASK_ZNODE_SUBFIX.getPath());

        private final String path;

        ApplicationZnodes(String path){
            this.path = path;
        }

        public String getPath(){
            return path;
        }
    }
}
