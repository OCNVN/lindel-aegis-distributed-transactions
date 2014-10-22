/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lindelit.xatransactions.coordinator;

import com.lindelit.xatransactions.ChildrenCache;
import com.lindelit.xatransactions.XATransactionResource;
import com.lindelit.xatransactions.XATransactionsBuilder;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import static org.apache.zookeeper.KeeperException.Code.CONNECTIONLOSS;
import static org.apache.zookeeper.KeeperException.Code.OK;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 *
 * @author carloslucero
 */
class XAWorkerSubcoordinator {
    private final static Logger log = Logger.getLogger(XAWorkerSubcoordinator.class);
    
    private XATransactionCoordinator xATransactionCoordinator;

    public XAWorkerSubcoordinator(XATransactionCoordinator xATransactionCoordinator) {
        this.xATransactionCoordinator = xATransactionCoordinator;
    }
    
    /* ****************************************
     * ****************************************
     * Administracion y coordinacion de workers
     * ****************************************
     * ****************************************
     */
    void getWorkers(){
        // Por cada tipo de workers alojados en namespaces diferentes
        for(XATransactionsBuilder.WorkerScheduleConfiguration wsc : xATransactionCoordinator.getDistributedTransactionConf().getWorkersScheduleConfigurations()){
            xATransactionCoordinator.zkc.zk.getChildren(
                    XATransactionResource.WorkerZnodes.WORKER_NAMESPACE.getPath(wsc, xATransactionCoordinator.getDistributedTransactionConf()), 
                    workersChangeWatcher, 
                    workersGetChildrenCallback, 
                    wsc);
        }
    }
    
    Watcher workersChangeWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            // Hubo un cambio en los hijos del namespace workers
            // osease que cambio la lista mijin
            if(event.getType() == Watcher.Event.EventType.NodeChildrenChanged){
                // Obtener la nueva lista de workers
                getWorkers();
            }
        }
    };
    
    AsyncCallback.ChildrenCallback workersGetChildrenCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx /* WorkerScheduleConfiguration */, List<String> children) {
            switch(KeeperException.Code.get(rc)){
            case CONNECTIONLOSS:
                getWorkers();
                break;
            case OK:
                log.info("[" + xATransactionCoordinator.getMasterId() + "] LISTA DE WORKERS OBTENIDA EN: " + path + ", " + children.size() + " WORKERS");
                /*for (String child : children) 
                    log.debug("[" + masterId + "] WORKER: " + child);*/
                
                // Hubo un cambio en los workers disponibles, las tareas deben
                // reasignarse acorde
                
                // Si workers han desaparecido, reasignar sus tareas a otros workers
                //reassignAndSet(children, (DistributedTransactionsBuilder.WorkerScheduleConfiguration) ctx);
                updateWorkerCache(children, (XATransactionsBuilder.WorkerScheduleConfiguration) ctx);
                break;
            default:
                log.error("[" + xATransactionCoordinator.getMasterId() + "] ERROR AL OBTENER LISTA DE WORKERS: " + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    
    void updateWorkerCache(List<String> children, XATransactionsBuilder.WorkerScheduleConfiguration wsc){
        // Lista de workers perdidos
        List<String> workerLoss;
        
        // Lista de workers agregados
        List<String> workerAdded;
        
        if(xATransactionCoordinator.getWorkersCache().get(wsc) == null) {    
            xATransactionCoordinator.getWorkersCache().put(wsc, new ChildrenCache());
            
            workerLoss = null;
            workerAdded = xATransactionCoordinator.getWorkersCache().get(wsc).addedAndSet(children);
        } else {
            // Workers perdidos
            workerLoss = xATransactionCoordinator.getWorkersCache().get(wsc).onlyRemoved(children);
            // Workers agregados
            workerAdded = xATransactionCoordinator.getWorkersCache().get(wsc).onlyAdded(children);
            
            if(workerLoss != null && workerLoss.size() > 0)
                xATransactionCoordinator.xaAssignSubcoordinator.reassignAndSet(workerLoss, wsc);
            
            xATransactionCoordinator.getWorkersCache().get(wsc).onlyAdd(children);
        }
    }
}
