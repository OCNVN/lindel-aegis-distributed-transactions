/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lindelit.xatransactions.coordinator;

import com.lindelit.xatransactions.ChildrenCache;
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
class XARollbackSubcoordinator {
    private final static Logger log = Logger.getLogger(XARollbackSubcoordinator.class);
    
    private XATransactionCoordinator xATransactionCoordinator;

    public XARollbackSubcoordinator(XATransactionCoordinator xATransactionCoordinator) {
        this.xATransactionCoordinator = xATransactionCoordinator;
    }
    
    /* ***********************************************
     * ***********************************************
     * Administracion y ejecucion de procesos Rollback
     * ***********************************************
     * ***********************************************
     */
        
    /*
     * Obtener los rollback pendientes
     */ 
    void getPendingRollbacks(){
        // Nuevos rollback a serr gestionados
        xATransactionCoordinator.zkc.zk.getChildren(
                XATransactionCoordinator.TransactionZnodes.FAILED_NAMESPACE.getPath(xATransactionCoordinator.getDistributedTransactionConf()),
                pendingRollbacksChangeWatcher,
                pendingRollbacksChildrenCallback,
                null);
    }
    
    /*
     * Escuchar cambios en el listado de transacciones fallidas
     */
    Watcher pendingRollbacksChangeWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            // Hubo un cambio en el listado de rollbacks pendientes a ejecutar
            if(event.getType() == Watcher.Event.EventType.NodeChildrenChanged){
                log.debug("WATCHER PENDING ROLLBACKS CHILDREN: " + event.getPath());
                // Obtener la nueva lista de transacciones
                getPendingRollbacks();
            }
        }
    };
    
    /*
     * Obtencion de listado de transacciones fallidas de zookeeper
     */
    AsyncCallback.ChildrenCallback pendingRollbacksChildrenCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            
            switch(KeeperException.Code.get(rc)){
            case CONNECTIONLOSS:
                getPendingRollbacks();
                break;
            case OK:
                log.info("[" + xATransactionCoordinator.getMasterId() + "] LISTA DE PENDING ROLBACKS OBTENIDA: " + children.size() + ", PATH: " + path);
                
                // Hubo un cambio en el listado de rollbacks pendientes
                
                // Obtener las tareas que se deben hacer rollback
                List<String> toProcess;
                if(xATransactionCoordinator.getRollbacksCache() == null){
                    xATransactionCoordinator.setRollbacksCache(new ChildrenCache(children));
                    
                    toProcess = children;
                } else {
                    toProcess = xATransactionCoordinator.getRollbacksCache().addedAndSet(children);
                }
                
                for(String rollbackTask: toProcess)
                    log.debug("[" + xATransactionCoordinator.getMasterId() + "] TRANSACCION FALLIDA: " + rollbackTask);
                
                // Asignar las nuevas transacciones para q sean ejecutadas
                // debe iniciar con los que tengan una posicion en el schedule = 0
                // Si es el ultimo worker en el schedule, no se asigna y mas bien se envia el resultado al cliente
                /*if(toProcess != null && !wsc.isLast()){
                    // Determinar el siguiente WorkerSchedule al que se le asigna 
                    Integer currentScheduleIndex = xATransactionCoordinator.getOrderWorkerScheduleD().get(wsc);
                    
                    if(xATransactionCoordinator.getWorkerScheduleOrder().size() - 1 > currentScheduleIndex){// Hay un siguiente Worker en el Schedule
                        xATransactionCoordinator.xaAssignSubcoordinator.assignTasks(toProcess, currentScheduleIndex);
                    }
                    
                    for (String child : toProcess) 
                        log.debug("[" + xATransactionCoordinator.getMasterId() + "] A ASIGNAR AL SIGUIENTE SCHEDULE: " + child);
                }else if(toProcess != null && wsc.isLast()){
                    // El resultado es del ultimo worker en el schedule,
                    // enviando resultado a cliente
                    log.info("[" + xATransactionCoordinator.getMasterId() + "] ES ULTIMO WORKER EN EL SCHEDULE, DEBE NOTIFICAR A CADA CLIENTE RESULTADO");
                    sendResults(toProcess, wsc);
                    
                }*/
                
                break;
            default:
                log.error("[" + xATransactionCoordinator.getMasterId() + "] ERROR AL OBTENER LISTA DE TAREAS: " + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
}
