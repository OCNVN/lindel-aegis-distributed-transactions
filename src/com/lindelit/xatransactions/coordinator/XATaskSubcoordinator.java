/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lindelit.xatransactions.coordinator;

import com.lindelit.xatransactions.ChildrenCache;
import com.lindelit.xatransactions.XATransactionClient;
import com.lindelit.xatransactions.XATransactionJSONInterpreter;
import com.lindelit.xatransactions.XATransactionResource;
import com.lindelit.xatransactions.XATransactionUtils;
import com.lindelit.xatransactions.XATransactionsBuilder;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import static org.apache.zookeeper.KeeperException.Code.CONNECTIONLOSS;
import static org.apache.zookeeper.KeeperException.Code.NODEEXISTS;
import static org.apache.zookeeper.KeeperException.Code.NONODE;
import static org.apache.zookeeper.KeeperException.Code.OK;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

/**
 *
 * @author carloslucero
 */
class XATaskSubcoordinator {
    private final static Logger log = Logger.getLogger(XATaskSubcoordinator.class);
    
    private XATransactionCoordinator xATransactionCoordinator;
    
    public XATaskSubcoordinator(XATransactionCoordinator xATransactionCoordinator) {
        this.xATransactionCoordinator = xATransactionCoordinator;
        
        
    }
    
    /* *******************************************************
     * *******************************************************
     * Administracion y asignacion de tareas a WorkersSchedule
     * *******************************************************
     * *******************************************************
     */
        
    /*
     * Obtener las tareas terminadas por workerSchedule
     */ 
    void getTasksDone(){
        for(XATransactionsBuilder.WorkerScheduleConfiguration wsc: xATransactionCoordinator.getWorkerScheduleOrder()){
            getWorkerTasksDone(wsc);
        }
    }
    
    /*
     * Por cada WorkerSchedule obtenemos las tareas ya terminadas
     */
    void getWorkerTasksDone(XATransactionsBuilder.WorkerScheduleConfiguration wsc){
        // Nuevas tareas para ser procesadas por el siguiente workerSchedule
        xATransactionCoordinator.zkc.zk.getChildren(
                XATransactionResource.WorkerZnodes.STATUS_NAMESPACE.getPath(wsc, xATransactionCoordinator.getDistributedTransactionConf()),
                workerTasksDoneChangeWatcher,
                workerTasksDoneGetChildrenCallback,
                wsc);
    }
    
    Watcher workerTasksDoneChangeWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            // Hubo un cambio en los hijos del namespace de las transacciones
            // osease que cambio la lista mijin
            if(event.getType() == Watcher.Event.EventType.NodeChildrenChanged){
                log.debug("WATCHER WORKER TASK DONE CHILDREN: " + event.getPath());
                // Obtener la nueva lista de transacciones
                getWorkerTasksDone(xATransactionCoordinator.getStatusNamespaceWorkersScheduleD().get(event.getPath()));
            }
        }
    };
    
    AsyncCallback.ChildrenCallback workerTasksDoneGetChildrenCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            XATransactionsBuilder.WorkerScheduleConfiguration wsc = (XATransactionsBuilder.WorkerScheduleConfiguration) ctx;
            
            switch(KeeperException.Code.get(rc)){
            case CONNECTIONLOSS:
                getWorkerTasksDone(wsc);
                break;
            case OK:
                log.info("[" + xATransactionCoordinator.getMasterId() + "] LISTA DE TAREAS TERMINADAS OBTENIDA: " + children.size() + ", PATH: " + path);
                
                // Hubo un cambio en las tareas terminadas, las tareas deben
                // reasignarse al siguiente WorkerSchedule acorde
                
                // Obtener las nuevas tareas terminadas
                List<String> toProcess;
                if(xATransactionCoordinator.getTasksCache().get(wsc) == null){
                    xATransactionCoordinator.getTasksCache().put(
                            wsc, 
                            new ChildrenCache(children));
                    
                    toProcess = children;
                } else {
                    toProcess = xATransactionCoordinator.getTasksCache().get(wsc).addedAndSet(children);
                }
                
                // Asignar las nuevas transacciones para q sean ejecutadas
                // debe iniciar con los que tengan una posicion en el schedule = 0
                // Si es el ultimo worker en el schedule, no se asigna y mas bien se envia el resultado al cliente
                if(toProcess != null && !wsc.isLast()){
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
                    
                }
                
                break;
            default:
                log.error("[" + xATransactionCoordinator.getMasterId() + "] ERROR AL OBTENER LISTA DE TAREAS: " + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    
    /*
     * Envia los resultados uno por uno de la transaccion a sus correspondientes clientes
     */
    void sendResults(List<String> tasks, XATransactionsBuilder.WorkerScheduleConfiguration wsc){
        for(String task : tasks){
            getResultData(task, wsc);
        }
    }
    
    /*
     * Obtener datos del resultado a ser enviado a cliente
     */
    void getResultData(String task, XATransactionsBuilder.WorkerScheduleConfiguration wsc){
        TaskAssignmentCtx taCtx;
        taCtx = new TaskAssignmentCtx(wsc, null, null, task, null);

        
        xATransactionCoordinator.zkc.zk.getData(
                XATransactionResource.WorkerZnodes.STATUS_NAMESPACE.getPath(taCtx.wsc, xATransactionCoordinator.getDistributedTransactionConf())+ "/" + taCtx.task,
                false,
                resultDataCallback,
                taCtx);
    }
    
    /*
     * Al obtener los datos de resultado final, estos son enviados
     * al cliente
     */
    AsyncCallback.DataCallback resultDataCallback = new AsyncCallback.DataCallback() {

        @Override
        public void processResult(int rc, String path, Object ctx /* TaskAssignmentCtx */, byte[] data, Stat stat) {
            TaskAssignmentCtx taCtx = (TaskAssignmentCtx) ctx;
            //XATransactionsBuilder.WorkerScheduleConfiguration wsc = taCtx.wsc;
            //String namespace = XATransactionResource.WorkerZnodes.STATUS_NAMESPACE.getPath(wsc, distributedTransactionConf);
            //String task = path.substring(namespace.length() + 1);
            
            switch(KeeperException.Code.get(rc)){
            case CONNECTIONLOSS:
                log.warn("[" + xATransactionCoordinator.getMasterId() + "] Conexion perdida al obtener data de resultado final: " + path);
                
                getResultData(taCtx.task, taCtx.wsc);
                
                break;
            case OK:
                log.info("[" + xATransactionCoordinator.getMasterId() + "] Data de resultado final obtenido, enviando a cliente: " + path);
                taCtx.data = data;
                
                XATransactionJSONInterpreter xatJsonInterpreter;
                try {
                    xatJsonInterpreter = new XATransactionJSONInterpreter(data);
                    JSONObject metadata = xatJsonInterpreter.getMetadata();
                    byte [] result = xatJsonInterpreter.getData();
                    
                    // Si hubo un error en la ejecucion de la transaccion en este punto
                    // se debe hacer rollback a la transaccion
                    if(xatJsonInterpreter.isError()){
                        log.debug("[" + xATransactionCoordinator.getMasterId() + "] El worker respondio con error " + path);
                        
                    }else {// Si la ejecucion por parte del worker es exitosa, enviar resultado al cliente
                        String clientId = metadata.get(XATransactionUtils.AssignMetadataNodes.CLIENT_ID_CHILD.getNode()).toString();
                                
                        // Crea un znode para notificar resultado de la transaccion al cliente
                        xATransactionCoordinator.zkc.zk.create(
                            XATransactionClient.ClientZnodes.RESULTS_NAMESPACES.getPath(clientId, xATransactionCoordinator.getDistributedTransactionConf()) + "/" + taCtx.task, 
                            result, 
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                            CreateMode.PERSISTENT, 
                            resultCreateCallback, 
                            taCtx /* TaskAssignmentCtx */);
                    }
                } catch (ParseException ex) {
                    ex.printStackTrace();
                }
                
                break;
            default:
                log.error("[" + xATransactionCoordinator.getMasterId() + "] Error al obtener data de resultado final: ", 
                        KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    
    /*
     * Callback de creacion de resultado de transaccion
     */
    AsyncCallback.StringCallback resultCreateCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx /* TaskAssignmentCtx */, String name) {
            TaskAssignmentCtx taCtx = (TaskAssignmentCtx) ctx;
            switch(KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                xATransactionCoordinator.zkc.zk.create(
                        path, 
                        taCtx.data, 
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                        CreateMode.PERSISTENT,
                        resultCreateCallback, 
                        ctx /* TaskAssignmentCtx */);
                break;
            case OK:
                log.info("[" + xATransactionCoordinator.getMasterId() + "] Resultado de transaccion creado correctamente: " + name);
                
                //deleteStatus(taCtx);
                clearTransactionData(taCtx);
                break;
            case NODEEXISTS:
                log.warn("[" + xATransactionCoordinator.getMasterId() + "] Resultado de transaccion ya existe o creado previamente: " + path);
                break;
            default:
                log.error("[" + xATransactionCoordinator.getMasterId() + "] Error al crear resultado de transaccion: ", KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    
    /*
     * Eliminar el status del ultimo worker del schedule procesado exitosamente
     * tambien eliminar los znode creados para rollback ya q no son necesarios mas
     */
    void clearTransactionData(TaskAssignmentCtx taCtx){
        Transaction transaction = xATransactionCoordinator.zkc.zk.transaction();
        
        // Eliminar status creado por el ultimo worker schedule
        transaction.delete(
                XATransactionResource.WorkerZnodes.STATUS_NAMESPACE.getPath(taCtx.wsc, xATransactionCoordinator.getDistributedTransactionConf()) + "/" + taCtx.task,
                -1);
        
        // Eliminar znodes creados para rollback
        for(XATransactionsBuilder.WorkerScheduleConfiguration wsc : xATransactionCoordinator.getDistributedTransactionConf().getWorkersScheduleConfigurations()){
            String rollbackPath = XATransactionResource.WorkerZnodes.ROLLBACK_NAMESPACE.getPath(wsc, xATransactionCoordinator.getDistributedTransactionConf()) + "/" + taCtx.task;
            
            transaction.delete(
                    rollbackPath, 
                    -1);
        }
        
        transaction.commit(clearTransactionCallback, taCtx);
    }
    
    AsyncCallback.MultiCallback clearTransactionCallback = new AsyncCallback.MultiCallback() {

        @Override
        public void processResult(int rc, String path, Object ctx, List<OpResult> opResults) {            
            TaskAssignmentCtx taCtx = (TaskAssignmentCtx) ctx;
            
            switch(KeeperException.Code.get(rc)) { 
            case CONNECTIONLOSS:
                log.warn("[" + xATransactionCoordinator.getMasterId() + "] Conexion perdida al hacer clear de la transaccion.");
                clearTransactionData(taCtx);
                
                break;
            case OK:
                log.info("[" + xATransactionCoordinator.getMasterId() + "] Clear de la transaccion realizado correctamente.");
                
                break;
            case NONODE: 
                log.warn("[" + xATransactionCoordinator.getMasterId() + "] Transaccion ya hecha clear previamente ");
                
                break;
            default:
                log.error("[" + xATransactionCoordinator.getMasterId() + "] Error al hacer clear de la transaccion ", 
                        KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    
    
}
