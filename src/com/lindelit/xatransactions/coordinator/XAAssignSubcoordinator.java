/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lindelit.xatransactions.coordinator;

import com.lindelit.xatransactions.XATransactionClient;
import com.lindelit.xatransactions.XATransactionJSONInterpreter;
import com.lindelit.xatransactions.XATransactionResource;
import com.lindelit.xatransactions.XATransactionUtils;
import com.lindelit.xatransactions.XATransactionsBuilder;
import java.util.List;
import java.util.logging.Level;
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
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

/**
 *
 * @author carloslucero
 */
class XAAssignSubcoordinator {
    private final static Logger log = Logger.getLogger(XATaskSubcoordinator.class);
    
    private XATransactionCoordinator xATransactionCoordinator;
    
    public XAAssignSubcoordinator(XATransactionCoordinator xATransactionCoordinator) {
        this.xATransactionCoordinator = xATransactionCoordinator;
    }
    
    /*
     * Asignar lista de tareas para ser ejecutadas por el siguiente WorkerSchedule
     */
    void assignTasks(List<String> tasks, int workerScheduleIndex){
        for(String task : tasks){
            getTaskData(task, workerScheduleIndex);
        }
    }
    
    /*
     * Obtener datos de la tarea a ser asignada
     */
    void getTaskData(String task, int workerScheduleIndex){
        XATransactionsBuilder.WorkerScheduleConfiguration wsc = xATransactionCoordinator.getWorkerScheduleOrder().get(workerScheduleIndex);
        
        xATransactionCoordinator.zkc.zk.getData(
                XATransactionResource.WorkerZnodes.STATUS_NAMESPACE.getPath(wsc, xATransactionCoordinator.getDistributedTransactionConf())+ "/" + task,
                false,
                taskDataCallback,
                workerScheduleIndex);
    }
 
    /*
     * Al obtener los datos de la tarea a ser asignada, se elige un worker
     * aleatoriamente para asignarla y se hace un llamado al metodo
     * para crear dicha asignacion
     */
    AsyncCallback.DataCallback taskDataCallback = new AsyncCallback.DataCallback() {

        @Override
        public void processResult(int rc, String path, Object ctx /* Indice del WorkerSchedule actual */, byte[] data, Stat stat) {
            XATransactionsBuilder.WorkerScheduleConfiguration wsc = xATransactionCoordinator.getWorkerScheduleOrder().get((int) ctx);
            String namespace = XATransactionResource.WorkerZnodes.STATUS_NAMESPACE.getPath(wsc, xATransactionCoordinator.getDistributedTransactionConf());
            String task = path.substring(namespace.length() + 1);
            
            switch(KeeperException.Code.get(rc)){
            case CONNECTIONLOSS:
                getTaskData(task, (int) ctx);
                break;
            case OK:
                
                XATransactionJSONInterpreter xatJsonInterpreter;
                try {
                    xatJsonInterpreter = new XATransactionJSONInterpreter(data);
                    
                    // Si hubo un error en la ejecucion de la transaccion en este punto
                    // se debe hacer un rollback en la transaccion
                    if(xatJsonInterpreter.isErrorStatus()){
                        log.debug("[" + xATransactionCoordinator.getMasterId() + "] El worker respondio con error " + path);
                        
                        TaskAssignmentCtx failedTaCtx = new TaskAssignmentCtx(wsc, null, null, task, data);
                        xATransactionCoordinator.xaRollbackSubcoordinator.createFailedTransaction(failedTaCtx);
                        // Creacion de znode de transaccion fallida en el namespace FAILED
                    }else {// Si la ejecucion por parte del worker es exitosa, continuar con el siguiente worker en el schedule
                        // Debe haber almenos 1 worker del cual elegir en el siguiente worker schedule
                        // La transaccion inicia con los workers que tengan una posicion en el schedule = 0
                        int nextWorkerScheduleIndex = ((int) ctx) + 1;
                        XATransactionsBuilder.WorkerScheduleConfiguration nextWsc = xATransactionCoordinator.getWorkerScheduleOrder().get(nextWorkerScheduleIndex);

                        if(xATransactionCoordinator.getWorkersCache().get(nextWsc).getList() != null && xATransactionCoordinator.getWorkersCache().get(nextWsc).getList().size() > 0){
                            // Elegir un worker randomicamente
                            List<String> list = xATransactionCoordinator.getWorkersCache().get(nextWsc).getList();
                            String designatedWorker = list.get(xATransactionCoordinator.random.nextInt(list.size()));

                            // Path del znode para asignar la tarea al worker elegido
                            String assignmentPath = XATransactionResource.WorkerZnodes.ASSIGN_NAMESPACE.getPath(nextWsc, xATransactionCoordinator.getDistributedTransactionConf())+ 
                                    "/" +
                                    designatedWorker + 
                                    "/" + 
                                    task;

                            // Path del znode para crear nodo de rollback
                            String rollbackPath = XATransactionResource.WorkerZnodes.ROLLBACK_NAMESPACE.getPath(nextWsc, xATransactionCoordinator.getDistributedTransactionConf())+ 
                                    "/" +
                                    task;

                            log.info("[" + xATransactionCoordinator.getMasterId() + "] Asignando tarea  [" + task + "], path: " + assignmentPath);
                            log.info("[" + xATransactionCoordinator.getMasterId() + "] Creando rollback [" + task + "], path: " + rollbackPath);

                            TaskAssignmentCtx taCtx = new TaskAssignmentCtx(wsc, nextWsc, designatedWorker, task, data);
                            createAssignment(assignmentPath, rollbackPath, taCtx);
                        }else{
                            // Si no hay workers detectados aun, volver a intentar asignar la tarea
                            getTaskData(task, (int) ctx);
                        }
                    }
                } catch (ParseException ex) {
                    ex.printStackTrace();
                }
                
                break;
            default:
                log.error("[" + xATransactionCoordinator.getMasterId() + "] Error al obtener datos de la tarea: ", 
                        KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    
    
    /*
     * Creacion de la asignacion
     */
    void createAssignment(String assignmentPath, String rollbackPath, TaskAssignmentCtx taCtx){
        Transaction transaction = xATransactionCoordinator.zkc.zk.transaction();
        
        // CreTransaction acion de asignacion
        transaction.create(
                assignmentPath, 
                taCtx.data, 
                ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                CreateMode.PERSISTENT);
        
        // Creacion de nodo para rollback
        transaction.create(
                rollbackPath, 
                taCtx.data, 
                ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                CreateMode.PERSISTENT);
        
        transaction.commit(assignTaskCallback, taCtx);
    }
    
    AsyncCallback.MultiCallback assignTaskCallback = new AsyncCallback.MultiCallback() {

        @Override
        public void processResult(int rc, String path, Object ctx, List<OpResult> opResults) {
            log.debug("ZKTransaccion assign y rollback: " + path);
            log.trace(opResults);
            
            TaskAssignmentCtx taCtx = (TaskAssignmentCtx) ctx;
            
            switch(KeeperException.Code.get(rc)) { 
            case CONNECTIONLOSS:
                log.warn("[" + xATransactionCoordinator.getMasterId() + "] Conexion perdida al crear Transaccion/Tarea y nodo rollback, intentando crear nuevamente");
                // Path del znode para asignar la tarea al worker elegido
                    String assignmentPath = XATransactionResource.WorkerZnodes.ASSIGN_NAMESPACE.getPath(taCtx.nextWsc, xATransactionCoordinator.getDistributedTransactionConf())+ 
                            "/" +
                            taCtx.designatedWorker + 
                            "/" + 
                            taCtx.task;

                    // Path del znode para crear nodo de rollback
                    String rollbackPath = XATransactionResource.WorkerZnodes.ROLLBACK_NAMESPACE.getPath(taCtx.nextWsc, xATransactionCoordinator.getDistributedTransactionConf())+ 
                            "/" +
                            taCtx.task;
                createAssignment(assignmentPath, rollbackPath, taCtx);
                
                break;
            case OK:
                log.info("[" + xATransactionCoordinator.getMasterId() + "] Transaccion/Tarea y nodo rollback asignada correctamente: " + path);
                
                deleteStatus(taCtx);
                
                break;
            case NODEEXISTS: 
                log.warn("[" + xATransactionCoordinator.getMasterId() + "] Transaccion/Tarea y nodo rollback ya asignada previamente: " + path);
                
                break;
            default:
                log.error("[" + xATransactionCoordinator.getMasterId() + "] Error al asignar Transaccion/Tarea: ", 
                        KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    
    /*
     * Eliminar el status del procesamiento de la tarea del worker una vez que ya
     * a sido asignado al siguiente WorkerSchedule o Enviado como resultado al cliente
     */
    void deleteStatus(TaskAssignmentCtx taCtx){
        xATransactionCoordinator.zkc.zk.delete(
                XATransactionResource.WorkerZnodes.STATUS_NAMESPACE.getPath(taCtx.wsc, xATransactionCoordinator.getDistributedTransactionConf()) + "/" + taCtx.task,
                -1,
                statusDeleteCallback,
                taCtx);
    }
    
    AsyncCallback.VoidCallback statusDeleteCallback = new AsyncCallback.VoidCallback() {

        @Override
        public void processResult(int rc, String path, Object ctx /* TaskAssignmentCtx */) {
            TaskAssignmentCtx taCtx = (TaskAssignmentCtx) ctx;
            
            switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                deleteStatus(taCtx);
                
                break;
            case OK:
                log.info("[" + xATransactionCoordinator.getMasterId() + "] Status eliminado exitosamente: " + path);
                
                break;
            case NONODE:
                log.info("[" + xATransactionCoordinator.getMasterId() + "] Status no existe o eliminada anteriormente: " + path);
                
                break;
            default:
                log.error("[" + xATransactionCoordinator.getMasterId() + "] Error al eliminar Status: " + 
                        KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    
    
    /* ************************************************************
     * ************************************************************
     * Reasignacion de tareas de workers perdidos a workers activos
     * ************************************************************
     * ************************************************************
     */
    void reassignAndSet(List<String> toProcess, XATransactionsBuilder.WorkerScheduleConfiguration wsc){
       
        for(String worker : toProcess){
            getAbsentWorkerTasks(worker, wsc);
            log.debug("[" + xATransactionCoordinator.getMasterId() + "] WORKER PERDIDO: " + worker + ", PERTENECIENTE A SCHEDULE: " + wsc.getName());
        }
        
    }
    
    /*
     * Obtiene las tareas del worker ausente
     */
    void getAbsentWorkerTasks(String worker, XATransactionsBuilder.WorkerScheduleConfiguration wsc){
        log.info("[" + xATransactionCoordinator.getMasterId() + "] Worker perdido, obteniendo asignaciones de worker ausente: " + worker);
        xATransactionCoordinator.zkc.zk.getChildren(
                XATransactionResource.WorkerZnodes.ASSIGN_NAMESPACE.getPath(wsc, xATransactionCoordinator.getDistributedTransactionConf()) + "/" + worker,
                false,
                workerAssignmentCallback,
                wsc);
    }
    
    AsyncCallback.ChildrenCallback workerAssignmentCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx /* WorkerScheduleConfiguration */, List<String> children) {
            XATransactionsBuilder.WorkerScheduleConfiguration wsc = (XATransactionsBuilder.WorkerScheduleConfiguration) ctx;
            String worker = path.substring(path.lastIndexOf("/") + 1);
            
            switch (KeeperException.Code.get(rc)) { 
            case CONNECTIONLOSS:
                
                getAbsentWorkerTasks(worker, wsc);
                
                break;
            case OK:
                log.info("[" + xATransactionCoordinator.getMasterId() + "] Lista de asignaciones de worker ausente " 
                        + path + ", "
                        + children.size() 
                        + " tareas");
                
                /*
                 * Reasignar las tareas del worker ausente 
                 */
                
                for(String task: children) {
                    getDataReassign(path + "/" + task, wsc);                    
                }
                break;
            default:
                log.error("[" + xATransactionCoordinator.getMasterId() + "] Error al obtener asignaciones de worker perdido: ",  KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    
    /* *************************************************
     * *************************************************
     * Recuperacion de tareas asignadas a worker ausente
     * *************************************************
     * *************************************************
     */
    
    /*
     * Obtener datos de tarea a reasignar
     * @param path Path de la tarea a reasignar
     * @param task Nombre de la tarea excluyendo el prefijo del path
     */
    void getDataReassign(String path, XATransactionsBuilder.WorkerScheduleConfiguration wsc){
        log.debug("GET DATA REASSIGN: " + path);
        xATransactionCoordinator.zkc.zk.getData(
                path,
                false,
                getDataReassignCallback,
                wsc);
    }
    
    AsyncCallback.DataCallback getDataReassignCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx /* WorkerScheduleConfiguration */, byte[] data, Stat stat) {
            XATransactionsBuilder.WorkerScheduleConfiguration wsc = (XATransactionsBuilder.WorkerScheduleConfiguration) ctx;
            String task = path.substring(path.lastIndexOf("/") + 1);
            
            switch(KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                getDataReassign(path, wsc); 
                
                break;
            case OK:
                recreateTask(new RecreateTaskCtx(path, task, data, wsc));
                
                break;
            default:
                log.error("[" + xATransactionCoordinator.getMasterId() + "] Error al obtener datos de tarea a reasignar ",
                        KeeperException.create(KeeperException.Code.get(rc)));
            }
        }
    };
    
    void deleteAssignment(String path){
        xATransactionCoordinator.zkc.zk.delete(
                path, 
                -1, 
                taskDeletionCallback, 
                null);
    }
    
    AsyncCallback.VoidCallback taskDeletionCallback = new AsyncCallback.VoidCallback() {
        public void processResult(int rc, String path, Object rtx){
            switch(KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                deleteAssignment(path);
                break;
            case OK:
                log.info("Task correctly deleted: " + path);
                break;
            default:
                log.error("Failed to delete task data" + 
                        KeeperException.create(KeeperException.Code.get(rc), path));
            } 
        }
    };
    
    /*
     * Recrear tarea al ser una reasignacion
     */
    void recreateTask(RecreateTaskCtx ctx){
        // Debe recrear la transaccion
        if(ctx.wsc.isFirst()){
            try {
                // Extraer metadata que se encuentra en el data de la tarea
                JSONObject metadataJson = XATransactionUtils.extractMetadata(ctx.data);
                String clientId = metadataJson.get(XATransactionUtils.AssignMetadataNodes.CLIENT_ID_CHILD.getNode()).toString();
                
                xATransactionCoordinator.zkc.zk.create(
                    XATransactionClient.ClientZnodes.TRANSACTIONS_NAMESPACES.getPath(clientId, xATransactionCoordinator.getDistributedTransactionConf()) + "/" + ctx.task,
                    ctx.data,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                    CreateMode.PERSISTENT,
                    recreateTaskCallback,
                    ctx);
            } catch (ParseException ex) {
                java.util.logging.Logger.getLogger(XATransactionCoordinator.class.getName()).log(Level.SEVERE, null, ex);
            }
            
        }else{ // Debe recrear un status en el anterior WorkerSchedule
            // Obtener el anterior workerSchedule que para recrear su status
            int order = xATransactionCoordinator.getOrderWorkerScheduleD().get(ctx.wsc);
            int beforeWorkerSchedule = order - 1;
            XATransactionsBuilder.WorkerScheduleConfiguration beforeWsc = xATransactionCoordinator.getWorkerScheduleOrder().get(beforeWorkerSchedule);
            
            xATransactionCoordinator.zkc.zk.create(
                XATransactionResource.WorkerZnodes.STATUS_NAMESPACE.getPath(beforeWsc, xATransactionCoordinator.getDistributedTransactionConf()) + "/" + ctx.task,
                ctx.data,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                CreateMode.PERSISTENT,
                recreateTaskCallback,
                ctx);
        }
    }
    
    AsyncCallback.StringCallback recreateTaskCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx /* RecreateTaskCtx */, String name) {
            RecreateTaskCtx recreateTaskCtx = (RecreateTaskCtx) ctx;
            
            switch(KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                recreateTask((RecreateTaskCtx) ctx);
       
                break;
            case OK:
                log.debug("TAREA RECREADA: " + path);
                deleteAssignment(recreateTaskCtx.path);
                
                break;
            case NODEEXISTS:
                log.info("Node exists already, but if it hasn't been deleted, " +
                		"then it will eventually, so we keep trying: " + path);
                recreateTask((RecreateTaskCtx) ctx);
                
                break;
            default:
                log.error("Something wwnt wrong when recreating task", 
                        KeeperException.create(KeeperException.Code.get(rc)));
            }
        }
    };
    
}
