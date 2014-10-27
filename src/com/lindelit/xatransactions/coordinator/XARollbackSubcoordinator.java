/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lindelit.xatransactions.coordinator;

import com.lindelit.xatransactions.ChildrenCache;
import com.lindelit.xatransactions.XATransactionJSONInterpreter;
import com.lindelit.xatransactions.XATransactionResource;
import com.lindelit.xatransactions.XATransactionUtils;
import com.lindelit.xatransactions.XATransactionsBuilder;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import static org.apache.zookeeper.KeeperException.Code.CONNECTIONLOSS;
import static org.apache.zookeeper.KeeperException.Code.NODEEXISTS;
import static org.apache.zookeeper.KeeperException.Code.NONODE;
import static org.apache.zookeeper.KeeperException.Code.OK;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author carloslucero
 */
class XARollbackSubcoordinator {
    private final static Logger log = Logger.getLogger(XARollbackSubcoordinator.class);
    
    private XATransactionCoordinator xATransactionCoordinator;
    // Diccionario failedTransaction -> RollbackTransactionCtx
    private Dictionary<String, RollbackTransactionCtx> rollbackTransactionsCtxDicitonary;

    public XARollbackSubcoordinator(XATransactionCoordinator xATransactionCoordinator) {
        this.xATransactionCoordinator = xATransactionCoordinator;
        
        rollbackTransactionsCtxDicitonary = new Hashtable<>();
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
                
                // Asignar tareas de rollback a los workers
                assignFailedTransactions(toProcess);
                
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
    
    /*
     * Asignacion de tarea para rollback
     */
    void assignFailedTransactions(List<String> failedTransactions){
        for(String failedTransaction : failedTransactions){
            getFailedTransactionData(failedTransaction);
        }
    }
    
    /*
     * Obtener datos de la tarea a ser asignada
     */
    void getFailedTransactionData(String failedTransaction){
        RollbackTransactionCtx rollTransactionCtx = new RollbackTransactionCtx();
        rollTransactionCtx.transaction = failedTransaction;
        
        // Colocamos el contexto del rollback de transaccion en el diccionario
        if(rollbackTransactionsCtxDicitonary.get(rollTransactionCtx.transaction) == null)
            rollbackTransactionsCtxDicitonary.put(rollTransactionCtx.transaction, rollTransactionCtx);
        
        // Obtenemos el data de la transaccion fallida
        xATransactionCoordinator.zkc.zk.getData(
                XATransactionCoordinator.TransactionZnodes.FAILED_NAMESPACE.getPath(xATransactionCoordinator.getDistributedTransactionConf())+ "/" + failedTransaction,
                false,
                failedTransactionDataCallback,
                rollTransactionCtx);
    }
    
    AsyncCallback.DataCallback failedTransactionDataCallback = new AsyncCallback.DataCallback() {

        @Override
        public void processResult(int rc, String path, Object ctx /* RollbackTransactionkCtx */, byte[] data, Stat stat) {
            RollbackTransactionCtx rollTransactionCtx = (RollbackTransactionCtx) ctx;
            
            switch(KeeperException.Code.get(rc)) { 
            case CONNECTIONLOSS:
                log.warn("[" + xATransactionCoordinator.getMasterId() + "] Conexion perdida al obtener data de transaccion fallida, intentando obtener nuevamente: " + path);
                
                xATransactionCoordinator.zkc.zk.getData(
                    path,
                    false,
                    failedTransactionDataCallback,
                    null);
                
                break;
            case OK:
                log.info("[" + xATransactionCoordinator.getMasterId() + "] Data de transaccion fallida obtenida exitosamente: " + path);
                // Determinar los workers que necesitan hacer rollback, asignar tarea para rollback
                XATransactionJSONInterpreter jsonInterpreter;
                try {
                    jsonInterpreter = new XATransactionJSONInterpreter(data);
                    
                    // Agregamos el failedWorkerSchedule y failedorkerScheduleIndex obtenidos del data de la transaccion fallida
                    // Es la misma referencia que esta en el diccionario rollbackTransactionsCtxDicitonary
                    rollTransactionCtx.failedWorkerScheduleIndex = jsonInterpreter.getFailedWorkerScheduleIndex();
                    rollTransactionCtx.failedWorkerSchedule = jsonInterpreter.getFailedWorkerSchedule();

                    log.debug("WorkerScheduleIndex fallido: " + rollTransactionCtx.failedWorkerScheduleIndex + ", " + rollTransactionCtx.failedWorkerSchedule);
                    
                    // Obtener data de rollback de cada worker involucrado hasta este punto en la transaccion
                    // Para eso recorremos desde el primer worker schedule hasta el actual que fallo
                    for(int i = 0; i <= rollTransactionCtx.failedWorkerScheduleIndex.intValue(); i++){
                        // Obtenemos el WorkerScheduleConfiguration correspondiente segun su indice
                        XATransactionsBuilder.WorkerScheduleConfiguration wsc = xATransactionCoordinator.getWorkerScheduleOrder().get(i);
                        
                        // Creamos el contexto de tarea para rollback y asignamos las propiedades
                        // transaction y wsc(WorkerScheduleConfiguration)
                        RollbackTaskCtx rollTaskCtx = new RollbackTaskCtx();
                        rollTaskCtx.transaction = rollTransactionCtx.transaction;
                        rollTaskCtx.wsc = wsc;
                        
                        // Obtenemos data para rollback de la transaccion dependiendo del wsc (WorkerScheduleConfiguration)
                        xATransactionCoordinator.zkc.zk.getData( 
                                XATransactionResource.WorkerZnodes.ROLLBACK_NAMESPACE.getPath(wsc, xATransactionCoordinator.getDistributedTransactionConf()) + "/" + rollTransactionCtx.transaction,
                                null, 
                                rollbackDataCallback, 
                                rollTaskCtx);
                    }
                    
                } catch (ParseException ex) {
                    java.util.logging.Logger.getLogger(XARollbackSubcoordinator.class.getName()).log(Level.SEVERE, null, ex);
                }
                
                break;
            case NONODE: 
                log.warn("[" + xATransactionCoordinator.getMasterId() + "] Znode de transaccion fallida no existe: " + path);
                
                break;
            default:
                log.error("[" + xATransactionCoordinator.getMasterId() + "] Error al obtener data de transaccion fallida: ", 
                        KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    
    AsyncCallback.DataCallback rollbackDataCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx /* RollbackTaskCtx */, byte[] data, Stat stat) {
            RollbackTaskCtx rollTaskCtx = (RollbackTaskCtx) ctx;
        
            switch(KeeperException.Code.get(rc)) { 
            case CONNECTIONLOSS:
                log.warn("[" + xATransactionCoordinator.getMasterId() + "] Conexion perdida al obtener data de rollback, intentando obtener nuevamente: " + path);
                
                xATransactionCoordinator.zkc.zk.getData(
                    path,
                    false,
                    rollbackDataCallback,
                    ctx);
                
                break;
            case OK:
                log.info("[" + xATransactionCoordinator.getMasterId() + "] Data de rollback obtenido exitosamente: " + path + ", " + new String(data));
                // Agregamos el data obtenido a rollTaskCtx
                rollTaskCtx.rollbackData = data;
                
                // Agregar el rollTaskCtx al RollbackTransactionCtx, primero consultamos del diccionaro segun la transaccion
                // para luego agregarlo
                RollbackTransactionCtx rollTransactionCtx = rollbackTransactionsCtxDicitonary.get(rollTaskCtx.transaction);
                rollTransactionCtx.addRollbackTask(rollTaskCtx);
                
                // Preguntamos si ya hemos obtenido todos los rollTaskCtx necesarios para asignar los rollback a los workers
                if(rollTransactionCtx.isReady()){
                    log.info("[" + xATransactionCoordinator.getMasterId() + "] La transaccion " + rollTransactionCtx.transaction + ", datos listos para asignar tareas rollback");
                    
                    for (RollbackTaskCtx rollTaskCtxIt : rollTransactionCtx.getRollbackTasks().values() ) {
                        log.debug("Rollback en transaccion: " + rollTransactionCtx.transaction + ", con worker-schedule: " + rollTaskCtxIt.wsc.getName() + " y data: " + new String(rollTaskCtxIt.rollbackData));
                    }
                }
                
                break;
            case NONODE: 
                log.warn("[" + xATransactionCoordinator.getMasterId() + "] Znode de data de rollback no existe: " + path);
                
                break;
            default:
                log.error("[" + xATransactionCoordinator.getMasterId() + "] Error al obtener data de rollback: ", 
                        KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    
    /*
     * Creacion de znode de transaccion fallida
     */
    void createFailedTransaction(TaskAssignmentCtx taCtx){
        log.debug("Creando transaccion fallida en worker schedule: " + taCtx.wsc.getName() + ": " + taCtx.wsc.getSheduleOrder());
        
        byte[] dataEdited;
        try {
            dataEdited = addWorkerScheduleFailedPhaseData(taCtx.data, taCtx.wsc);
            xATransactionCoordinator.zkc.zk.create(
                XATransactionCoordinator.TransactionZnodes.FAILED_NAMESPACE.getPath(xATransactionCoordinator.getDistributedTransactionConf()) + "/" + taCtx.task, 
                dataEdited, 
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT, 
                failedTaskCallback, 
                taCtx);
        } catch (ParseException ex) {
            ex.printStackTrace();
        }   
    }
    
    AsyncCallback.StringCallback failedTaskCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            TaskAssignmentCtx taCtx = (TaskAssignmentCtx) ctx;
            
            switch(KeeperException.Code.get(rc)) { 
            case CONNECTIONLOSS:
                log.warn("[" + xATransactionCoordinator.getMasterId() + "] Conexion perdida al crear znode de transaccion fallida, intentando crear nuevamente: " + path);
                
                createFailedTransaction(taCtx);
                
                break;
            case OK:
                log.info("[" + xATransactionCoordinator.getMasterId() + "] Znode de transaccion fallida creado correctamente: " + path);
                
                break;
            case NODEEXISTS: 
                log.warn("[" + xATransactionCoordinator.getMasterId() + "] Znode de transaccion fallida ya existe o creado previamente: " + path);
                
                break;
            default:
                log.error("[" + xATransactionCoordinator.getMasterId() + "] Error al crear znode de transaccion fallida: ", 
                        KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    
    /*
     * Edita Fase de ejecucion por default para agregar 
     */
    protected byte[] addWorkerScheduleFailedPhaseData(byte[] data, XATransactionsBuilder.WorkerScheduleConfiguration wsc) throws ParseException{
        JSONParser parser=new JSONParser();
        
        String dataString = new String(data);
        Object dataObject = parser.parse(dataString);
        JSONObject dataJson = (JSONObject) dataObject;
        
        JSONObject phaseJsonChild = (JSONObject) dataJson.get(XATransactionUtils.TransactionPhaseNodes.XA_PHASE_NODE.getNode());
        phaseJsonChild.put(XATransactionUtils.TransactionPhaseNodes.FAILED_WORKER_SCHEDULE_CHILD.getNode(), wsc.getName());
        phaseJsonChild.put(XATransactionUtils.TransactionPhaseNodes.FAILED_WORKER_SCHEDULE_INDEX_CHILD.getNode(), wsc.getSheduleOrder());
        
        return dataJson.toJSONString().getBytes();
    }
    
}