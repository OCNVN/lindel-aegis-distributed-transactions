/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lindelit.xatransactions;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.lindelit.xatransactions.Bootstrap.ApplicationZnodes;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
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
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author carloslucero
 */
public class XATransactionCoordinator implements Watcher{
    private final static Logger log = Logger.getLogger(XATransactionCoordinator.class);
    
    // Configuracion de la transaccion distribuida
    XATransactionsBuilder.DistributedTransactionConfiguration distributedTransactionConf;
    
    // Id del master
    private String masterId;
    
    // WorkersCache por cada WorkerSchedule
    Dictionary<XATransactionsBuilder.WorkerScheduleConfiguration, ChildrenCache>  workersCache;
    // WorkerScheduleConfiguration ordenado segun el orden establecido para la transaccion
    ArrayList<XATransactionsBuilder.WorkerScheduleConfiguration>  workerScheduleOrder;
    // WorkersCache por cada Namespace
    Dictionary<String, XATransactionsBuilder.WorkerScheduleConfiguration>  statusNamespaceWorkersScheduleD;
    // Orden de ejecucion por objeto WorkerScheduleConfiguration 
    Dictionary<XATransactionsBuilder.WorkerScheduleConfiguration, Integer>  orderWorkerScheduleD;
    
    // Posibles estados en los que puede estar el master
    enum TransaccionMasterStates {RUNNING, ELECTED, NOTELECTED};
    
    private TransaccionMasterStates state = TransaccionMasterStates.RUNNING;
    
    // Conexion a zookeeper
    ZKConexion zkc;
    
    // Generar numeros aleatorios
    private Random random = new Random();

    public void init() {
        zkc = new ZKConexion();
        try {
            zkc.connect(this);
            
            // Iniciar recursos necesarios en zookeeper
            TransactionMasterBootstrap tmb = new TransactionMasterBootstrap(distributedTransactionConf);
            tmb.execute();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    // Znodes necesarios para el funcionamiento del master
    public enum TransactionZnodes {
        TRANSACTION_ZNODE {
            public String getPath(XATransactionsBuilder.DistributedTransactionConfiguration dt){
                String path = ApplicationZnodes.TRANSACTIONS_NAMESPACE.getPath() + 
                    "/" +
                    dt.getId();
                
                return path;
            }
        },
        
        MASTER_ZNODE {
            public String getPath(XATransactionsBuilder.DistributedTransactionConfiguration dt){
                String path = ApplicationZnodes.MASTERS_NAMESPACE.getPath() + 
                    "/" +
                    dt.getId();
                
                return path;
            }
        },
        
        WORKERS_NAMESPACE {
            public String getPath(XATransactionsBuilder.DistributedTransactionConfiguration dt){
                String path = ApplicationZnodes.WORKERS_NAMESPACE.getPath() + 
                    "/" +
                    dt.getId();
                
                return path;
            }
        },
        
        TRANSACTIONS_NAMESPACE {
            public String getPath(XATransactionsBuilder.DistributedTransactionConfiguration dt){
                String path = ApplicationZnodes.TRANSACTIONS_NAMESPACE.getPath() + 
                    "/" +
                    dt.getId();
                
                return path;
            }
        },
        
        ASSIGNS_NAMESPACE {
            public String getPath(XATransactionsBuilder.DistributedTransactionConfiguration dt){
                String path = ApplicationZnodes.ASSIGNS_NAMESPACE.getPath() + 
                    "/" +
                    dt.getId();
                
                return path;
            }
        },
        
        STATUS_NAMESPACE {
            public String getPath(XATransactionsBuilder.DistributedTransactionConfiguration dt){
                String path = ApplicationZnodes.STATUS_NAMESPACE.getPath() + 
                    "/" +
                    dt.getId();
                
                return path;
            }
        },
        
        ROLLBACK_NAMESPACE {
            public String getPath(XATransactionsBuilder.DistributedTransactionConfiguration dt){
                String path = ApplicationZnodes.ROLLBACK_NAMESPACE.getPath() + 
                    "/" +
                    dt.getId();
                
                return path;
            }
        },
        
        RESULTS_NAMESPACE {
            public String getPath(XATransactionsBuilder.DistributedTransactionConfiguration dt){
                String path = ApplicationZnodes.RESULTS_NAMESPACE.getPath() + 
                    "/" +
                    dt.getId();
                
                return path;
            }
        },
        
        TRANSACTION_CLIENTS_NAMESPACE {
            public String getPath(XATransactionsBuilder.DistributedTransactionConfiguration dt){
                String path = ApplicationZnodes.TRANSACTION_CLIENT_NAMESPACE.getPath() + 
                    "/" +
                    dt.getId();
                
                return path;
            }
        };

        //MASTER_ZNODE_SUBFIX     ("master-"),
        //WORKER_ZNODE_SUBFIX     ("worker-"),
        //TASK_ZNODE_SUBFIX       ("task-"),

        public abstract String getPath(XATransactionsBuilder.DistributedTransactionConfiguration dt);
    }

    public XATransactionCoordinator(XATransactionsBuilder.DistributedTransactionConfiguration distributedTransactionConf, String masterId) {
        this.distributedTransactionConf = distributedTransactionConf;
        this.masterId = masterId;
        
        workersCache = new Hashtable<>();
        tasksCache = new Hashtable<>();
        statusNamespaceWorkersScheduleD = new Hashtable<>();
        orderWorkerScheduleD = new Hashtable<>();
        transactionsCache = new Hashtable<>();
        
        // Armamos diccionar que relaciona el namespace de workerSchedule con
        // el objteto que contiene la configuracion para es workerSchedule
        for(XATransactionsBuilder.WorkerScheduleConfiguration wsc: distributedTransactionConf.getWorkersScheduleConfigurations()){
            statusNamespaceWorkersScheduleD.put(XATransactionResource.WorkerZnodes.STATUS_NAMESPACE.getPath(wsc, distributedTransactionConf), wsc);
        }
        
        // Por el momento usamos tal cual el orden extraido del archivo de configuracion,
        // Esto debe cambiarse y ordenar la lista usando el atributo schedule-order
        // para probar esto sera suficiente
        workerScheduleOrder = distributedTransactionConf.getWorkersScheduleConfigurations();
        
        // Creamos el diccionario de indice de orden de schedule segun objeto
        // WorkerScheduleConfiguration
        for (int i = 0 ; i <  workerScheduleOrder.size(); i++) {
            XATransactionsBuilder.WorkerScheduleConfiguration wsc = workerScheduleOrder.get(i);
            
            orderWorkerScheduleD.put(wsc, i);
        }
        
        log.debug(TransactionZnodes.TRANSACTION_ZNODE + ": \t" + TransactionZnodes.TRANSACTION_ZNODE.getPath(distributedTransactionConf));
        log.debug(TransactionZnodes.MASTER_ZNODE + ": \t \t" + TransactionZnodes.MASTER_ZNODE.getPath(distributedTransactionConf));
        log.debug(TransactionZnodes.WORKERS_NAMESPACE + ": \t" + TransactionZnodes.WORKERS_NAMESPACE.getPath(distributedTransactionConf));
        log.debug(TransactionZnodes.TRANSACTIONS_NAMESPACE + ": \t" + TransactionZnodes.TRANSACTIONS_NAMESPACE.getPath(distributedTransactionConf));
        log.debug(TransactionZnodes.ASSIGNS_NAMESPACE + ": \t" + TransactionZnodes.ASSIGNS_NAMESPACE.getPath(distributedTransactionConf));
        log.debug(TransactionZnodes.STATUS_NAMESPACE + ": \t" + TransactionZnodes.STATUS_NAMESPACE.getPath(distributedTransactionConf));
    }

    @Override
    public void process(WatchedEvent event) {
        
    }
    
    /* **********************************************
     * **********************************************
     * Adquisicion del rol master y eleccion de lider
     * **********************************************
     * **********************************************
     */
    
    /*
     * Intentar asumir el rol master
     */
    public void runForMaster(){
        zkc.zk.create(
                TransactionZnodes.MASTER_ZNODE.getPath(distributedTransactionConf), 
                this.masterId.getBytes(), 
                ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                CreateMode.EPHEMERAL, 
                masterCreateCallback, 
                null);
    }
    
    private AsyncCallback.StringCallback masterCreateCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(KeeperException.Code.get(rc)){
            case CONNECTIONLOSS:
                log.info("[" + masterId + "] Conexion perdida al crear lock Master: " + path);
                checkMaster();
                break;
            case OK:
                state = TransaccionMasterStates.ELECTED;
                log.info("[" + masterId + "] Eh sido elegido como MASTER: " + path);
                // Asumir el rol master
                // ...
                takeLeadership();
                break;
            case NODEEXISTS:
                state = TransaccionMasterStates.NOTELECTED;
                log.info("[" + masterId + "] El master ya a sido elegido: ");
                // Monitoreo del master ya que otro proceso adquirio el rol
                masterExists();
                break;
            default:
                state = TransaccionMasterStates.NOTELECTED;
		log.error("[" + masterId + "] Ocurrio un error al correr como master: " + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    
    /*
     * Monitoreo del master en caso de que otro proceso haya asumido el rol,
     * de esta manera sabremos cuando el master a caido y a liberado el lock
     */
    private void masterExists(){
        zkc.zk.exists(
                TransactionZnodes.MASTER_ZNODE.getPath(distributedTransactionConf), 
                masterExistsWatcher, 
                masterExistsCallback, 
                null);
    }
    
    Watcher masterExistsWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if(event.getType() == Event.EventType.NodeDeleted){
                log.info("[" + masterId + "] Watcher Nodo master eliminado: " + event.getPath());
                
                // El lock del znode master a sido liberado. 
                // Intentar obtener el rol master.
                runForMaster();
            }
        }
    };
    
    AsyncCallback.StatCallback masterExistsCallback = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch(KeeperException.Code.get(rc)){
            case CONNECTIONLOSS:
                masterExists();
                break;
            case OK:
                // Master existe
                if(stat == null){
                    state = TransaccionMasterStates.RUNNING;
                    runForMaster();
                }
                break;
            case NONODE:
                // No hay master elegido por lo tanto no existe el lock
                state = TransaccionMasterStates.RUNNING;
                runForMaster();
                log.info("[" + masterId + "] El master no existe, tratando de adquirir rol master");
                break;
            default:
                
                break;
            }
        }
    };
    
    /*
     * En caso de perdida de conexion al crear el MASTER_ZNODE, este puede haberse
     * creado pero no recibimos la confirmacion, asi que verificamos si se a
     * creado o no durante la perdia de conexion
     */
    private void checkMaster(){
        zkc.zk.getData(
            TransactionZnodes.MASTER_ZNODE.getPath(distributedTransactionConf), 
            false, 
            masterCheckCallback, 
            null);
    }
    
    AsyncCallback.DataCallback masterCheckCallback = new AsyncCallback.DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                log.warn("[" + masterId + "] MasterCheck Conexion perdida");     
                checkMaster();
                break;
            case NONODE:
                log.debug("[" + masterId + "] MasterCheck el lider no a sido elegido intentar tomar el rol ");     
            	runForMaster();
                break; 
            case OK:
                log.debug("[" + masterId + "] MasterCheck el lock del lider existe verificando si soy yo"); 
                if(masterId.equals( new String(data) ) ) {
                    state = TransaccionMasterStates.ELECTED;
                    takeLeadership();
                } else {
                    state = TransaccionMasterStates.NOTELECTED;
                    masterExists();
                }
                
                break;
            default:
                log.error("Error when reading data.", KeeperException.create(KeeperException.Code.get(rc), path));               
            }
        } 
    };
    
    /*
     * Asume el rol master y realiza las tareas correspondientes
     */
    void takeLeadership(){
        log.info("[" + masterId + "] EJECUTANDO TAREAS DEL ROL MASTER ");
        
        // Administracion y coordinacion de workers
        getWorkers();
        
        // Administracion y coordinacion de clientes
        getClients();
        
        // Administracion y coordinacion de transacciones
        //getTransactions();
        
        // Administracion y coordinacion de tareas en WorkerSchedule
        getTasksDone();
    }
    
    /* ****************************************
     * ****************************************
     * Administracion y coordinacion de workers
     * ****************************************
     * ****************************************
     */
    void getWorkers(){
        // Por cada tipo de workers alojados en namespaces diferentes
        for(XATransactionsBuilder.WorkerScheduleConfiguration wsc :distributedTransactionConf.getWorkersScheduleConfigurations()){
            zkc.zk.getChildren(
                    XATransactionResource.WorkerZnodes.WORKER_NAMESPACE.getPath(wsc, distributedTransactionConf), 
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
            if(event.getType() == Event.EventType.NodeChildrenChanged){
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
                log.info("[" + masterId + "] LISTA DE WORKERS OBTENIDA EN: " + path + ", " + children.size() + " WORKERS");
                /*for (String child : children) 
                    log.debug("[" + masterId + "] WORKER: " + child);*/
                
                // Hubo un cambio en los workers disponibles, las tareas deben
                // reasignarse acorde
                
                // Si workers han desaparecido, reasignar sus tareas a otros workers
                //reassignAndSet(children, (DistributedTransactionsBuilder.WorkerScheduleConfiguration) ctx);
                updateWorkerCache(children, (XATransactionsBuilder.WorkerScheduleConfiguration) ctx);
                break;
            default:
                log.error("[" + masterId + "] ERROR AL OBTENER LISTA DE WORKERS: " + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    
    void updateWorkerCache(List<String> children, XATransactionsBuilder.WorkerScheduleConfiguration wsc){
        // Lista de workers perdidos
        List<String> workerLoss;
        
        // Lista de workers agregados
        List<String> workerAdded;
        
        if(workersCache.get(wsc) == null) {    
            workersCache.put(wsc, new ChildrenCache());
            
            workerLoss = null;
            workerAdded = workersCache.get(wsc).addedAndSet(children);
        } else {
            // Workers perdidos
            workerLoss = workersCache.get(wsc).onlyRemoved(children);
            // Workers agregados
            workerAdded = workersCache.get(wsc).onlyAdded(children);
            
            if(workerLoss != null && workerLoss.size() > 0)
                reassignAndSet(workerLoss, wsc);
            
            workersCache.get(wsc).onlyAdd(children);
        }
    }
    
    /*
     * Reasignar tareas de workers perdidos a workers activos
     */
    void reassignAndSet(List<String> toProcess, XATransactionsBuilder.WorkerScheduleConfiguration wsc){
       
        for(String worker : toProcess){
            getAbsentWorkerTasks(worker, wsc);
            log.debug("[" + masterId + "] WORKER PERDIDO: " + worker + ", PERTENECIENTE A SCHEDULE: " + wsc.getName());
        }
        
    }
    
    /*
     * Obtiene las tareas del worker ausente
     */
    void getAbsentWorkerTasks(String worker, XATransactionsBuilder.WorkerScheduleConfiguration wsc){
        log.info("[" + masterId + "] Worker perdido, obteniendo asignaciones de worker ausente: " + worker);
        zkc.zk.getChildren(
                XATransactionResource.WorkerZnodes.ASSIGN_NAMESPACE.getPath(wsc, distributedTransactionConf) + "/" + worker,
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
                log.info("[" + masterId + "] Lista de asignaciones de worker ausente " 
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
                log.error("[" + masterId + "] Error al obtener asignaciones de worker perdido: ",  KeeperException.create(KeeperException.Code.get(rc), path));
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
        zkc.zk.getData(
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
                log.error("[" + masterId + "] Error al obtener datos de tarea a reasignar ",
                        KeeperException.create(KeeperException.Code.get(rc)));
            }
        }
    };
    
    void deleteAssignment(String path){
        zkc.zk.delete(
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
    
    class RecreateTaskCtx {
        String path; 
        String task;
        byte[] data;
        XATransactionsBuilder.WorkerScheduleConfiguration wsc;
        
        RecreateTaskCtx(String path, String task, byte[] data, XATransactionsBuilder.WorkerScheduleConfiguration wsc) {
            this.path = path;
            this.task = task;
            this.data = data;
            this.wsc = wsc;
        }
    }
    
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
                
                zkc.zk.create(
                    XATransactionClient.ClientZnodes.TRANSACTIONS_NAMESPACES.getPath(clientId, distributedTransactionConf) + "/" + ctx.task,
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
            int order = orderWorkerScheduleD.get(ctx.wsc);
            int beforeWorkerSchedule = order - 1;
            XATransactionsBuilder.WorkerScheduleConfiguration beforeWsc = workerScheduleOrder.get(beforeWorkerSchedule);
            
            zkc.zk.create(
                XATransactionResource.WorkerZnodes.STATUS_NAMESPACE.getPath(beforeWsc, distributedTransactionConf) + "/" + ctx.task,
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
    
    
    
    /* ********************************************
     * ********************************************
     * Administracion y asignacion de transacciones
     * ********************************************
     * ********************************************
     */
    
    // Cache con las transacciones disponibles
    Dictionary<String, ChildrenCache> transactionsCache;
    
    /*
     * Obtener las transacciones pendientes
     */ 
    void getTransactions(List<String> clientes){
        for(String clientId: clientes){
            // Nuevas transacciones para ser procesadas, antes de iniciar en el schedule
            getClientTransactions(clientId);
        }
    }
    
    protected void getClientTransactions(String clientId){
        zkc.zk.getChildren(
            XATransactionClient.ClientZnodes.TRANSACTIONS_NAMESPACES.getPath(clientId, distributedTransactionConf),
            //TransactionZnodes.TRANSACTIONS_NAMESPACES.getPath(distributedTransactionConf),
            transactionsChangeWatcher,
            transactionsGetChildrenCallback,
            clientId);
    }
    
    Watcher transactionsChangeWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            // Hubo un cambio en los hijos del namespace de las transacciones
            // osease que cambio la lista mijin
            if(event.getType() == Event.EventType.NodeChildrenChanged){
                String clientId = event.getPath().substring(event.getPath().lastIndexOf("/") + 1);
                log.debug("Watcher client transactions, nuevo watcher clientId: " + clientId);
                
                // Obtener la nueva lista de transacciones
                getClientTransactions(clientId);
            }
        }
    };
    
    AsyncCallback.ChildrenCallback transactionsGetChildrenCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx /* clientId */, List<String> children) {
            String clientId = (String) ctx;
            
            switch(KeeperException.Code.get(rc)){
            case CONNECTIONLOSS:
                //String clientId = path.substring(path.lastIndexOf("/") + 1);
                getClientTransactions(clientId);
                break;
            case OK:
                log.info("[" + masterId + "] LISTA DE TRANSACCIONES OBTENIDA EN [" + path + "]: " + children.size());
                /*for (String child : children) 
                    log.debug("[" + masterId + "] TASK: " + child);*/
                
                // Hubo un cambio en las transacciones disponibles, las transacciones deben
                // reasignarse acorde
                
                // Obtener las nuevas transacciones agregadas al sistema
                List<String> toProcess;
                if(transactionsCache.get(clientId) == null){
                    transactionsCache.put(clientId, new ChildrenCache(children));
                    //transactionsCache = new ChildrenCache(children);
                    
                    toProcess = children;
                } else {
                    toProcess = transactionsCache.get(clientId).addedAndSet(children);
                }
                
                // Asignar las nuevas transacciones para q sean ejecutadas
                // debe iniciar con los que tengan una posicion en el schedule = 0
                if(toProcess != null){
                    assignTransactions((String) ctx, toProcess);
                }
                
                break;
            case NONODE:
                // Puede no crearse aun el path
                // reintentar la lectura
                getClientTransactions((String) ctx);
                break;
            default:
                log.error("[" + masterId + "] ERROR AL OBTENER LISTA DE TAREAS: " + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    
    /*
     * Asignar lista de transacciones para ser ejecutadas por el primer WorkerSchedule
     */
    void assignTransactions(String clientId, List<String> transactions){
        for(String transaction : transactions){
            getTransactionData(clientId, transaction);
        }
    }
    
    /*
     * Obtener datos de la transaccion a ser asignada
     */
    void getTransactionData(String clientId, String transaction){
        
        zkc.zk.getData(
                XATransactionClient.ClientZnodes.TRANSACTIONS_NAMESPACES.getPath(clientId, distributedTransactionConf)+ "/" + transaction,
                false,
                transactionDataCallback,
                clientId);
    }
    
    /*
     * Al obtener los datos de la transaccion a ser asignada, se elige un worker
     * aleatoriamente para asignarla y se hace un llamado al metodo
     * para crear dicha asignacion
     */
    AsyncCallback.DataCallback transactionDataCallback = new AsyncCallback.DataCallback() {

        @Override
        public void processResult(int rc, String path, Object ctx /* clientId */, byte[] data, Stat stat) {
            String transaction = path.substring(path.lastIndexOf("/") + 1);
            
            switch(KeeperException.Code.get(rc)){
            case CONNECTIONLOSS:
                
                getTransactionData((String) ctx, transaction);
                break;
            case OK:
                try {
                    // Agregar metadatos de asignacion a los datos
                    data = addAssignMetadata(data, (String) ctx /* clientId */);
                    
                    // Agregar nodo de status a los datos
                    data = addTransactionStatus(data);
                    log.debug("ASIGNACION DATA METADATOS/STATUS: " + new String(data) + ", PATH: " + path);
                } catch (ParseException ex) {
                    log.error("Error al agregar Metadatos/Status al data de transaccion: " + ex.getMessage());
                }
                
                // Debe haber almenos 1 worker del cual elegir
                // La transaccion inicia con worker en posicion = 0 en el schedule
                XATransactionsBuilder.WorkerScheduleConfiguration wsc = workerScheduleOrder.get(0);
                if(workersCache.get(wsc).getList() != null && workersCache.get(wsc).getList().size() > 0){
                    // Elegir un worker randomicamente
                    List<String> list = workersCache.get(wsc).getList();
                    String designatedWorker = list.get(random.nextInt(list.size()));

                    // Path del znode para asignar la tarea al worker elegido
                    String assignmentPath = XATransactionResource.WorkerZnodes.ASSIGN_NAMESPACE.getPath(wsc, distributedTransactionConf)+ 
                            "/" +
                            designatedWorker + 
                            "/" + 
                            transaction;
                    
                    // Path del znode para crear nodo de rollback
                    String rollbackPath = XATransactionResource.WorkerZnodes.ROLLBACK_NAMESPACE.getPath(wsc, distributedTransactionConf)+ 
                            "/" +
                            transaction;

                    log.info("[" + masterId + "] Asignando tarea  [" + transaction + "], path: " + assignmentPath);
                    log.info("[" + masterId + "] Creando rollback [" + transaction + "], path: " + rollbackPath);
                    TransactionAssignmentCtx taCtx = new TransactionAssignmentCtx(wsc, null, designatedWorker, transaction, (String) ctx, data);
                    createTransactionAssignment(assignmentPath, rollbackPath, taCtx);
                }else{
                    // Si no hay workers detectados aun, volver a intentar asignar la tarea
                    getTransactionData((String) ctx, transaction);
                }
                break;
            default:
                log.error("[" + masterId + "] Error al obtener datos de la tarea: ", 
                        KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    
    class TransactionAssignmentCtx{
        public XATransactionsBuilder.WorkerScheduleConfiguration wsc;
        public XATransactionsBuilder.WorkerScheduleConfiguration nextWsc;
        public String designatedWorker;
        public String transaction;
        public String clientId;
        public byte[] data;

        public TransactionAssignmentCtx(XATransactionsBuilder.WorkerScheduleConfiguration wsc, XATransactionsBuilder.WorkerScheduleConfiguration nextWsc, String designatedWorker, String transaction, String clientId, byte[] data) {
            this.wsc = wsc;
            this.nextWsc = nextWsc;
            this.designatedWorker = designatedWorker;
            this.transaction = transaction;
            this.clientId = clientId;
            this.data = data;
        }
    }
    
    /*
     * Agrega metadatos de transaccion al data de la transaccion
     */
    protected byte[] addAssignMetadata(byte[] data, String clientId) throws ParseException{
        JSONParser parser=new JSONParser();

        String dataString = new String(data);
        Object dataObject = parser.parse(dataString);
        JSONObject dataJson = (JSONObject) dataObject;
        
        JSONObject metaJsonChild = new JSONObject();
        metaJsonChild.put(XATransactionUtils.AssignMetadataNodes.CLIENT_ID_CHILD.getNode(), clientId);
        
        dataJson.put(XATransactionUtils.AssignMetadataNodes.XA_ASSIGN_METADATA_NODE.getNode(), metaJsonChild);
        
        return dataJson.toJSONString().getBytes();
    }
    
    /*
     * Agrega Status de transaccion sin valores al data de la transaccion
     */
    protected byte[] addTransactionStatus(byte[] data) throws ParseException{
        JSONParser parser=new JSONParser();
        
        String dataString = new String(data);
        Object dataObject = parser.parse(dataString);
        JSONObject dataJson = (JSONObject) dataObject;
        
        JSONObject statusJsonChild = new JSONObject();
        statusJsonChild.put(XATransactionUtils.TransactionStatusNodes.STATUS_CHILD.getNode(), "");
        statusJsonChild.put(XATransactionUtils.TransactionStatusNodes.MESSAGE_CHILD.getNode(), "");
        
        dataJson.put(XATransactionUtils.TransactionStatusNodes.XA_STATUS_NODE.getNode(), statusJsonChild);
        
        return dataJson.toJSONString().getBytes();
    }
    
    /*
     * Creacion de la asignacion
     */
    void createTransactionAssignment(String assignmentPath, String rollbackPath, TransactionAssignmentCtx taCtx){
        Transaction transaction = zkc.zk.transaction();
        
        // Creacion de asignacion de transaccion
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
        
        transaction.commit(assignTransactionCallback, taCtx);
    }
    
    AsyncCallback.MultiCallback assignTransactionCallback = new AsyncCallback.MultiCallback() {

        @Override
        public void processResult(int rc, String path, Object ctx, List<OpResult> opResults) {
            TransactionAssignmentCtx traCtx = (TransactionAssignmentCtx) ctx;
            
            switch(KeeperException.Code.get(rc)) { 
            case CONNECTIONLOSS:
                // Path del znode para asignar la tarea al worker elegido
                    String assignmentPath = XATransactionResource.WorkerZnodes.ASSIGN_NAMESPACE.getPath(traCtx.wsc, distributedTransactionConf)+ 
                            "/" +
                            traCtx.designatedWorker + 
                            "/" + 
                            traCtx.transaction;
                    
                    // Path del znode para crear nodo de rollback
                    String rollbackPath = XATransactionResource.WorkerZnodes.ROLLBACK_NAMESPACE.getPath(traCtx.wsc, distributedTransactionConf)+ 
                            "/" +
                            traCtx.transaction;
                createTransactionAssignment(assignmentPath, rollbackPath, traCtx);
                
                break;
            case OK:
                log.info("[" + masterId + "] Transaccion/Tarea, nodo rollback asignada correctamente.");
                
                // Una vez asignada la transaccion la eliminamos del TRANSACTION_NAMESPACE
                deleteTransaction(traCtx);
                
                break;
            case NODEEXISTS: 
                log.warn("[" + masterId + "] Transaccion/Tarea, nodo rollback ya asignada previamente.");
                
                break;
            default:
                log.error("[" + masterId + "] Error al asignar Transaccion/Tarea y crear nodo rollback: ", 
                        KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    
    void deleteTransaction(TransactionAssignmentCtx taCtx){
        zkc.zk.delete(
                XATransactionClient.ClientZnodes.TRANSACTIONS_NAMESPACES.getPath(taCtx.clientId, distributedTransactionConf)+ "/" + taCtx.transaction,
                -1,
                transactionDeleteCallback,
                taCtx);
    }
    
    AsyncCallback.VoidCallback transactionDeleteCallback = new AsyncCallback.VoidCallback() {

        @Override
        public void processResult(int rc, String path, Object ctx /* TransactionAssignmentCtx */) {
            TransactionAssignmentCtx taCtx = (TransactionAssignmentCtx) ctx;
            
            switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                deleteTransaction(taCtx);
                
                break;
            case OK:
                log.info("[" + masterId + "] Transaccion eliminada exitosamente: " + path);
                
                break;
            case NONODE:
                log.info("[" + masterId + "] Transaccion no existe o eliminada anteriormente: " + path);
                
                break;
            default:
                log.error("[" + masterId + "] Error al eliminar Transaccion: " + 
                        KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    
    /* *******************************************************
     * *******************************************************
     * Administracion y asignacion de tareas a WorkersSchedule
     * *******************************************************
     * *******************************************************
     */
    
    // Cache con las tareas disponibles por cada WorkerSchedule
    Dictionary<XATransactionsBuilder.WorkerScheduleConfiguration, ChildrenCache> tasksCache;
        
    /*
     * Obtener las tareas terminadas por workerSchedule
     */ 
    void getTasksDone(){
        for(XATransactionsBuilder.WorkerScheduleConfiguration wsc: workerScheduleOrder){
            getWorkerTasksDone(wsc);
        }
    }
    
    /*
     * Por cada WorkerSchedule obtenemos las tareas ya terminadas
     */
    void getWorkerTasksDone(XATransactionsBuilder.WorkerScheduleConfiguration wsc){
        // Nuevas tareas para ser procesadas por el siguiente workerSchedule
        zkc.zk.getChildren(
                XATransactionResource.WorkerZnodes.STATUS_NAMESPACE.getPath(wsc, distributedTransactionConf),
                workerTasksDoneChangeWatcher,
                workerTasksDoneGetChildrenCallback,
                wsc);
    }
    
    Watcher workerTasksDoneChangeWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            // Hubo un cambio en los hijos del namespace de las transacciones
            // osease que cambio la lista mijin
            if(event.getType() == Event.EventType.NodeChildrenChanged){
                log.debug("WATCHER WORKER TASK DONE CHILDREN: " + event.getPath());
                // Obtener la nueva lista de transacciones
                getWorkerTasksDone(statusNamespaceWorkersScheduleD.get(event.getPath()));
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
                log.info("[" + masterId + "] LISTA DE TAREAS TERMINADAS OBTENIDA: " + children.size() + ", PATH: " + path);
                
                // Hubo un cambio en las tareas terminadas, las tareas deben
                // reasignarse al siguiente WorkerSchedule acorde
                
                // Obtener las nuevas tareas terminadas
                List<String> toProcess;
                if(tasksCache.get(wsc) == null){
                    tasksCache.put(
                            wsc, 
                            new ChildrenCache(children));
                    
                    toProcess = children;
                } else {
                    toProcess = tasksCache.get(wsc).addedAndSet(children);
                }
                
                // Asignar las nuevas transacciones para q sean ejecutadas
                // debe iniciar con los que tengan una posicion en el schedule = 0
                // Si es el ultimo worker en el schedule, no se asigna y mas bien se envia el resultado al cliente
                if(toProcess != null && !wsc.isLast()){
                    // Determinar el siguiente WorkerSchedule al que se le asigna 
                    Integer currentScheduleIndex = orderWorkerScheduleD.get(wsc);
                    
                    if(workerScheduleOrder.size() - 1 > currentScheduleIndex){// Hay un siguiente Worker en el Schedule
                        assignTasks(toProcess, currentScheduleIndex);
                    }
                    
                    for (String child : toProcess) 
                        log.debug("[" + masterId + "] A ASIGNAR AL SIGUIENTE SCHEDULE: " + child);
                }else if(toProcess != null && wsc.isLast()){
                    // El resultado es del ultimo worker en el schedule,
                    // enviando resultado a cliente
                    log.info("[" + masterId + "] ES ULTIMO WORKER EN EL SCHEDULE, DEBE NOTIFICAR A CADA CLIENTE RESULTADO");
                    sendResults(toProcess, wsc);
                    
                }
                
                break;
            default:
                log.error("[" + masterId + "] ERROR AL OBTENER LISTA DE TAREAS: " + KeeperException.create(KeeperException.Code.get(rc), path));
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
        TaskAssignmentCtx taCtx = new TaskAssignmentCtx(wsc, null, null, task, null);
        
        zkc.zk.getData(
                XATransactionResource.WorkerZnodes.STATUS_NAMESPACE.getPath(taCtx.wsc, distributedTransactionConf)+ "/" + taCtx.task,
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
                log.warn("[" + masterId + "] Conexion perdida al obtener data de resultado final: " + path);
                
                getResultData(taCtx.task, taCtx.wsc);
                
                break;
            case OK:
                log.info("[" + masterId + "] Data de resultado final obtenido, enviando a cliente: " + path);
                taCtx.data = data;
                
                XATransactionJSONInterpreter xatJsonInterpreter;
                try {
                    xatJsonInterpreter = new XATransactionJSONInterpreter(data);
                    JSONObject metadata = xatJsonInterpreter.getMetadata();
                    byte [] result = xatJsonInterpreter.getData();
                    
                    // Si hubo un error en la ejecucion de la transaccion en este punto
                    // se debe hacer rollback a la transaccion
                    if(xatJsonInterpreter.isError()){
                        log.debug("[" + masterId + "] El worker respondio con error " + path);
                        
                    }else {// Si la ejecucion por parte del worker es exitosa, enviar resultado al cliente
                        String clientId = metadata.get(XATransactionUtils.AssignMetadataNodes.CLIENT_ID_CHILD.getNode()).toString();
                                
                        // Crea un znode para notificar resultado de la transaccion al cliente
                        zkc.zk.create(
                            XATransactionClient.ClientZnodes.RESULTS_NAMESPACES.getPath(clientId, distributedTransactionConf) + "/" + taCtx.task, 
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
                log.error("[" + masterId + "] Error al obtener data de resultado final: ", 
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
                zkc.zk.create(
                        path, 
                        taCtx.data, 
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                        CreateMode.PERSISTENT,
                        resultCreateCallback, 
                        ctx /* TaskAssignmentCtx */);
                break;
            case OK:
                log.info("[" + masterId + "] Resultado de transaccion creado correctamente: " + name);
                
                //deleteStatus(taCtx);
                clearTransactionData(taCtx);
                break;
            case NODEEXISTS:
                log.warn("[" + masterId + "] Resultado de transaccion ya existe o creado previamente: " + path);
                break;
            default:
                log.error("[" + masterId + "] Error al crear resultado de transaccion: ", KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    
    /*
     * Eliminar el status del ultimo worker del schedule procesado exitosamente
     * tambien eliminar los znode creados para rollback ya q no son necesarios mas
     */
    void clearTransactionData(TaskAssignmentCtx taCtx){
        Transaction transaction = zkc.zk.transaction();
        
        // Eliminar status creado por el ultimo worker schedule
        transaction.delete(
                XATransactionResource.WorkerZnodes.STATUS_NAMESPACE.getPath(taCtx.wsc, distributedTransactionConf) + "/" + taCtx.task,
                -1);
        
        // Eliminar znodes creados para rollback
        for(XATransactionsBuilder.WorkerScheduleConfiguration wsc : distributedTransactionConf.getWorkersScheduleConfigurations()){
            String rollbackPath = XATransactionResource.WorkerZnodes.ROLLBACK_NAMESPACE.getPath(wsc, distributedTransactionConf) + "/" + taCtx.task;
            
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
                log.warn("[" + masterId + "] Conexion perdida al hacer clear de la transaccion.");
                clearTransactionData(taCtx);
                
                break;
            case OK:
                log.info("[" + masterId + "] Clear de la transaccion realizado correctamente.");
                
                break;
            case NONODE: 
                log.warn("[" + masterId + "] Transaccion ya hecha clear previamente ");
                
                break;
            default:
                log.error("[" + masterId + "] Error al hacer clear de la transaccion ", 
                        KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    
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
        XATransactionsBuilder.WorkerScheduleConfiguration wsc = workerScheduleOrder.get(workerScheduleIndex);
        
        zkc.zk.getData(
                XATransactionResource.WorkerZnodes.STATUS_NAMESPACE.getPath(wsc, distributedTransactionConf)+ "/" + task,
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
            XATransactionsBuilder.WorkerScheduleConfiguration wsc = workerScheduleOrder.get((int) ctx);
            String namespace = XATransactionResource.WorkerZnodes.STATUS_NAMESPACE.getPath(wsc, distributedTransactionConf);
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
                    if(xatJsonInterpreter.isError())
                        log.debug("[" + masterId + "] El worker respondio con error " + path);
                    else {// Si la ejecucion por parte del worker es exitosa, continuar con el siguiente worker en el schedule
                        // Debe haber almenos 1 worker del cual elegir en el siguiente worker schedule
                        // La transaccion inicia con los workers que tengan una posicion en el schedule = 0
                        int nextWorkerScheduleIndex = ((int) ctx) + 1;
                        XATransactionsBuilder.WorkerScheduleConfiguration nextWsc = workerScheduleOrder.get(nextWorkerScheduleIndex);

                        if(workersCache.get(nextWsc).getList() != null && workersCache.get(nextWsc).getList().size() > 0){
                            // Elegir un worker randomicamente
                            List<String> list = workersCache.get(nextWsc).getList();
                            String designatedWorker = list.get(random.nextInt(list.size()));

                            // Path del znode para asignar la tarea al worker elegido
                            String assignmentPath = XATransactionResource.WorkerZnodes.ASSIGN_NAMESPACE.getPath(nextWsc, distributedTransactionConf)+ 
                                    "/" +
                                    designatedWorker + 
                                    "/" + 
                                    task;

                            // Path del znode para crear nodo de rollback
                            String rollbackPath = XATransactionResource.WorkerZnodes.ROLLBACK_NAMESPACE.getPath(nextWsc, distributedTransactionConf)+ 
                                    "/" +
                                    task;

                            log.info("[" + masterId + "] Asignando tarea  [" + task + "], path: " + assignmentPath);
                            log.info("[" + masterId + "] Creando rollback [" + task + "], path: " + rollbackPath);

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
                log.error("[" + masterId + "] Error al obtener datos de la tarea: ", 
                        KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    
    /*
     * Creacion de la asignacion
     */
    void createAssignment(String assignmentPath, String rollbackPath, TaskAssignmentCtx taCtx){
        Transaction transaction = zkc.zk.transaction();
        
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
                log.warn("[" + masterId + "] Conexion perdida al crear Transaccion/Tarea y nodo rollback, intentando crear nuevamente");
                // Path del znode para asignar la tarea al worker elegido
                    String assignmentPath = XATransactionResource.WorkerZnodes.ASSIGN_NAMESPACE.getPath(taCtx.nextWsc, distributedTransactionConf)+ 
                            "/" +
                            taCtx.designatedWorker + 
                            "/" + 
                            taCtx.task;

                    // Path del znode para crear nodo de rollback
                    String rollbackPath = XATransactionResource.WorkerZnodes.ROLLBACK_NAMESPACE.getPath(taCtx.nextWsc, distributedTransactionConf)+ 
                            "/" +
                            taCtx.task;
                createAssignment(assignmentPath, rollbackPath, taCtx);
                
                break;
            case OK:
                log.info("[" + masterId + "] Transaccion/Tarea y nodo rollback asignada correctamente: " + path);
                
                deleteStatus(taCtx);
                
                break;
            case NODEEXISTS: 
                log.warn("[" + masterId + "] Transaccion/Tarea y nodo rollback ya asignada previamente: " + path);
                
                break;
            default:
                log.error("[" + masterId + "] Error al asignar Transaccion/Tarea: ", 
                        KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    
    /*
     * Eliminar el status del procesamiento de la tarea del worker una vez que ya
     * a sido asignado al siguiente WorkerSchedule o Enviado como resultado al cliente
     */
    void deleteStatus(TaskAssignmentCtx taCtx){
        zkc.zk.delete(
                XATransactionResource.WorkerZnodes.STATUS_NAMESPACE.getPath(taCtx.wsc, distributedTransactionConf) + "/" + taCtx.task,
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
                log.info("[" + masterId + "] Status eliminado exitosamente: " + path);
                
                break;
            case NONODE:
                log.info("[" + masterId + "] Status no existe o eliminada anteriormente: " + path);
                
                break;
            default:
                log.error("[" + masterId + "] Error al eliminar Status: " + 
                        KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    
    class TaskAssignmentCtx{
        XATransactionsBuilder.WorkerScheduleConfiguration wsc;
        XATransactionsBuilder.WorkerScheduleConfiguration nextWsc;
        public String task;
        public byte[] data;
        String designatedWorker;

        public TaskAssignmentCtx(XATransactionsBuilder.WorkerScheduleConfiguration wsc, XATransactionsBuilder.WorkerScheduleConfiguration nextWsc, String designatedWorker, String task, byte[] data) {
            this.wsc = wsc;
            this.nextWsc = nextWsc;
            this.task = task;
            this.data = data;
            this.designatedWorker = designatedWorker;
        }
    }
    
    /* *******************************************************
     * *******************************************************
     * Monitoreo de namespaces de tareas por cada cliente
     * *******************************************************
     * *******************************************************
     */
    
    // Cache con los namespaces por cliente
    ChildrenCache clientsCache;
        
    /*
     * Obtener los namespaces por cliente en esta transaccion
     */ 
    void getClients(){
        zkc.zk.getChildren(
                TransactionZnodes.TRANSACTION_CLIENTS_NAMESPACE.getPath(distributedTransactionConf),
                clientsChangeWatcher,
                clientsChildrenCallback,
                null);
    }
    
    Watcher clientsChangeWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            // Hubo un cambio en los hijos del namespace de las transacciones
            // osease que cambio la lista mijin
            if(event.getType() == Event.EventType.NodeChildrenChanged){
                log.debug("WATCHER LISTA DE CLIENTES CAMBIADO: " + event.getPath());
                // Obtener la nueva lista de namespaces por cliente
                getClients();
            }
        }
    };
    
    AsyncCallback.ChildrenCallback clientsChildrenCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch(KeeperException.Code.get(rc)){
            case CONNECTIONLOSS:
                getClients();
                break;
            case OK:
                log.info("[" + masterId + "] LISTA CLIENTES OBTENIDA: " + children.size() + " en: " + path);
                for (String child : children) 
                    log.debug("[" + masterId + "] CLIENTE: " + child);
                
                // Hubo un cambio en la lista de clientes
                // Obtener los nuevos clientes
                List<String> toProcess;
                if(clientsCache == null){
                    clientsCache = new ChildrenCache(children);
                    
                    toProcess = children;
                } else {
                    toProcess = clientsCache.addedAndSet(children);
                }
                
                // Escuchar transacciones agregadas por cada cliente
                if(toProcess != null){
                    getTransactions(toProcess);
                    for(String child: toProcess)
                        log.debug("[" + masterId + "] ESCUCHAR TRANSACCIONES PARA EL CLIENTE: " + child);
                }
                
                break;
            default:
                log.error("[" + masterId + "] ERROR AL OBTENER LISTA DE CLIENTES: " + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    
    
    
    
    
    
    
    
    
    
    class TransactionMasterBootstrap {
        XATransactionsBuilder.DistributedTransactionConfiguration dtc;

        public TransactionMasterBootstrap(XATransactionsBuilder.DistributedTransactionConfiguration dtc) {
            this.dtc = dtc;
        }
        
        public void execute(){
            try{
                zkc.zk.create(
                    TransactionZnodes.ASSIGNS_NAMESPACE.getPath(dtc), 
                    "Root namespace".getBytes(), 
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                    CreateMode.PERSISTENT);
            }catch (KeeperException | InterruptedException ex){}

            try{
                zkc.zk.create(
                    TransactionZnodes.STATUS_NAMESPACE.getPath(dtc), 
                    "Root namespace".getBytes(), 
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                    CreateMode.PERSISTENT);
            }catch (KeeperException | InterruptedException ex){}
            
            try{
                zkc.zk.create(
                    TransactionZnodes.RESULTS_NAMESPACE.getPath(dtc), 
                    "Root namespace".getBytes(), 
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                    CreateMode.PERSISTENT);
            }catch (KeeperException | InterruptedException ex){}

            try{
                zkc.zk.create(
                    TransactionZnodes.TRANSACTIONS_NAMESPACE.getPath(dtc), 
                    "Root namespace".getBytes(), 
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                    CreateMode.PERSISTENT);
            }catch (KeeperException | InterruptedException ex){}

            try{
                zkc.zk.create(
                    TransactionZnodes.WORKERS_NAMESPACE.getPath(dtc), 
                    "Root namespace".getBytes(), 
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                    CreateMode.PERSISTENT);
            }catch (KeeperException | InterruptedException ex){}
            
            try{
                zkc.zk.create(
                    TransactionZnodes.TRANSACTION_CLIENTS_NAMESPACE.getPath(dtc), 
                    "Root namespace".getBytes(), 
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                    CreateMode.PERSISTENT);
            }catch (KeeperException | InterruptedException ex){}
            
            try{
                zkc.zk.create(
                    TransactionZnodes.ROLLBACK_NAMESPACE.getPath(dtc), 
                    "Root namespace".getBytes(), 
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                    CreateMode.PERSISTENT);
            }catch (KeeperException | InterruptedException ex){}
        }
    }
}