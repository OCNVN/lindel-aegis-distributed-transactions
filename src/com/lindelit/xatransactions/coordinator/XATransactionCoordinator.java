/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lindelit.xatransactions.coordinator;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.lindelit.xatransactions.Bootstrap.ApplicationZnodes;
import com.lindelit.xatransactions.ChildrenCache;
import com.lindelit.xatransactions.XATransactionClient;
import com.lindelit.xatransactions.XATransactionJSONInterpreter;
import com.lindelit.xatransactions.XATransactionResource;
import com.lindelit.xatransactions.XATransactionUtils;
import com.lindelit.xatransactions.XATransactionsBuilder;
import com.lindelit.xatransactions.ZKConexion;
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
    
    // Subcoordinador de tareas
    XATaskSubcoordinator xaTaskSubcoordinator;
    // Subcoordinador de workers
    XAWorkerSubcoordinator xaWorkerSubcoordinator;
    // Subcoordinador de assignaciones
    XAAssignSubcoordinator xaAssignSubcoordinator;
    // Subcoordinador de rollbacks
    XARollbackSubcoordinator xaRollbackSubcoordinator;
    
    // Configuracion de la transaccion distribuida
    private XATransactionsBuilder.DistributedTransactionConfiguration distributedTransactionConf;
    
    // Id del master
    private String masterId;
    
    // WorkersCache por cada WorkerSchedule
    private Dictionary<XATransactionsBuilder.WorkerScheduleConfiguration, ChildrenCache>  workersCache;
    // WorkerScheduleConfiguration ordenado segun el orden establecido para la transaccion
    private ArrayList<XATransactionsBuilder.WorkerScheduleConfiguration>  workerScheduleOrder;
    // WorkersCache por cada Namespace
    private Dictionary<String, XATransactionsBuilder.WorkerScheduleConfiguration>  statusNamespaceWorkersScheduleD;
    // Orden de ejecucion por objeto WorkerScheduleConfiguration 
    private Dictionary<XATransactionsBuilder.WorkerScheduleConfiguration, Integer>  orderWorkerScheduleD;
    
    // Cache con las tareas disponibles por cada WorkerSchedule
    private Dictionary<XATransactionsBuilder.WorkerScheduleConfiguration, ChildrenCache> tasksCache;
    
    // Cache con las rollbacks pendientes a ser gestionados
    private ChildrenCache rollbacksCache;
    
    // Posibles estados en los que puede estar el master
    enum TransaccionMasterStates {RUNNING, ELECTED, NOTELECTED};
    
    private TransaccionMasterStates state = TransaccionMasterStates.RUNNING;
    
    // Conexion a zookeeper
    ZKConexion zkc;
    
    // Generar numeros aleatorios
    protected Random random = new Random();

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
        
        FAILED_NAMESPACE {
            public String getPath(XATransactionsBuilder.DistributedTransactionConfiguration dt){
                String path = ApplicationZnodes.FAILED_NAMESPACE.getPath() + 
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
        
        // Iniciacion de sub coordinadores
        xaTaskSubcoordinator = new XATaskSubcoordinator(this);
        xaAssignSubcoordinator = new XAAssignSubcoordinator(this);
        xaWorkerSubcoordinator = new XAWorkerSubcoordinator(this);
        xaRollbackSubcoordinator = new XARollbackSubcoordinator(this);
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
        xaWorkerSubcoordinator.getWorkers();
        
        // Administracion y coordinacion de clientes
        getClients();
        
        // Administracion y coordinacion de transacciones
        //getTransactions();
        
        // Administracion y coordinacion de tareas en WorkerSchedule
        xaTaskSubcoordinator.getTasksDone();
        
        // Administracion y coordinacion de rollbacks
        xaRollbackSubcoordinator.getPendingRollbacks();
    }
    
    
    
    
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
                    
                    // Agregar fase de la transaccion
                    data = addTransactionPhase(data);
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
        statusJsonChild.put(XATransactionUtils.TransactionStatusNodes.STATUS_CHILD.getNode(), XATransactionUtils.TransactionStatusNodes.STATUS_START_VALUE_NODE.getNode());
        statusJsonChild.put(XATransactionUtils.TransactionStatusNodes.MESSAGE_CHILD.getNode(), "");
        
        dataJson.put(XATransactionUtils.TransactionStatusNodes.XA_STATUS_NODE.getNode(), statusJsonChild);
        
        return dataJson.toJSONString().getBytes();
    }
    
    /*
     * Agrega Fase de ejecucion por default estamos en la fase de execution
     */
    protected byte[] addTransactionPhase(byte[] data) throws ParseException{
        JSONParser parser=new JSONParser();
        
        String dataString = new String(data);
        Object dataObject = parser.parse(dataString);
        JSONObject dataJson = (JSONObject) dataObject;
        
        JSONObject phaseJsonChild = new JSONObject();
        phaseJsonChild.put(XATransactionUtils.TransactionPhaseNodes.PHASE_CHLD.getNode(), XATransactionUtils.TransactionPhaseNodes.PHASE_EXECUTION_VALUE_NODE.getNode());
        
        dataJson.put(XATransactionUtils.TransactionPhaseNodes.XA_PHASE_NODE.getNode(), phaseJsonChild);
        
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
                    "ASSIGNS namespace".getBytes(), 
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                    CreateMode.PERSISTENT);
            }catch (KeeperException | InterruptedException ex){}

            try{
                zkc.zk.create(
                    TransactionZnodes.STATUS_NAMESPACE.getPath(dtc), 
                    "STATUS namespace".getBytes(), 
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                    CreateMode.PERSISTENT);
            }catch (KeeperException | InterruptedException ex){}
            
            try{
                zkc.zk.create(
                    TransactionZnodes.RESULTS_NAMESPACE.getPath(dtc), 
                    "RESULTS namespace".getBytes(), 
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                    CreateMode.PERSISTENT);
            }catch (KeeperException | InterruptedException ex){}

            try{
                zkc.zk.create(
                    TransactionZnodes.TRANSACTIONS_NAMESPACE.getPath(dtc), 
                    "TRANSACTIONS namespace".getBytes(), 
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
                    "TRANSACTION CLIENTS namespace".getBytes(), 
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                    CreateMode.PERSISTENT);
            }catch (KeeperException | InterruptedException ex){}
            
            try{
                zkc.zk.create(
                    TransactionZnodes.FAILED_NAMESPACE.getPath(dtc), 
                    "FAILED namespace".getBytes(), 
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                    CreateMode.PERSISTENT);
            }catch (KeeperException | InterruptedException ex){}
            
            try{
                zkc.zk.create(
                    TransactionZnodes.ROLLBACK_NAMESPACE.getPath(dtc), 
                    "ROLLBACK namespace".getBytes(), 
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                    CreateMode.PERSISTENT);
            }catch (KeeperException | InterruptedException ex){}
            
            
        }
    }
    
    /*
     * Getters & Setters
     */

    /*
     * Getters & Setters
     */
    public String getMasterId() {
        return masterId;
    }

    public XATransactionsBuilder.DistributedTransactionConfiguration getDistributedTransactionConf() {
        return distributedTransactionConf;
    }

    public ArrayList<XATransactionsBuilder.WorkerScheduleConfiguration> getWorkerScheduleOrder() {
        return workerScheduleOrder;
    }

    public Dictionary<String, XATransactionsBuilder.WorkerScheduleConfiguration> getStatusNamespaceWorkersScheduleD() {
        return statusNamespaceWorkersScheduleD;
    }

    public Dictionary<XATransactionsBuilder.WorkerScheduleConfiguration, ChildrenCache> getTasksCache() {
        return tasksCache;
    }

    public Dictionary<XATransactionsBuilder.WorkerScheduleConfiguration, Integer> getOrderWorkerScheduleD() {
        return orderWorkerScheduleD;
    }

    public Dictionary<XATransactionsBuilder.WorkerScheduleConfiguration, ChildrenCache> getWorkersCache() {
        return workersCache;
    }

    public ChildrenCache getRollbacksCache() {
        return rollbacksCache;
    }

    public void setRollbacksCache(ChildrenCache rollbacksCache) {
        this.rollbacksCache = rollbacksCache;
    }
    
}