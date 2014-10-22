/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lindelit.xatransactions;

import org.apache.log4j.Logger;
import com.lindelit.xatransactions.coordinator.XATransactionCoordinator.TransactionZnodes;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import static org.apache.zookeeper.KeeperException.Code.CONNECTIONLOSS;
import static org.apache.zookeeper.KeeperException.Code.NODEEXISTS;
import static org.apache.zookeeper.KeeperException.Code.OK;
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
public class XATransactionResource implements Watcher{
    private final static Logger log = Logger.getLogger(XATransactionResource.class);
    // Identificador de proceso
    private String workerId;
    
    // Configuracion del worker
    XATransactionsBuilder.WorkerScheduleConfiguration workerScheduleConf;
    
    // Configuracion del proceso master a cargo de coordinar este worker
    XATransactionsBuilder.DistributedTransactionConfiguration distributedTransactionConfiguration;
    
    // Conexion a Zookeeper
    ZKConexion zkc;
    
    /*
     * Para no bloquear el thread callback del cliente Zookeeper, usamos 
     * un thread pool executor para paralelizar la computacion del callback
     */
    private ThreadPoolExecutor executor;

    public void init() {
        zkc = new ZKConexion();
        try {
            zkc.connect(this);
            
            // Iniciar recursos necesarios en zookeeper
            TransactionWorkerBootstrap twb = new TransactionWorkerBootstrap(workerScheduleConf, distributedTransactionConfiguration);
            twb.execute();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent event) {
    }
    
    // Znodes necesarios para el funcionamiento del worker
    public enum WorkerZnodes {
        WORKER_NAMESPACE {
            public String getPath(XATransactionsBuilder.WorkerScheduleConfiguration ws, XATransactionsBuilder.DistributedTransactionConfiguration dtc){
                String path = TransactionZnodes.WORKERS_NAMESPACE.getPath(dtc) + 
                    "/" +
                    ws.getName();
                
                return path;
            }
        },
        
        ASSIGN_NAMESPACE {
            public String getPath(XATransactionsBuilder.WorkerScheduleConfiguration ws, XATransactionsBuilder.DistributedTransactionConfiguration dtc){
                String path = TransactionZnodes.ASSIGNS_NAMESPACE.getPath(dtc) + 
                    "/" +
                    ws.getName();
                
                return path;
            }
        },
        
        STATUS_NAMESPACE {
            public String getPath(XATransactionsBuilder.WorkerScheduleConfiguration ws, XATransactionsBuilder.DistributedTransactionConfiguration dtc){
                String path = TransactionZnodes.STATUS_NAMESPACE.getPath(dtc) + 
                    "/" +
                    ws.getName();
                
                return path;
            }
        },
        
        ROLLBACK_NAMESPACE {
            public String getPath(XATransactionsBuilder.WorkerScheduleConfiguration ws, XATransactionsBuilder.DistributedTransactionConfiguration dtc){
                String path = TransactionZnodes.ROLLBACK_NAMESPACE.getPath(dtc) + 
                    "/" +
                    ws.getName();
                
                return path;
            }
        };
        

        public abstract String getPath(XATransactionsBuilder.WorkerScheduleConfiguration ws, XATransactionsBuilder.DistributedTransactionConfiguration dtc);
    }
    
    public XATransactionResource(XATransactionsBuilder.WorkerScheduleConfiguration workerScheduleConf, XATransactionsBuilder.DistributedTransactionConfiguration distributedTransactionConfiguration, String workerId) {
        this.workerScheduleConf = workerScheduleConf;
        this.distributedTransactionConfiguration = distributedTransactionConfiguration;
        this.workerId = workerId;
        
        this.executor = new ThreadPoolExecutor(1, 1, 
                1000L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(200));
        
        log.debug(WorkerZnodes.WORKER_NAMESPACE + ": \t" + WorkerZnodes.WORKER_NAMESPACE.getPath(workerScheduleConf, distributedTransactionConfiguration));
        log.debug(WorkerZnodes.ASSIGN_NAMESPACE + ": \t" + WorkerZnodes.ASSIGN_NAMESPACE.getPath(workerScheduleConf, distributedTransactionConfiguration));
    }
    
    /* 
     * Creacion del nodo para las tareas asignadas a este worker
     */
    public void bootstrap(){
        createAssignNode();
    }
    
    /* *****************************************
     * *****************************************
     * Znode para recibir asignaciones de tareas
     * *****************************************
     * *****************************************
     */
    void createAssignNode(){
        zkc.zk.create(
                WorkerZnodes.ASSIGN_NAMESPACE.getPath(workerScheduleConf, distributedTransactionConfiguration)+ "/" + workerId, 
                new byte[0], 
                ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                CreateMode.PERSISTENT,
                createAssignCallback, 
                null);
    }
    
    AsyncCallback.StringCallback createAssignCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) { 
            case CONNECTIONLOSS:
                createAssignNode();
                break;
            case OK:
                log.info("[" + workerId + "] Nodo de asignacion creado.");
                break;
            case NODEEXISTS:
                log.warn("[" + workerId + "] Nodo de asignacion ya existe.");
                break;
            default:
                log.error("[" + workerId + "] Error al crear nodo de asignacion: " + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    
    /* *****************************************
     * *****************************************
     * Registro del proceso worker en el sistema
     * *****************************************
     * *****************************************
     */
    public void register(){
        zkc.zk.create(
                WorkerZnodes.WORKER_NAMESPACE.getPath(workerScheduleConf, distributedTransactionConfiguration)+ "/" + workerId,
                "Idle".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                createWorkerCallback,
                null);
    }
    
    AsyncCallback.StringCallback createWorkerCallback = new AsyncCallback.StringCallback() {

        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(KeeperException.Code.get(rc)){
            case CONNECTIONLOSS:
                register();
                break;
            case OK:
                log.info("[" + workerId + "] Registrado exitosamente ");
                break;
            case NODEEXISTS:
                log.warn("[" + workerId + "] El worker ya existe ");
                break;
            default:
                log.error("[" + workerId + "] Ocurrio un error: " + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    
    /* ********************************************
     * ********************************************
     * Administracion de tareas asignadas al worker
     * ********************************************
     * ********************************************
     */
    
    // Cache de las tareas asignadas
    ChildrenCache assignedTasksCache = new ChildrenCache();
    
    /*
     * Obtener las tareas asignadas
     */
    public void getTasks(){
        zkc.zk.getChildren(
                WorkerZnodes.ASSIGN_NAMESPACE.getPath(workerScheduleConf, distributedTransactionConfiguration)+ "/" + workerId,
                newTaskWatcher,
                tasksGetChildrenCallback,
                null);
    }
    
    Watcher newTaskWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if(event.getType() == Event.EventType.NodeChildrenChanged) {
                assert new String(WorkerZnodes.ASSIGN_NAMESPACE.getPath(workerScheduleConf, distributedTransactionConfiguration)+ "/" + workerId ).equals( event.getPath() );
                
                getTasks();
            }
        }
    };
    
    /*
     * Recibe las tareas (children) cuando exista un cambio en las asignaciones
     * para este worker
     */
    AsyncCallback.ChildrenCallback tasksGetChildrenCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch(KeeperException.Code.get(rc)) { 
            case CONNECTIONLOSS:
                getTasks();
                break;
            case OK:
                if(children != null){
                    executor.execute(new Runnable() {
                        List<String> children;
                        AsyncCallback.DataCallback cb;
                        
                        /*
                         * Initializes input of anonymous class
                         */
                        public Runnable init (List<String> children, AsyncCallback.DataCallback cb) {
                            this.children = children;
                            this.cb = cb;
                            
                            return this;
                        }
                        
                        public void run() {
                            if(children == null) {
                                return;
                            }
    
                            log.debug("[" + workerId + "] Reorrienda nuevas tareas asignadas");
                            //setStatus("Working");
                            for(String task : children){
                                log.trace("[" + workerId + "] New task: {" + task + "}");
                                zkc.zk.getData(WorkerZnodes.ASSIGN_NAMESPACE.getPath(workerScheduleConf, distributedTransactionConfiguration)+ "/" + workerId  + "/" + task,
                                        false,
                                        cb,
                                        task);   
                            }
                        }
                    }.init(assignedTasksCache.addedAndSet(children), taskDataCallback));
                } 
                break;
            default:
                log.error("[" + workerId + "] Error al obtener las tareas asignadas: " + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    
    /*
     * procesa los datos de cada tarea asignada al worker
     */
    AsyncCallback.DataCallback taskDataCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx /* Nombre de la tarea*/, byte[] data, Stat stat) {
            switch(KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                zkc.zk.getData(path, false, taskDataCallback, ctx);
                break;
            case OK:
                /*
                 *  Aqui es donde se ejecuta la tarea
                 */
                executor.execute( new Runnable() {
                    byte[] data;
                    Object ctx;
                    
                    /*
                     * Initializes the variables this anonymous class needs
                     */
                    public Runnable init(byte[] data, Object ctx) {
                        this.data = data;
                        this.ctx = ctx;
                        
                        return this;
                    }
                    
                    public void run() {
                        /*
                         * En esta seccion se realizara la ejecucion de la transaccion
                         */
                        log.info("[" + workerId + "] " + /*ZKConexion.getCurrentTimeStamp() +*/ " Ejecutando tarea: " + "{" + ctx + "} :" + new String(data));
                        
                        // Objeto que ejecuta la logica del worker, es cargado dinamicamente
                        AbstractXATransactionExecutable distributedTransactionExecutable = null;
                        
                        Boolean flagEjecucionExitosa = false;
                        String errorMsg = "";
                        try {
                            Object object = Class.forName(workerScheduleConf.getImplementationClassName()).newInstance(); 
                            distributedTransactionExecutable = (AbstractXATransactionExecutable) object;
                            
                            // Data para la ejecucion de la tarea
                            distributedTransactionExecutable.setData(data);
                            
                            // Ejecucion de la tarea, logica del usuario
                            distributedTransactionExecutable.runExecute();
                            
                            flagEjecucionExitosa = true;
                        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
                            ex.printStackTrace();
                        } catch (Exception ex){
                            log.error("Error de ejecucion de tarea: " + ex.getMessage());
                            
                            errorMsg = ex.getMessage().toString();
                            flagEjecucionExitosa = false;
                        }
                        
                        byte[] result = null;
                        JSONObject metadata = null;
                        try{
                            if(flagEjecucionExitosa){
                                // Si la ejecucion es satisfactoria agregar status Success
                                distributedTransactionExecutable.setSuccessStatus();
                            }else{
                                // Si fallo la ejecucion agregar status Error
                                distributedTransactionExecutable.setErrorStatus(errorMsg);
                            }
                            
                            // Resultado
                            result = distributedTransactionExecutable.getData();
                            // Metadata 
                            // metadata = distributedTransactionExecutable.getMetadata();
                        } catch(ParseException ex){
                            ex.printStackTrace();
                        }   
                        
                        // Crea un znode para notificar la ejecucion de la tarea
                        // y ser procesado por el siguiente worker
                        zkc.zk.create(
                                WorkerZnodes.STATUS_NAMESPACE.getPath(workerScheduleConf, distributedTransactionConfiguration)+"/" + (String) ctx, 
                                result, 
                                ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                                CreateMode.PERSISTENT, 
                                taskStatusCreateCallback, 
                                result /* Resultado de la tarea actual */);
                            
                        // Eliminar asignacion
                        zkc.zk.delete(
                                WorkerZnodes.ASSIGN_NAMESPACE.getPath(workerScheduleConf, distributedTransactionConfiguration) + "/"  + workerId + "/" + (String) ctx, 
                                -1, 
                                taskVoidCallback, 
                                null);

                    }
                }.init(data, ctx));
                
                break;
            default:
                log.error("[" + workerId + "] Failed to get task data: ", KeeperException.create(KeeperException.Code.get(rc), path));
            }

        }
    };
    
    /*
     * Una vez ejecutada la tarea se crea un znode indicando su estado
     */
    AsyncCallback.StringCallback taskStatusCreateCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx /* byte [] Data */, String name) {
            switch(KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                zkc.zk.create(
                        path, 
                        (byte[]) ctx, 
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                        CreateMode.PERSISTENT,
                        taskStatusCreateCallback, 
                        ctx /* Resultado de la tarea actual */);
                break;
            case OK:
                log.info("[" + workerId + "] Status/Resultado de tarea creado correctamente: " + name);
                break;
            case NODEEXISTS:
                log.warn("[" + workerId + "] Status/Resultado ya existe o creado previamente: " + path);
                break;
            default:
                log.error("[" + workerId + "] Error al crear status/resultado para la tarea: ", KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    
    /*
     * Callback de eliminacion de asignacion de tarea al worker
     */
    AsyncCallback.VoidCallback taskVoidCallback = new AsyncCallback.VoidCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx) {
            switch(KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                zkc.zk.delete(
                    path,
                    -1, 
                    taskVoidCallback, 
                    null);
                break;
            case OK:
                log.info("[" + workerId + "] Asignacion de tarea eliminada exitosamente: " + path);
                break;
            default:
                log.error("[" + workerId + "] Error al elminar la asignacion de tarea: " + KeeperException.create(KeeperException.Code.get(rc), path));
            } 
        }
    };
    
    
    
    
    class TransactionWorkerBootstrap {
        XATransactionsBuilder.WorkerScheduleConfiguration wsc;
        XATransactionsBuilder.DistributedTransactionConfiguration dtc;

        public TransactionWorkerBootstrap(XATransactionsBuilder.WorkerScheduleConfiguration wsc, XATransactionsBuilder.DistributedTransactionConfiguration dtc) {
            this.wsc = wsc;
            this.dtc = dtc;
        }

        public void execute(){
            try{
                zkc.zk.create(
                    WorkerZnodes.WORKER_NAMESPACE.getPath(wsc, dtc), 
                    "Worker namespace".getBytes(), 
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                    CreateMode.PERSISTENT);
            }catch (KeeperException | InterruptedException ex){}

            try{
                zkc.zk.create(
                    WorkerZnodes.ASSIGN_NAMESPACE.getPath(wsc, dtc), 
                    "Assign namespace".getBytes(), 
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                    CreateMode.PERSISTENT);
            }catch (KeeperException | InterruptedException ex){}
            
            try{
                zkc.zk.create(
                    WorkerZnodes.STATUS_NAMESPACE.getPath(wsc, dtc), 
                    "Status namespace".getBytes(), 
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                    CreateMode.PERSISTENT);
            }catch (KeeperException | InterruptedException ex){}
            
            try{
                zkc.zk.create(
                    WorkerZnodes.ROLLBACK_NAMESPACE.getPath(wsc, dtc), 
                    "Status namespace".getBytes(), 
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                    CreateMode.PERSISTENT);
            }catch (KeeperException | InterruptedException ex){}
        }
    }
}
