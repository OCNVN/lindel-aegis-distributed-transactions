/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lindelit.transactions.distributed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import static org.apache.zookeeper.KeeperException.Code.CONNECTIONLOSS;
import static org.apache.zookeeper.KeeperException.Code.NONODE;
import static org.apache.zookeeper.KeeperException.Code.OK;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

/**
 *
 * @author carloslucero
 */
public class DistributedTransactionClient implements Watcher{
    private final static Logger log = Logger.getLogger(DistributedTransactionClient.class);
    
    // Idnetificador del cliente
    private String clientId;
    
    // Configuraciones de las transacciones que el cliente soporta
    private ArrayList<DistributedTransactionsBuilder.DistributedTransactionConfiguration> distributedTransactions;
    
    // Configuracion de transacciones por ID
    private Dictionary<String, DistributedTransactionsBuilder.DistributedTransactionConfiguration> distributedTransactionsDictionary;
    
    // Conexion a zookeeper
    com.lindelit.coordinator.ZKConexion zkc;

    public void init() {
        zkc = new com.lindelit.coordinator.ZKConexion();
        try {
            zkc.connect(this);
            
            // Iniciar recursos necesarios en zookeeper
            TransactionClientBootstrap tcb = new TransactionClientBootstrap(distributedTransactions, clientId);
            tcb.execute();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    // Znodes necesarios para el funcionamiento del master
    public enum ClientZnodes {
        TRANSACTION_CLIENT_ZNODE {
            public String getPath(String clientId, DistributedTransactionsBuilder.DistributedTransactionConfiguration dtc){
                String path = DistributedTransactionMaster.TransactionZnodes.TRANSACTION_CLIENTS_NAMESPACE.getPath(dtc)+  
                    "/" +
                    clientId;
                
                return path;
            }
        },
        TRANSACTIONS_NAMESPACES {
            public String getPath(String clientId, DistributedTransactionsBuilder.DistributedTransactionConfiguration dtc){
                String path = DistributedTransactionMaster.TransactionZnodes.TRANSACTIONS_NAMESPACE.getPath(dtc) + 
                    "/" +
                    clientId;
                
                return path;
            }
        },
        
        RESULTS_NAMESPACES {
            public String getPath(String clientId, DistributedTransactionsBuilder.DistributedTransactionConfiguration dtc){
                String path = DistributedTransactionMaster.TransactionZnodes.RESULTS_NAMESPACE.getPath(dtc) +
                    "/" +
                    clientId;
                
                return path;
            }
        };

        public abstract String getPath(String clientId, DistributedTransactionsBuilder.DistributedTransactionConfiguration dtc);
    }
    
    public DistributedTransactionClient(String clientId, ArrayList<DistributedTransactionsBuilder.DistributedTransactionConfiguration> distributedTransactions) {
        this.distributedTransactions = distributedTransactions;
        this.clientId = clientId;
        
        distributedTransactionsDictionary = new Hashtable<>();
        for(DistributedTransactionsBuilder.DistributedTransactionConfiguration dtc: distributedTransactions){
            log.debug(dtc.getId());
            distributedTransactionsDictionary.put(dtc.getId(), dtc);
        }
        
        log.debug("NAMESPACES");
        for(DistributedTransactionsBuilder.DistributedTransactionConfiguration dtc: distributedTransactions){
            log.debug(ClientZnodes.TRANSACTIONS_NAMESPACES.getPath(clientId, dtc));
            log.debug(ClientZnodes.RESULTS_NAMESPACES.getPath(clientId, dtc));
            log.debug(ClientZnodes.TRANSACTION_CLIENT_ZNODE.getPath(clientId, dtc));
        }
    }

    /* **********************
     * **********************
     * Envio de transacciones
     * **********************
     * **********************
     */
    public void submitTransaction(String transactionId, String transaction, TransactionObject transactionCtx){
        transactionCtx.setTransaction(transaction);
        DistributedTransactionsBuilder.DistributedTransactionConfiguration dtc = distributedTransactionsDictionary.get(transactionId);
        transactionCtx.setDistributedTransaction(dtc);
        
        zkc.zk.create(
                ClientZnodes.TRANSACTIONS_NAMESPACES.getPath(clientId, dtc)+ "/" + clientId + "-" + DistributedTransaction.TransactionSubfixes.TRANSACTION_ZNODE_SUBFIX.getSubfix(),
                transaction.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL,
                createTransactionCallback,
                transactionCtx);
    }
    
    AsyncCallback.StringCallback createTransactionCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx /* TransactionObject */, String name) {
            TransactionObject transactionObject = (TransactionObject)ctx;
            switch (KeeperException.Code.get(rc)) { 
            case CONNECTIONLOSS:
                /*
                 * Handling connection loss for a sequential node is a bit
                 * delicate. Executing the ZooKeeper create command again
                 * might lead to duplicate transactions. For now, let's assume
                 * that it is ok to create a duplicate transactions.
                 */
                log.warn("Conexion perdida, posible creacion de transaccion duplicada: " + name);
                submitTransaction(
                        transactionObject.getDistributedTransaction().getId(),
                        transactionObject.getTransaction(), 
                        transactionObject);
                
                break;
            case OK:
                log.debug(ADTUtils.getCurrentTimeStamp() + " Transaccion creada: " + name);
                // El nombre de la transaccion es generado automaticamente por
                // zookeeper
                transactionObject.setTransactionName(name);
                transactionObject.submitTimestamp = (new Date()).getTime();
                
                // Monitoreamos el estado de la transaccion enviada
                watchResults(name.replace(ClientZnodes.TRANSACTIONS_NAMESPACES.getPath(clientId, transactionObject.distributedTransaction), ClientZnodes.RESULTS_NAMESPACES.getPath(clientId, transactionObject.distributedTransaction)), ctx);
                
                break;
            default:
                log.error("Error al crear la transaccion: " + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    
    // Diccionario Path del znode del resultado de transaccion => TransactionObject
    public ConcurrentHashMap<String, Object> ctxMap = new ConcurrentHashMap<String, Object>();
    public ConcurrentHashMap<String, TransactionObject> ctxMapCopy = new ConcurrentHashMap<String, TransactionObject>();
    
    /*
     * Monitorear el estado de la transaccion enviada
     */
    void watchResults(String path, Object ctx /* TransactionObject */){
        ctxMap.put(path, ctx);
        ctxMapCopy.put(path, (TransactionObject) ctx);
        
        zkc.zk.exists(
                path,
                resultsWatcher,
                existsCallback,
                ctx /* TransactionObject */);
    }
    
    Watcher resultsWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if(event.getType() == Event.EventType.NodeCreated) {
                assert ctxMap.containsKey( event.getPath() );
                
                // Cuando el znode sea creado obtenemos los datos del znode
                // q contienen informacion de la transaccion ejecutada
                zkc.zk.getData(event.getPath(), 
                        false, 
                        getDataCallback, 
                        ctxMap.get(event.getPath()));
            }
        }
    };
    
    AsyncCallback.StatCallback existsCallback = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                watchResults(path, ctx);
                
                break;
            case OK:
                if(stat != null){
                    // si el znode existe obtenemos los datos q contienen 
                    // informacion de la transaccion ejecutada
                    zkc.zk.getData(
                            path, 
                            false, 
                            getDataCallback, 
                            ctx);
                    log.info("Znode del resultado existe: " + path);
                } 
                
                break;
            case NONODE:
                break;
            default:     
                log.error("Ocurrio un error al verificar la existencia del znode de resultado: " + 
                        KeeperException.create(KeeperException.Code.get(rc), path));
                
                break;
            }
        }
    };
    
    AsyncCallback.DataCallback getDataCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                /*
                 * Try again.
                 */
                zkc.zk.getData(path, false, getDataCallback, ctxMap.get(path));
                return;
            case OK:
                /*
                 *  Print result
                 */
                String transactionResult = new String(data);
                log.debug(ADTUtils.getCurrentTimeStamp() + " Transaccion procesada: " + path + ", " + transactionResult);
                ((TransactionObject) ctx).executeTimestamp = (new Date()).getTime();
                
                /*
                 *  Setting the result of the transaction
                 */
                assert(ctx != null);
                ((TransactionObject) ctx).setStatus(transactionResult.contains("done"));
                
                /*
                 *  Delete result znode
                 */
                zkc.zk.delete(path, -1, transactionResultDeleteCallback, null);
                ctxMap.remove(path);
                break;
            case NONODE:
                log.warn("El znode del status no existe! " + path);
                return; 
            default:
                log.error("Error al obtener los datos del status de la transaccion: " + 
                        KeeperException.create(KeeperException.Code.get(rc), path));               
            }
        }
    };
    
    AsyncCallback.VoidCallback transactionResultDeleteCallback = new AsyncCallback.VoidCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx) {
            switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                zkc.zk.delete(path, -1, transactionResultDeleteCallback, null);
                break;
            case OK:
                log.debug("Resultado eliminado " + path);
                break;
            default:
                log.error("Error al eliminar el resultado " + 
                        KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    
    
    
    
    
    
    
    
    
    public static class TransactionObject {
        private DistributedTransactionsBuilder.DistributedTransactionConfiguration distributedTransaction;
        private String transaction;
        private String transactionName;
        private boolean done = false;
        private boolean succesful = false;
        private CountDownLatch latch = new CountDownLatch(1);
        public long submitTimestamp;
        public long executeTimestamp;

        public DistributedTransactionsBuilder.DistributedTransactionConfiguration getDistributedTransaction() {
            return distributedTransaction;
        }

        public void setDistributedTransaction(DistributedTransactionsBuilder.DistributedTransactionConfiguration distributedTransaction) {
            this.distributedTransaction = distributedTransaction;
        }
        
        String getTransaction () {
            return transaction;
        }
        
        void setTransaction (String transaction) {
            this.transaction = transaction;
        }
        
        void setTransactionName(String name){
            this.transactionName = name;
        }
        
        String getTransactionName (){
            return transactionName;
        }
        
        void setStatus (boolean status){
            succesful = status;
            done = true;
            latch.countDown();
        }
        
        public void waitUntilDone () {
            try{
                latch.await();
            } catch (InterruptedException e) {
                log.warn("InterruptedException while waiting for transaction to get done");
            }
        }
        
        synchronized boolean isDone(){
            return done;     
        }
        
        synchronized boolean isSuccesful(){
            return succesful;
        }
        
    }
    
    @Override
    public void process(WatchedEvent event) {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    class TransactionClientBootstrap {
        private ArrayList<DistributedTransactionsBuilder.DistributedTransactionConfiguration> distributedTransactions;
        String clientId;

        public TransactionClientBootstrap(ArrayList<DistributedTransactionsBuilder.DistributedTransactionConfiguration> distributedTransactions, String clientId) {
            this.distributedTransactions = distributedTransactions;
            this.clientId = clientId;
        }
        
        public void execute(){
            for(DistributedTransactionsBuilder.DistributedTransactionConfiguration dtc : distributedTransactions){
                try{
                    zkc.zk.create(
                        ClientZnodes.TRANSACTION_CLIENT_ZNODE.getPath(clientId, dtc), 
                        "Root namespace".getBytes(), 
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                        CreateMode.PERSISTENT);
                }catch (KeeperException | InterruptedException ex){}
                
                try{
                    zkc.zk.create(
                        ClientZnodes.RESULTS_NAMESPACES.getPath(clientId, dtc), 
                        "Root namespace".getBytes(), 
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                        CreateMode.PERSISTENT);
                }catch (KeeperException | InterruptedException ex){}
                
                try{
                    zkc.zk.create(
                        ClientZnodes.TRANSACTIONS_NAMESPACES.getPath(clientId, dtc), 
                        "Root namespace".getBytes(), 
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                        CreateMode.PERSISTENT);
                }catch (KeeperException | InterruptedException ex){}
            }
        }
    }
}