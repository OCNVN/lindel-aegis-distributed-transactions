/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lindelit.xatransactions;

import com.lindelit.xatransactions.coordinator.XATransactionCoordinator;
import java.util.ArrayList;
import org.apache.log4j.Logger;

/**
 *
 * @author carloslucero
 */
public class XATransaction {
    private final static Logger log = Logger.getLogger(XATransaction.class);
    
    // Replicas del master
    ArrayList<XATransactionCoordinator> mastersReplicas;
    
    // Instancias de los workers e instancias paralelas
    ArrayList<ArrayList<XATransactionResource>> workersParallels;
    
    // Configuracion de la transaccion distribuida
    XATransactionsBuilder.DistributedTransactionConfiguration distributedTransactionConf;
    
    public enum TransactionSubfixes {
        TRANSACTION_ZNODE_SUBFIX ("transaction-");

        private final String subfix;
        TransactionSubfixes(String subfix){
            this.subfix = subfix;
        }
        public String getSubfix(){
            return subfix;
        }
    }

    public XATransaction(XATransactionsBuilder.DistributedTransactionConfiguration distributedTransactionConf) {
        this.distributedTransactionConf = distributedTransactionConf;
    }
    
    /*
     * Crea n cantidades de instancias de procesos master como replicas de
     * respaldo (Leader election)
     */
    void runMasters(){
        mastersReplicas = new ArrayList<>();
        log.debug("Replicas del master: " + distributedTransactionConf.getMasterReplicas());
        
        for (int i = 0; i < distributedTransactionConf.getMasterReplicas(); i++) {
            log.info("Replica de Master: " + i);
            // Creacion de procesos master, Id generado a partir del ID de la transaccion
            // y el numero correspondiente a la replica creada
            XATransactionCoordinator xatCoordinator = new XATransactionCoordinator(
                    distributedTransactionConf, 
                    distributedTransactionConf.getId() + "-" + i);
            
            // Inicializar coordinador
            xatCoordinator.init();
            // Correr como master
            xatCoordinator.runForMaster();
            
            mastersReplicas.add(xatCoordinator);
        }
    }
    
    /*
     * Crea los workers configurados para la transaccion y n cantidades de
     * instancias por cada worker para trabajar en paralelo (Parallelism)
     */
    void runWorkers(){
        // Recorremos los workers configurados para la transaccion
        for (XATransactionsBuilder.WorkerScheduleConfiguration wsc : distributedTransactionConf.getWorkersScheduleConfigurations()) {
            // Iniciamos por cada workers n cantidades de instancias segun paralelismo
            log.debug("Paralelismo de workers: " + wsc.getParallelism());
            
            // Si la implementacion del worker es externa, debe realizarse en el
            // componente o lenguaje externo, no aqui
            if(!wsc.isExternal()){
                for (int i = 0; i < wsc.getParallelism(); i++) {
                    log.debug("\t Instancia: " + i);
                    XATransactionResource xatResource = new XATransactionResource(
                            wsc, 
                            distributedTransactionConf,
                            wsc.getName() + "-" + (i + wsc.getIdOffset()));

                    // Iniciar resource
                    xatResource.init();
                    // Recursos necesarios en zookeeper
                    xatResource.bootstrap();
                    // Registrar resource
                    xatResource.register();
                    // Obtener tareas
                    xatResource.getTasks();
                }
            }
        }
    }

}
