/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lindelit.xatransactions;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import org.apache.log4j.Logger;
import org.jdom2.DataConversionException;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

/**
 *
 * @author carloslucero
 */
public class XATransactionsBuilder {
    private final static Logger log = Logger.getLogger(XATransactionsBuilder.class);
    
    // Documento cargado con la configuracion de las transacciones
    protected Document jdomDocument;
    
    // Transacciones distribuidas
    protected ArrayList<DistributedTransactionConfiguration> distributedTransactionsConfigurations;
    
    protected ArrayList<XATransaction> distributedTransactions;
    
    // Transacciones distribuidas por ID
    protected Dictionary<String, DistributedTransactionConfiguration> dtcDictionary;
    
    /*
     * Cargar configuracion y crear las transacciones
     * @param filename Path del archivo de configuracion
     */
    public XATransactionsBuilder(String filename) throws JDOMException, IOException{
        dtcDictionary = new Hashtable<>();
        
        // Archivo de configuracion
        File confFile = new File(filename);
        
        _build(confFile);
        loadConfiguration();
    }
    
    /*
     * Cargar configuracion y crear las transacciones
     * @param confFile Archivo de configuracion
     */
    public XATransactionsBuilder(File confFile) throws JDOMException, IOException{
        dtcDictionary = new Hashtable<>();
        
        _build(confFile);
        loadConfiguration();
    }
    
    public void runDistributedTransactions() throws JDOMException{
        
        bootstrap();
        run();
    }
    
    private void run(){
        distributedTransactions = new ArrayList<>();
        
        for(DistributedTransactionConfiguration dtc : distributedTransactionsConfigurations){
            XATransaction dt = new XATransaction(dtc);
            dt.runMasters();
            dt.runWorkers();
            
            distributedTransactions.add(dt);
        }
    }
    
    private void _build(File confFile) throws JDOMException, IOException{
        SAXBuilder jdomBuilder = new SAXBuilder();
        
        // Cargar archivo de configuracion xml
        this.jdomDocument = jdomBuilder.build(confFile);
    }
    
    private void bootstrap(){
        // Iniciar recursos necesarios en Zookeeper
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.init();
        bootstrap.execute();
    }
    
    private void loadConfiguration() throws DataConversionException{
        distributedTransactionsConfigurations = new ArrayList<>();
        
        Element applicationNode = this.jdomDocument.getRootElement();
         
        log.debug(applicationNode.getName());
        
        // Cargar configuracion de transacciones distribuidas
        for(Element transaccionDistribuida : applicationNode.getChildren("distributed-transaction")){
            log.debug(transaccionDistribuida.getName());
            
            String id = transaccionDistribuida.getAttribute("id").getValue();
            Integer masterReplicas = transaccionDistribuida.getAttribute("master-replicas").getIntValue();
            
            DistributedTransactionConfiguration dt = new DistributedTransactionConfiguration(id, masterReplicas);
            
            // Cargar configuracion de workers schedule
            Element workersScheduleNode = transaccionDistribuida.getChild("workers-schedule");
            if(workersScheduleNode != null){
                // Elements worker-schedule
                List<Element> workerScheduleElements = workersScheduleNode.getChildren("worker-schedule");
                
                for (int i = 0; i < workerScheduleElements.size(); i++) {
                    Element workerSchedule = workerScheduleElements.get(i);
                    
                    log.debug(workerSchedule.getName());

                    String name = workerSchedule.getAttribute("name").getValue();
                    Integer scheduleOrder = workerSchedule.getAttribute("schedule-order").getIntValue();
                    Integer parallelism = workerSchedule.getAttribute("parallelism").getIntValue();
                    String implementationClassName = workerSchedule.getAttribute("implementation-class-name").getValue();
                    Integer idOffset = workerSchedule.getAttribute("id-offset").getIntValue();
                    
                    // Por default debe ser false external
                    Boolean external = workerSchedule.getAttribute("external").getBooleanValue();
                    
                    WorkerScheduleConfiguration ws = new WorkerScheduleConfiguration(name, scheduleOrder, parallelism, implementationClassName, idOffset, external);
                    
                    // Si es el ultimo worker en el schedule
                    if(i == (workerScheduleElements.size() - 1))
                        ws.setLast(true);
                    else
                        ws.setLast(false);
                    
                    // Si es el primer worker en el schedule
                    if(i == 0)
                        ws.setFirst(true);
                    else
                        ws.setFirst(false);
                    
                    log.debug("name: " + name + ", schedule-order: " + scheduleOrder + ", parallelism: " + parallelism);

                    dt.getWorkersScheduleConfigurations().add(ws);
                }
            }
            
            distributedTransactionsConfigurations.add(dt);
            dtcDictionary.put(dt.getId(), dt);
        }
    }
    
    public DistributedTransactionConfiguration getTransactionConfiguration(String id){
        return dtcDictionary.get(id);
    }
    
    
    public class DistributedTransactionConfiguration {
        // ID de la transaccion distribuida
        private String id;
        // Numero de instancias del master para recuperacion ante fallos
        private int masterReplicas = 1;

        // Configuracion de cada worker
        private ArrayList<WorkerScheduleConfiguration> workersScheduleConfigurations;

        public DistributedTransactionConfiguration(String id, int masterReplicas) {
            this.id = id;
            this.masterReplicas = masterReplicas;
            
            workersScheduleConfigurations = new ArrayList<>();
        }
        
        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public int getMasterReplicas() {
            return masterReplicas;
        }

        public void setMasterReplicas(int masterReplicas) {
            this.masterReplicas = masterReplicas;
        }

        public ArrayList<WorkerScheduleConfiguration> getWorkersScheduleConfigurations() {
            return workersScheduleConfigurations;
        }

        public void setWorkersScheduleConfigurations(ArrayList<WorkerScheduleConfiguration> workersScheduleConfigurations) {
            this.workersScheduleConfigurations = workersScheduleConfigurations;
        }
    }
    
    // Wrapper para configuracion de los workers
    public class WorkerScheduleConfiguration {
        // Nombre del worker
        private String name;
        // Orden de ejecucion dentro de la transaccion
        private int sheduleOrder;
        // Nivel de paralelismo
        private int parallelism;
        // Ofset para la generacion de ID's
        private int idOffset;
        // Si es el ultimo worker en el schedule
        private boolean last;
        // Si es el primer worker en el schedule
        private boolean first;
        // Si el worker se ejecuta externamente/otro lenguaje
        private boolean external;
        
        private String implementationClassName;

        public WorkerScheduleConfiguration(){}
        
        public WorkerScheduleConfiguration(String name, int sheduleOrder, int parallelism, String implementationClassName, int idOffset, boolean external) {
            this.name = name;
            this.sheduleOrder = sheduleOrder;
            this.parallelism = parallelism;
            this.implementationClassName = implementationClassName;
            this.idOffset = idOffset;
            this.external = external;
        }

        public boolean isExternal() {
            return external;
        }

        public void setExternal(boolean external) {
            this.external = external;
        }

        public boolean isLast() {
            return last;
        }

        public void setLast(boolean last) {
            this.last = last;
        }

        public boolean isFirst() {
            return first;
        }

        public void setFirst(boolean first) {
            this.first = first;
        }
        
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getSheduleOrder() {
            return sheduleOrder;
        }

        public void setSheduleOrder(int sheduleOrder) {
            this.sheduleOrder = sheduleOrder;
        }

        public int getParallelism() {
            return parallelism;
        }

        public void setParallelism(int parallelism) {
            this.parallelism = parallelism;
        }

        public String getImplementationClassName() {
            return implementationClassName;
        }

        public void setImplementationClassName(String implementationClassName) {
            this.implementationClassName = implementationClassName;
        }

        public int getIdOffset() {
            return idOffset;
        }

        public void setIdOffset(int idOffset) {
            this.idOffset = idOffset;
        }
    }
}
