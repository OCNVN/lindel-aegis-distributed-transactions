/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lindelit.xatransactions.test;

import com.lindelit.xatransactions.XATransactionClient;
import com.lindelit.xatransactions.XATransactionsBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jdom2.JDOMException;
import org.json.simple.JSONObject;

/**
 *
 * @author carloslucero
 */
public class PruebaXATransactionClient1 {
    static {System.setProperty("log4j.configurationFile", "log4j2.xml");}
    private final static Logger log = LogManager.getLogger(PruebaXATransactionClient1.class);
    
    public static JSONObject generateTask(){
        JSONObject task = new JSONObject();
        task.put("nombres", "carlos fernando");
        task.put("apellidos", "lucero alvarez");
        task.put("email", "nandolucero@hotmail.com");
        task.put("prueba", "iniciamos carlos");
        
        return task;
    }
    
    public static void submitAutomaticTransactions(int ammount, XATransactionClient client, JSONObject transaction){
        for (int i = 0; i < ammount; i++) {
            XATransactionClient.TransactionObject transactionObject = new XATransactionClient.TransactionObject();
            client.submitTransaction("logeo", transaction.toJSONString(), transactionObject);
            
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
    }
    
    public static void main(String[] args) throws JDOMException, IOException, InterruptedException{        
        // Creacion del builder
        XATransactionsBuilder builder = new XATransactionsBuilder("aegis-conf.xml");
        // Correr transacciones distribuidas
        // builder.runDistributedTransactions();
        
        // Cliente de prueba
        XATransactionsBuilder.DistributedTransactionConfiguration dtcLogeo = builder.getTransactionConfiguration("logeo");
        XATransactionsBuilder.DistributedTransactionConfiguration dtcEjemplo = builder.getTransactionConfiguration("post-ejemplo");
        
        ArrayList<XATransactionsBuilder.DistributedTransactionConfiguration> dtcs = new ArrayList<>();
        dtcs.add(dtcEjemplo);
        dtcs.add(dtcLogeo);
        XATransactionClient client = new XATransactionClient("cliente-1", dtcs);
        
        client.init();
        
        // Creacion de datos de transaccion JSON
        JSONObject task = generateTask();
        log.debug("TAREA JSON: " + task.toJSONString());
        
        submitAutomaticTransactions(1000, client, task);
        
        Thread.sleep(150000);
        
        
        // Calculo de latencia
        ConcurrentHashMap<String, XATransactionClient.TransactionObject> transacciones = client.ctxMapCopy;
        log.info("TERMINADO TODO " + transacciones.size());
        long sumaLatencias = 0;
        long latenciaMaxima = 0;
        long latenciaMinima = 100000000;
        for (Map.Entry<String, XATransactionClient.TransactionObject> entry : transacciones.entrySet()) {
            String string = entry.getKey();
            XATransactionClient.TransactionObject transactionObject = entry.getValue();
            
            long latencia = transactionObject.executeTimestamp - transactionObject.submitTimestamp;
            log.info("LATENCIA  : " + latencia);
            
            if(latencia < latenciaMinima)
                latenciaMinima = latencia;
            
            if(latencia > latenciaMaxima)
                latenciaMaxima = latencia;
            
            sumaLatencias += latencia;
        }
        
        long latenciaPromedio = sumaLatencias / transacciones.size();
        log.info("LATENCIA PROMEDIO : " + latenciaPromedio);
        log.info("LATENCIA MINIMA   : " + latenciaMinima);
        log.info("LATENCIA MAXIMA   : " + latenciaMaxima);
        
        Thread.sleep(600000);
    }
}
