/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lindelit.transactions.test;

import com.lindelit.transactions.distributed.DistributedTransactionClient;
import com.lindelit.transactions.distributed.DistributedTransactionsBuilder;
import static com.lindelit.transactions.test.PruebaClient1.generateTask;
import static com.lindelit.transactions.test.PruebaClient1.submitAutomaticTransactions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.jdom2.JDOMException;
import org.json.simple.JSONObject;

/**
 *
 * @author carloslucero
 */
public class PruebaClient2 {
    private final static Logger log = Logger.getLogger(PruebaClient2.class);
    
    public static JSONObject generateTask(){
        JSONObject task = new JSONObject();
        task.put("nombres", "johanna elizabeth");
        task.put("apellidos", "leon tenesaca");
        task.put("email", "jely@hotmail.com");
        task.put("prueba", "iniciamos joha");
        
        return task;
    }
    
    public static void main(String[] args) throws JDOMException, IOException, InterruptedException{
        DOMConfigurator.configure("log4j.xml");
        
        // Creacion del builder
        DistributedTransactionsBuilder builder = new DistributedTransactionsBuilder("aegis-conf-2.xml");
        // Correr transacciones distribuidas
        builder.runDistributedTransactions();
        
        /*
        // Cliente de prueba
        DistributedTransactionsBuilder.DistributedTransactionConfiguration dtcLogeo = builder.getTransactionConfiguration("logeo");
        DistributedTransactionsBuilder.DistributedTransactionConfiguration dtcEjemplo = builder.getTransactionConfiguration("post-ejemplo");
        
        ArrayList<DistributedTransactionsBuilder.DistributedTransactionConfiguration> dtcs = new ArrayList<>();
        dtcs.add(dtcEjemplo);
        dtcs.add(dtcLogeo);
        DistributedTransactionClient client = new DistributedTransactionClient("cliente-2", dtcs);
        
        client.init();
        
        // Creacion de datos de transaccion JSON
        JSONObject task = generateTask();
        log.debug("TAREA JSON: " + task.toJSONString());
        
        PruebaClient1.submitAutomaticTransactions(10, client, task);
        
        Thread.sleep(31000);
        
        
        // Calculo de latencia
        ConcurrentHashMap<String, DistributedTransactionClient.TransactionObject> transacciones = client.ctxMapCopy;
        log.info("TERMINADO TODO " + transacciones.size());
        long sumaLatencias = 0;
        long latenciaMaxima = 0;
        long latenciaMinima = 100000000;
        for (Map.Entry<String, DistributedTransactionClient.TransactionObject> entry : transacciones.entrySet()) {
            String string = entry.getKey();
            DistributedTransactionClient.TransactionObject transactionObject = entry.getValue();
            
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
        log.info("LATENCIA MAXIMA   : " + latenciaMaxima);*/
        
        Thread.sleep(600000);
    }
}
