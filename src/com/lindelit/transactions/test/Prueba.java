/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lindelit.transactions.test;

import com.lindelit.transactions.distributed.DistributedTransactionClient;
import com.lindelit.transactions.distributed.DistributedTransactionsBuilder;
import java.io.IOException;
import org.apache.log4j.xml.DOMConfigurator;
import org.jdom2.JDOMException;

/**
 *
 * @author carloslucero
 */
public class Prueba {
    public static void main(String[] args) throws JDOMException, IOException, InterruptedException{
        DOMConfigurator.configure("log4j.xml");
        
        // Creacion del builder
        DistributedTransactionsBuilder builder = new DistributedTransactionsBuilder("aegis-conf.xml");
        // Correr transacciones distribuidas
        builder.runDistributedTransactions();
        
        
        Thread.sleep(600000);
    }
}
