/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lindelit.xatransactions.test;

import com.lindelit.xatransactions.XATransactionsBuilder;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jdom2.JDOMException;

/**
 *
 * @author carloslucero
 */
public class PruebaXATransaction2 {
    static {System.setProperty("log4j.configurationFile", "log4j2.xml");}
    private final static Logger log = LogManager.getLogger(PruebaXATransaction2.class);
    
    public static void main(String[] args) throws JDOMException, IOException, InterruptedException{
        // Creacion del builder
        XATransactionsBuilder builder = new XATransactionsBuilder("aegis-conf-2.xml");
        // Correr transacciones distribuidas
        builder.runDistributedTransactions();
        
        Thread.sleep(600000);
    }
}
