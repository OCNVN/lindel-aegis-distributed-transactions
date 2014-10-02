/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lindelit.xatransactions.test;

import com.lindelit.xatransactions.XATransactionsBuilder;
import java.io.IOException;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.jdom2.JDOMException;

/**
 *
 * @author carloslucero
 */
public class PruebaXATransaction2 {
    private final static Logger log = Logger.getLogger(PruebaXATransaction2.class);
    
    public static void main(String[] args) throws JDOMException, IOException, InterruptedException{
        DOMConfigurator.configure("log4j.xml");
        
        // Creacion del builder
        XATransactionsBuilder builder = new XATransactionsBuilder("aegis-conf-2.xml");
        // Correr transacciones distribuidas
        builder.runDistributedTransactions();
        
        Thread.sleep(600000);
    }
}
