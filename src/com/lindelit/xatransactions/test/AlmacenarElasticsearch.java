/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lindelit.xatransactions.test;

import com.lindelit.xatransactions.AbstractXATransactionExecutable;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

/**
 *
 * @author carloslucero
 */
public class AlmacenarElasticsearch extends AbstractXATransactionExecutable{
    private final static Logger log = Logger.getLogger(AlmacenarElasticsearch.class);
    
    @Override
    public byte[] execute(byte[] data) {
        JSONObject dataJson = new JSONObject();
        
        try {
            dataJson = parseData();
            String valorPrueba = dataJson.get("prueba").toString();
            
            dataJson.put("prueba", valorPrueba + "->almacenar elasticsearch");
        } catch (ParseException ex) {
            ex.printStackTrace();
        }
        
        return dataJson.toJSONString().getBytes();
    }

    @Override
    public byte[] rollback(byte[] data) {
        log.debug("DICE QUE HAGA ROLLBACK DE ALGO!");
        
        return data;
    }

    @Override
    public byte[] validate(byte[] data) {
        log.debug("DICE QUE HAGA VALIDATE DE ALGO!");
        
        return data;
    }
    
}