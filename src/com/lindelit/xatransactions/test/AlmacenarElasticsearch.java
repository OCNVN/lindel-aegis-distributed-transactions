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
    public JSONObject execute(JSONObject dataJson) {
        String valorPrueba = dataJson.get("prueba").toString();
        dataJson.put("prueba", valorPrueba + "->almacenar elasticsearch");
            
        return dataJson;
    }

    @Override
    public JSONObject rollback(JSONObject dataJson) {
        log.debug("DICE QUE HAGA ROLLBACK DE ALGO! " + new  String(getData()));
        
        return dataJson;
    }

    @Override
    public JSONObject validate(JSONObject dataJson) {
        log.debug("DICE QUE HAGA VALIDATE DE ALGO!");
        
        return dataJson;
    }
    
}