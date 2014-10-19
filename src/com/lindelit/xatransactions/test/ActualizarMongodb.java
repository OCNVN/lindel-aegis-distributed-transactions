/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lindelit.xatransactions.test;

import com.lindelit.xatransactions.AbstractXATransactionExecutable;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

/**
 *
 * @author carloslucero
 */
public class ActualizarMongodb extends AbstractXATransactionExecutable{
    private final static Logger log = Logger.getLogger(ActualizarMongodb.class);
    
    @Override
    public JSONObject execute(JSONObject dataJson) throws Exception {
        String valorPrueba = dataJson.get("prueba").toString();
        dataJson.put("prueba", valorPrueba + "->actualizar mongodb");
        
        //if("prueba".compareTo("prueba") == 0)
        //    throw new Exception("Se jodio pex");
        
        return dataJson;
    }

    @Override
    public JSONObject rollback(JSONObject dataJson) {
        log.debug("DICE QUE HAGA ROLLBACK DE ALGO!");
        
        return dataJson;
    }

    @Override
    public JSONObject validate(JSONObject dataJson) {
        log.debug("DICE QUE HAGA VALIDATE DE ALGO!");
        
        return dataJson;
    }
}