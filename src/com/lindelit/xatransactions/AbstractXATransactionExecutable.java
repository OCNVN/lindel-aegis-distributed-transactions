/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lindelit.xatransactions;

import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

/**
 *
 * @author carloslucero
 */
public abstract class AbstractXATransactionExecutable {
    // private byte[] data;
    
    private XATransactionJSONInterpreter xATransactionJSONInterpreter;
    
    /*
     * Metodos abstractos, aqui va la logica del servicio
     */
    public abstract JSONObject execute(JSONObject dataJson)  throws Exception;
    public abstract JSONObject rollback(JSONObject dataJson);
    public abstract JSONObject validate(JSONObject dataJson);
    
    /*
     * Ejecuta la logica de ejecucion del worker
     */
    public byte[] runExecute() throws Exception{
        JSONObject dataJson = xATransactionJSONInterpreter.getDataJson();
        
        try {
            dataJson = execute(dataJson);
            
            setData(dataJson.toJSONString().getBytes());
        } catch (ParseException ex) {
            ex.printStackTrace();
        }
        
        return getData();
    }
    
    /*
     * Ejecuta la logica de rollback de worker
     */
    public byte[] runRollback(){
        JSONObject dataJson = xATransactionJSONInterpreter.getDataJson();
        
        try {
            dataJson = rollback(dataJson);
            
            setData(dataJson.toJSONString().getBytes());
        } catch (ParseException ex) {
            ex.printStackTrace();
        }
        
        
        return getData();
    }
    
    /*
     * Ejecuta logica de validacion de worker
     */
    public byte[] runValidate(){
        JSONObject dataJson = xATransactionJSONInterpreter.getDataJson();
        
        try {
            dataJson = validate(dataJson);
            
            setData(dataJson.toJSONString().getBytes());
        } catch (ParseException ex) {
            ex.printStackTrace();
        }
        
        return getData();
    }

    public byte[] getData() {
        return xATransactionJSONInterpreter.getData();
    }

    public void setData(byte[] data) throws ParseException {
        xATransactionJSONInterpreter = new XATransactionJSONInterpreter(data);
    }
    
    public JSONObject getMetadata() throws ParseException {
        return xATransactionJSONInterpreter.getMetadata();
    }
    
    public void setSuccessStatus() throws ParseException {
        xATransactionJSONInterpreter.setSuccessStatus();
    }
    
    public void setErrorStatus(String mensaje) throws ParseException {
        xATransactionJSONInterpreter.setErrorStatus(mensaje);
    }
    
    public Boolean isErrorStatus() {
        return xATransactionJSONInterpreter.isErrorStatus();
    }
    
    public Boolean isExecutionPhase() {
        return xATransactionJSONInterpreter.isExecutionPhase();
    }
    
    public Boolean isRollbackPhase() {
        return xATransactionJSONInterpreter.isRollbackPhase();
    }
}
