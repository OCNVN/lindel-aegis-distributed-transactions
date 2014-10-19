/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lindelit.xatransactions;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author carloslucero
 */
public abstract class AbstractXATransactionExecutable {
    private byte[] data;
    
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
        JSONObject dataJson = new JSONObject();
        
        try {
            dataJson = parseData();
            dataJson = execute(dataJson);
        } catch (ParseException ex) {
            ex.printStackTrace();
        }
        
        setData(dataJson.toJSONString().getBytes());
        return getData();
    }
    
    /*
     * Ejecuta la logica de rollback de worker
     */
    public byte[] runRollback(){
        JSONObject dataJson = new JSONObject();
        
        try {
            dataJson = parseData();
            dataJson = rollback(dataJson);
        } catch (ParseException ex) {
            ex.printStackTrace();
        }
        
        setData(dataJson.toJSONString().getBytes());
        return getData();
    }
    
    /*
     * Ejecuta logica de validacion de worker
     */
    public byte[] runValidate(){
        JSONObject dataJson = new JSONObject();
        
        try {
            dataJson = parseData();
            dataJson = validate(dataJson);
        } catch (ParseException ex) {
            ex.printStackTrace();
        }
        
        setData(dataJson.toJSONString().getBytes());
        return getData();
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }
    
    public JSONObject getMetadata() throws ParseException{
        JSONObject metadata = (JSONObject) parseData().get(XATransactionUtils.AssignMetadataNodes.XA_ASSIGN_METADATA_NODE.getNode());
        return metadata;
    }
    
    protected JSONObject parseData() throws ParseException{
        JSONParser parser=new JSONParser();

        String dataString = new String(data);
        Object dataObject = parser.parse(dataString);
        JSONObject dataJson = (JSONObject) dataObject;
        
        return dataJson;
    }
    
    public void setSuccessStatus() throws ParseException{
        JSONObject dataJson = parseData();
        
        JSONObject xaStatusNode = (JSONObject) dataJson.get(XATransactionUtils.TransactionStatusNodes.XA_STATUS_NODE.getNode());
        xaStatusNode.put(XATransactionUtils.TransactionStatusNodes.STATUS_CHILD.getNode(), XATransactionUtils.TransactionStatusNodes.STATUS_SUCCESS_VALUE_NODE.getNode());
        
        this.data = dataJson.toJSONString().getBytes();
    }
    
    public void setErrorStatus(String mensaje) throws ParseException{
        JSONObject dataJson = parseData();
        
        JSONObject xaStatusNode = (JSONObject) dataJson.get(XATransactionUtils.TransactionStatusNodes.XA_STATUS_NODE.getNode());
        xaStatusNode.put(XATransactionUtils.TransactionStatusNodes.STATUS_CHILD.getNode(), XATransactionUtils.TransactionStatusNodes.STATUS_ERROR_VALUE_NODE.getNode());
        xaStatusNode.put(XATransactionUtils.TransactionStatusNodes.MESSAGE_CHILD.getNode(), mensaje);
        
        this.data = dataJson.toJSONString().getBytes();
    }
}
