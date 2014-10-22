/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lindelit.xatransactions;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author carloslucero
 */
public class XATransactionUtils {
    public static String getCurrentTimeStamp() {
        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");//dd/MM/yyyy
        Date now = new Date();
        String strDate = sdfDate.format(now);
        
        return strDate;
    }
    
    public static JSONObject parseData(byte [] data) throws ParseException{
        JSONParser parser=new JSONParser();

        String dataString = new String(data);
        Object dataObject = parser.parse(dataString);
        JSONObject dataJson = (JSONObject) dataObject;
        
        return dataJson;
    }
    
    public static JSONObject extractMetadata(byte [] data) throws ParseException{
        JSONObject dataJson = parseData(data);
        JSONObject metadataJson = (JSONObject) dataJson.get(AssignMetadataNodes.XA_ASSIGN_METADATA_NODE.getNode());
        
        return metadataJson;
    }
    
    public enum AssignMetadataNodes {
        XA_ASSIGN_METADATA_NODE ("XA-ASSIGN-METADATA"),
        CLIENT_ID_CHILD ("client-id");

        private final String node;
        AssignMetadataNodes(String node){
            this.node = node;
        }
        public String getNode(){
            return node;
        }
    }
    
    public enum TransactionStatusNodes {
        XA_STATUS_NODE ("XA-TRANSACTION-STATUS"),
        STATUS_CHILD ("status"),
        MESSAGE_CHILD ("message"),
        
        // Valores que puede tomar el child status
        STATUS_SUCCESS_VALUE_NODE ("success"),
        STATUS_ERROR_VALUE_NODE ("error"),
        STATUS_START_VALUE_NODE ("start");
        
        private final String node;
        TransactionStatusNodes(String node){
            this.node = node;
        }
        public String getNode(){
            return node;
        }
    }
}