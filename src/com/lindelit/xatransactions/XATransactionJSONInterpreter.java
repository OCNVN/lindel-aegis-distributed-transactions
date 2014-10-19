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
public class XATransactionJSONInterpreter {
    private byte[] data;
    private JSONObject dataJson;

    public XATransactionJSONInterpreter(byte[] data) throws ParseException {
        this.data = data;
        parseData();
    }
    
    public byte[] getData() {
        return data;
    }

    public JSONObject getDataJson() {
        return dataJson;
    }

    public void setDataJson(JSONObject dataJson) {
        this.dataJson = dataJson;
    }

    public void setData(byte[] data) throws ParseException {
        this.data = data;
        parseData();
    }
    
    public JSONObject getMetadata() throws ParseException{
        JSONObject metadata = (JSONObject) dataJson.get(XATransactionUtils.AssignMetadataNodes.XA_ASSIGN_METADATA_NODE.getNode());
        return metadata;
    }
    
    private void parseData() throws ParseException{
        JSONParser parser=new JSONParser();

        String dataString = new String(data);
        Object dataObject = parser.parse(dataString);
        JSONObject dataJson = (JSONObject) dataObject;
        
        this.dataJson = dataJson;
    }
}
