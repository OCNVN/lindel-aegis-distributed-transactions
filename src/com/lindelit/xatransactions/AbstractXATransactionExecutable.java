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
    public abstract byte[] execute(byte[] data);
    public abstract byte[] rollback(byte[] data);
    public abstract byte[] validate(byte[] data);

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }
    
    public JSONObject parseData() throws ParseException{
        JSONParser parser=new JSONParser();

        String dataString = new String(data);
        Object dataObject = parser.parse(dataString);
        JSONObject dataJson = (JSONObject) dataObject;
        
        return dataJson;
    }
}
