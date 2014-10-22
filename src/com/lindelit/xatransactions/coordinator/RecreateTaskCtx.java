/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lindelit.xatransactions.coordinator;

import com.lindelit.xatransactions.XATransactionsBuilder;

/**
 *
 * @author carloslucero
 */
class RecreateTaskCtx {
    String path; 
    String task;
    byte[] data;
    XATransactionsBuilder.WorkerScheduleConfiguration wsc;
        
    RecreateTaskCtx(String path, String task, byte[] data, XATransactionsBuilder.WorkerScheduleConfiguration wsc) {
        this.path = path;
        this.task = task;
        this.data = data;
        this.wsc = wsc;
    }
}
