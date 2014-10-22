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
class TaskAssignmentCtx{
    XATransactionsBuilder.WorkerScheduleConfiguration wsc;
    XATransactionsBuilder.WorkerScheduleConfiguration nextWsc;
    public String task;
    public byte[] data;
    String designatedWorker;

    public TaskAssignmentCtx(XATransactionsBuilder.WorkerScheduleConfiguration wsc, XATransactionsBuilder.WorkerScheduleConfiguration nextWsc, String designatedWorker, String task, byte[] data) {
        this.wsc = wsc;
        this.nextWsc = nextWsc;
        this.task = task;
        this.data = data;
        this.designatedWorker = designatedWorker;
    }
}