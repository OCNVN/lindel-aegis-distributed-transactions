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
class TransactionAssignmentCtx{
    public XATransactionsBuilder.WorkerScheduleConfiguration wsc;
    public XATransactionsBuilder.WorkerScheduleConfiguration nextWsc;
    public String designatedWorker;
    public String transaction;
    public String clientId;
    public byte[] data;

    public TransactionAssignmentCtx(XATransactionsBuilder.WorkerScheduleConfiguration wsc, XATransactionsBuilder.WorkerScheduleConfiguration nextWsc, String designatedWorker, String transaction, String clientId, byte[] data) {
        this.wsc = wsc;
        this.nextWsc = nextWsc;
        this.designatedWorker = designatedWorker;
        this.transaction = transaction;
        this.clientId = clientId;
        this.data = data;
    }
}
