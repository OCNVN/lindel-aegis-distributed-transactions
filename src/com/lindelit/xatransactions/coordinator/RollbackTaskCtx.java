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
class RollbackTaskCtx{
    public XATransactionsBuilder.WorkerScheduleConfiguration wsc;
    public byte[] rollbackData;
    // Debe coincidir con transaction en RollbackTransactionCtx
    public String transaction;

    public RollbackTaskCtx(XATransactionsBuilder.WorkerScheduleConfiguration wsc, String transaction, byte[] rollbackData) {
        this.wsc = wsc;
        this.rollbackData = rollbackData;
        this.transaction = transaction;
    }

    public RollbackTaskCtx() {
    }
}
