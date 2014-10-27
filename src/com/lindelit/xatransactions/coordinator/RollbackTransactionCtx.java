/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lindelit.xatransactions.coordinator;

import com.lindelit.xatransactions.XATransactionsBuilder;
import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * @author carloslucero
 */
class RollbackTransactionCtx {
    public Integer failedWorkerScheduleIndex;
    public String failedWorkerSchedule;
    public String transaction;
    // HashMap formado por nombre de worker-schedule como clave, y de valor la tarea a hacer rollback
    private HashMap<String, RollbackTaskCtx> rollbackTasks;
    // Lista de los nombres de worker-schedule involucrados en el rollback de la transaccion
    private ArrayList<String> rollbackWorkerSchedules;

    public RollbackTransactionCtx(Integer failedWorkerScheduleIndex, String failedWorkerSchedule) {
        this.failedWorkerScheduleIndex = failedWorkerScheduleIndex;
        this.failedWorkerSchedule = failedWorkerSchedule;
        
        this.rollbackTasks = new HashMap<>();
        this.rollbackWorkerSchedules = new ArrayList<>();
    }

    public RollbackTransactionCtx() {
        this.rollbackTasks = new HashMap<>();
        this.rollbackWorkerSchedules = new ArrayList<>();
    }
    
    // Agregar tarea miembro del rollback de la transaccion
    public void addRollbackTask(XATransactionsBuilder.WorkerScheduleConfiguration wsc, byte[] rollbackData){
        // No pueden haber mas de una tarea miembro del rollback con un mismo worker-schedule
        if(!rollbackTasks.containsKey(wsc.getName())){
            rollbackWorkerSchedules.add(wsc.getName());

            RollbackTaskCtx rollCtx = new RollbackTaskCtx(wsc, this.transaction, rollbackData);
            rollbackTasks.put(wsc.getName(), rollCtx);
        }
    }
    
    public void addRollbackTask(RollbackTaskCtx rollTaskCtx){
        // No pueden haber mas de una tarea miembro del rollback con un mismo worker-schedule
        if(!rollbackTasks.containsKey(rollTaskCtx.wsc.getName())){
            rollbackWorkerSchedules.add(rollTaskCtx.wsc.getName());
            
            rollbackTasks.put(rollTaskCtx.wsc.getName(), rollTaskCtx);
        }
    }

    public HashMap<String, RollbackTaskCtx> getRollbackTasks() {
        return rollbackTasks;
    }
    
    
    // Si todos los contextos de tareas necearios para hacer el rollback estan disponibles
    // para proceder con el rollback
    public Boolean isReady(){
        Boolean flag = false;
        if(rollbackTasks.size() == failedWorkerScheduleIndex + 1)
            return true;
        
        return flag;
    }
}
