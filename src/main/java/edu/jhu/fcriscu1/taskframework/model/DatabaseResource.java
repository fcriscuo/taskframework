package edu.jhu.fcriscu1.taskframework.model;

import lombok.extern.log4j.Log4j;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalUnit;
import java.util.OptionalInt;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;

/**
 * Created by fcriscuo on 7/3/16.
 * Represents a shared resource that can only be accessed concurrently by
 * a fixed number of clients
 * Intended to represent a limited number of connections to a database
 */

@Log4j
public class DatabaseResource {
    //TODO: move constants to a properties file for easier reconfiguration
    private final Integer DEFAULT_CONNECTIONS = 5;
    private final Semaphore semaphore;
    private final Lock connectionLock;

    private Boolean freeConnections[];
    public DatabaseResource(Integer nConn) {
        Integer nConnections = (nConn>0) ? nConn: DEFAULT_CONNECTIONS;
        this.semaphore = new Semaphore(nConnections);
        this.freeConnections = new Boolean[nConnections];
        for(int i = 0; i<nConnections; i++) {
            freeConnections[i] = true;
        }
        this.connectionLock = new ReentrantLock();
    }

    public TaskMessage processTask(TaskRequest taskRequest){
        TaskMessage message = new TaskMessage(taskRequest);
        // set the start Instant for this task
        message.setProcessingStartedInstant(Instant.now());
        // decrease semaphore count
        try {
            //semaphore.acquire();
            if(!semaphore.tryAcquire(2000L,TimeUnit.MILLISECONDS) ) {
                message.setMessage("ERROR: " +taskRequest.getTaskId() +" unable to acquire database connection");
                log.error("Failed to obtain connection for  "+taskRequest.getTaskId());
                return message;
            }
            int assignedConnection = this.getConnection();
            if(assignedConnection >=0 ){
                // process the request - sleep for the requested duration
                TimeUnit.MILLISECONDS.sleep(taskRequest.getResourceDuration().toMillis());
                // set the completion Instant for this task
                message.setProcessingCompleteInstant(Instant.now());
                message.setMessage("Task: " +taskRequest.getTaskId() +" completed in " +message.resolveTotalDuration().toMillis() +" milliseconds");
                this.releaseConnection(assignedConnection);
            } else {
                message.setMessage("ERROR: " +taskRequest.getTaskId() +" unable to acquire database connection");
            }

        } catch (InterruptedException e) {
            message.setMessage("EXCEPTION: " +taskRequest.getTaskId() +": " +e.getMessage());
            e.printStackTrace();
        } finally {
            semaphore.release();
            return message;
        }

    }

    // private method to reserve a connection
    private int getConnection() {
        try {
            boolean found = false;
            connectionLock.lock();  // single threaded access to lock
            OptionalInt iOpt = IntStream.range(0,freeConnections.length-1).filter((i) ->freeConnections[i]).findFirst();
            if(iOpt.isPresent()){
                freeConnections[iOpt.getAsInt()] = false;
                return iOpt.getAsInt();
            }
            return -1;
        } finally {
            connectionLock.unlock();
        }
    }

    private void releaseConnection(int i) {
        try {
            connectionLock.lock();
            // free the specified connection
            freeConnections[i] = true;
        } finally {
            connectionLock.unlock();
        }

    }
    // main class for standalone testing
    public static void main(String... args) {
        // define a TaskRequest
        TaskRequest tr = new TaskRequest.Builder().duration(Duration.ofMillis(2000L))
                .id("Task001").build();
        DatabaseResource dr = new DatabaseResource(10);
        TaskMessage tm = dr.processTask(tr);
        log.info(tm.getMessage());
    }


}
