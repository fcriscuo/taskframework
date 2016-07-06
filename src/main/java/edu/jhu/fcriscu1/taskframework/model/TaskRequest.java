package edu.jhu.fcriscu1.taskframework.model;

import lombok.ToString;
import lombok.extern.log4j.Log4j;

import java.time.Duration;
import java.time.Instant;

/**
 * Created by fcriscuo on 7/3/16.
 * Represents the time interval that a task should hold a shared resource
 * Also includes a timestamp for when a task was created
 */
@Log4j
@ToString
public class TaskRequest  {
    private final String taskId;
    // how long this task should utilize a resource in msecs
    private final Duration resourceDuration;
    private final Instant createdInstant;

    private TaskRequest(Builder builder){
        this.createdInstant = Instant.now();
        this.resourceDuration = builder.duration;
        this.taskId = builder.id;
    }

    public String getTaskId() {
        return taskId;
    }

    public Duration getResourceDuration() {
        return resourceDuration;
    }

    public Instant getCreatedInstant() {
        return createdInstant;
    }

    public static void main(String... args) {
        TaskRequest tr = new TaskRequest.Builder().id("Task001").duration(Duration.ofMinutes(1L)).build();
        log.info("TaskRequest " +tr.getTaskId() +" created at " +tr.getCreatedInstant().toEpochMilli()/1000L +" seconds");
    }


    public static class Builder {
        private String id;
        private Duration duration;


        public Builder(){}
        public Builder id(String id){
            this.id = id;
            return this;
        }
        public Builder duration(Duration duration){
            this.duration = duration;
            return this;
        }

        public TaskRequest build() {
            return new TaskRequest(this);
        }

    }
}
