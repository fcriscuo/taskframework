package edu.jhu.fcriscu1.taskframework.model;

import lombok.ToString;
import lombok.extern.log4j.Log4j;

import java.time.Duration;
import java.time.Instant;

/**
 * Created by fcriscuo on 7/3/16.
 * Represents how long a TaskRequest waited before being processed and
 * how long the actual processing required
 */
@Log4j
@ToString
public class TaskMessage {
    private final TaskRequest taskRequest;
    private Instant processingCompleteInstant;
    private Instant processingStartedInstant;
    private String message;

    public TaskMessage( TaskRequest request) {
        this.taskRequest = request;
    }

    /*
    The queue duration is defined as the difference from when the task was
    created and place on the queue until it was selected by one of
    the task processors. It includes the time when this TaskRequest was blocked from
    the queue
     */
    public Duration resolveQueueDuration(){
       return Duration.between(this.getProcessingStartedInstant() ,getTaskRequest().getCreatedInstant());
    }

    public Duration resolveTotalDuration() {
        return Duration.between(getTaskRequest().getCreatedInstant(),this.getProcessingCompleteInstant() );
    }


    public Duration resolveProcessingDuration() {
        return Duration.between( this.getProcessingStartedInstant(),this.getProcessingCompleteInstant());
    }

    public TaskRequest getTaskRequest() { return this.taskRequest;}


    public Instant getProcessingCompleteInstant() {
        return processingCompleteInstant;
    }

    public void setProcessingCompleteInstant(Instant processingCompleteInstant) {
        this.processingCompleteInstant = processingCompleteInstant;
    }

    public Instant getProcessingStartedInstant() {
        return processingStartedInstant;
    }

    public void setProcessingStartedInstant(Instant processingStartedInstant) {
        this.processingStartedInstant = processingStartedInstant;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String generateReport() {
        return new StringBuilder("Task ID ")
                .append(this.getTaskRequest().getTaskId())
                .append(" Queue time: ")
                .append(this.resolveQueueDuration().toMillis())
                .append(" msecs   Processing time: ")
                .append(this.resolveProcessingDuration().toMillis())
                .append(" msecs   Total time: ")
                .append(this.resolveTotalDuration().toMillis())
                .append(" msecs")
                .append("\nMessage: ")
                .append(this.getMessage()).toString();
    }
}
