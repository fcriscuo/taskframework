package edu.jhu.fcriscu1.taskframework.simulation;

import edu.jhu.fcriscu1.taskframework.model.TaskRequest;
import lombok.extern.log4j.Log4j;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Created by fcriscuo on 7/4/16.
 * Responsible for producing a Stream of TaskRequest objects with
 * processing times within a specified range
 */
@Log4j
public class TaskRequestProducer {
    private final Integer requestCount;
    private final Long minProcessingDuration;
    private final Long maxProcessingDuration;


    private final String taskIdPrefix = "Task_";

    private TaskRequestProducer(Builder builder ){
        this.requestCount = builder.requestCount;
        this.minProcessingDuration = builder.minProcessingDuration;
        this.maxProcessingDuration = builder.maxProcessingDuration;
    }

    public Stream<TaskRequest> generateTaskRequestStream(){
        Random r = new Random();
        AtomicInteger count = new AtomicInteger(0);
        return r.longs(this.requestCount, this.minProcessingDuration,this.maxProcessingDuration+1)
                .mapToObj((dur) -> {
                        count.incrementAndGet();
                        return new TaskRequest.Builder().duration(Duration.ofMillis(dur)).id("SQL_Task_"+count).build();
                        }
                );

    }

    public static class Builder{
        private Integer requestCount;
        private Long minProcessingDuration;
        private Long maxProcessingDuration;
        public Builder() {}

        public Builder requestCount(Integer count){
            this.requestCount = count;
            return this;
        }

        public Builder minProcessingDuration(Long duration){
            this.minProcessingDuration = duration;
            return this;
        }
        public Builder maxProcessingDuration(Long duration){
            this.maxProcessingDuration = duration;
            return this;
        }

        public TaskRequestProducer build() {
            return new TaskRequestProducer(this);
        }

    }
    // main method for stand alone testing
    public static void main(String... args) {
        new TaskRequestProducer.Builder().requestCount(100)
                .minProcessingDuration(200L).maxProcessingDuration(1000L).build()
                .generateTaskRequestStream().forEach((tr) ->
          log.info("Task id: " +tr.getTaskId() +" duration " +tr.getResourceDuration().toMillis() +" milliseconds"));
    }



}
