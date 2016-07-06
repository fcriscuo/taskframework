package edu.jhu.fcriscu1.taskframework.datastructure;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Queues;
import edu.jhu.fcriscu1.taskframework.model.TaskRequest;
import edu.jhu.fcriscu1.taskframework.service.PropertiesService;
import lombok.extern.log4j.Log4j;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by fcriscuo on 7/3/16.
 * Represents a service interface
 */
@Log4j
public enum TaskQueueService {
    INSTANCE;


    private  final Integer QUEUE_SIZE = 100;
    private BlockingQueue<TaskRequest> taskRequestQueue= Suppliers.memoize(new TaskQueueSupplier(QUEUE_SIZE)).get();

    //TODO: incorporate required queue functions directly rather than expose queue
    public BlockingQueue<TaskRequest> taskRequestQueue() { return this.taskRequestQueue;}

    private class TaskQueueSupplier implements Supplier<BlockingQueue<TaskRequest>> {
         Integer queueSize = PropertiesService.INSTANCE.getIntegerPropertyByName("orphan.default.task.queue.size")
                 .orElse(QUEUE_SIZE);
        private final ArrayBlockingQueue<TaskRequest> taskRequestQueue;

        TaskQueueSupplier(Integer queueSize) {
            this.taskRequestQueue= Queues.newArrayBlockingQueue(queueSize);
        }

        @Override
        public BlockingQueue<TaskRequest> get() {
            return this.taskRequestQueue;
        }
    }

    //main method for standalone testing
    public static void main(String... args) {
        Integer requestCount = 1000;
        Long waitTime = 1500L;
        CountDownLatch latch = new CountDownLatch(requestCount);
        Runnable dequeueThread = ()-> {
            while (latch.getCount()>0) {
                try {
                    TaskRequest tr= TaskQueueService.INSTANCE.taskRequestQueue().take();
                     Duration qTime = Duration.between(tr.getCreatedInstant(),Instant.now());
                    log.info("Dequeued task " +tr.getTaskId() +" queue time= " +qTime.toMillis() +" millisecconds");
                   latch.countDown();
               } catch (InterruptedException e) {
                   log.error(e.getMessage());
                   e.printStackTrace();
               }
            }
        };
        // dequeue task requests on sepaarte thread
        new Thread(dequeueThread).start();
        // create a List of identical TaskRequest objects
                IntStream.rangeClosed(1,requestCount).forEach((i) ->
               {
                    try {
                        TaskRequest tr = new TaskRequest.Builder().duration(Duration.ofMillis(1000L)).id("Task_"+i).build();
                        if(TaskQueueService.INSTANCE.taskRequestQueue().offer(tr,waitTime, TimeUnit.MILLISECONDS)){
                            log.info("Queued task " +tr.getTaskId());
                        } else {
                            latch.countDown();
                            log.error("Error failed to add TaskRequest " +tr.getTaskId() +" to queue");
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });
        // wait for countdown latch
        try {
            latch.await(4L,TimeUnit.MINUTES);
            log.info("FINIS....");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
