package edu.jhu.fcriscu1.taskframework.process;

import edu.jhu.fcriscu1.taskframework.datastructure.TaskMessageQueue;
import edu.jhu.fcriscu1.taskframework.service.DatabaseService;
import edu.jhu.fcriscu1.taskframework.service.PropertiesService;
import edu.jhu.fcriscu1.taskframework.service.TaskQueueService;
import edu.jhu.fcriscu1.taskframework.model.TaskMessage;
import edu.jhu.fcriscu1.taskframework.model.TaskRequest;
import edu.jhu.fcriscu1.taskframework.simulation.TaskRequestProducer;
import lombok.extern.log4j.Log4j;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created by fcriscuo on 7/4/16.
 * Represents a long running task. Executes on its own thread.
 * Takes TaskRequests off the shared input queue, sleeps for the interval
 * specified in the request and then issues a TaskMessage
 */
@Log4j
public class TaskProcessor implements Runnable {
    private static final Long queueTimeout = PropertiesService.INSTANCE
            .getLongPropertyByName("orphan.default.task.queue.wait.time.limit=200")
            .orElse(1000L);

    @Override
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()){
               this.processTaskRequest( TaskQueueService.INSTANCE.taskRequestQueue()
                       .poll(queueTimeout,TimeUnit.MILLISECONDS))
                       .ifPresent((message) -> {
                           // put the TaskMessage onto the global TaskMessage queue
                           try {
                               if(TaskMessageQueue.INSTANCE.getTaskMessageQueue().offer(message,queueTimeout,TimeUnit.MILLISECONDS)){
                                   log.info("Processing completed for task: " +message.getTaskRequest().getTaskId() +" in "
                                           +message.resolveProcessingDuration().toMillis() +" milliseconds by thread " +Thread.currentThread().getName());
                                   log.info("Task queue size: " +TaskQueueService.INSTANCE.taskRequestQueue().size());
                               } else {
                                   log.error("ERROR: failed to put processing message for task " +message.getTaskRequest().getTaskId()
                                           +" onto  TaskMessage queue");
                               }
                           } catch (InterruptedException e) {
                               e.printStackTrace();
                           }
                       });
            }

        } catch (InterruptedException e) {
            log.info(Thread.currentThread().getName() +" interrupted");
        }
    }
    /*
     Private method to process the TaskRequest and generate a TaskMessage
     */
    private Optional<TaskMessage> processTaskRequest(TaskRequest taskRequest){
        if( taskRequest != null){
            return Optional.of(DatabaseService.INSTANCE.completeDatabaseOperation(taskRequest));
        }
        return Optional.empty();

    }
    // main method for stand alone testing
    public static void main(String... args) {
        // instantiate a List of TaskProcessors
        final List<Runnable> processorList = Arrays.asList(
                new TaskProcessor(),
               new TaskProcessor(),
                new TaskProcessor(),
                new TaskProcessor()
        );
        // start the threads
        final List<Thread> threads = processorList
                .stream()
                .map(runnable -> new Thread(runnable))
                .peek(Thread::start)
                .collect(Collectors.toList());
        // generate a Stream of TaskRequests
        try {
            new TaskRequestProducer.Builder().requestCount(100)
                     .minProcessingDuration(250L).maxProcessingDuration(1000L).build()
                    .generateTaskRequestStream().forEach((tr) -> {
                try {
                    TaskQueueService.INSTANCE.taskRequestQueue().put(tr);
                } catch (InterruptedException e) {
                  log.error(e.getMessage());
                }
            });
            // sleep until processing end
            while(!TaskQueueService.INSTANCE.taskRequestQueue().isEmpty()){
                TimeUnit.SECONDS.sleep(10L);
            }
            log.info("Task queue is empty");
        } catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        } finally {
            threads.forEach(Thread::interrupt);
        }
    }
}
