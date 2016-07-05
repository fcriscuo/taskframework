package edu.jhu.fcriscu1.taskframework.app;

import com.google.common.collect.Lists;
import edu.jhu.fcriscu1.taskframework.process.DataQualityProducer;
import edu.jhu.fcriscu1.taskframework.process.TaskMessageConsumer;
import edu.jhu.fcriscu1.taskframework.process.TaskProcessor;
import edu.jhu.fcriscu1.taskframework.service.PropertiesService;
import edu.jhu.fcriscu1.taskframework.service.TaskQueueService;
import edu.jhu.fcriscu1.taskframework.simulation.TaskRequestProducer;
import lombok.extern.log4j.Log4j;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by fcriscuo on 7/4/16.
 * Application to drive a simulated workflow
 */
@Log4j
public class TaskWorkflowApp {
    // these values serve as backup for missing properties file entries
    private static final Integer NUM_TASK_PROCESSORS = 4;
    private static final Integer NUM_TASK_REQUESTS = 500;
    private static final Long MIN_TASK_PROCESSING_DURATION = 250L;
    private static final Long MAX_TASK_PROCESSING_DURATION = 1200L;
    private static final String DEFAULT_OUTPUT_FILENAME = "/tmp/testframework/message.txt";
    private List<Thread> taskProcessorThreadList;
    private CountDownLatch taskLatch = new CountDownLatch(2);
    private  TaskMessageConsumer messageConsumer;

    public TaskWorkflowApp() {
        this.initializeWorkflowServices();
        this.simulateWorkflow();
        this.shutdownWorkflowServices();
    }

    public static void main(String... args) {
        TaskWorkflowApp app = new TaskWorkflowApp();
        log.info("FINIS....");
    }

    private void simulateWorkflow() {
        Runnable intervalProcessorTask = this.initializeIntervalProcessors();
        Runnable requestProcessorTask = this.initializeTaskRequestProducer();
        // start these threads
        new Thread(intervalProcessorTask).start();
        new Thread(requestProcessorTask).start();
        // wait for the two task threads to complete
        try {
            Long waitTime = PropertiesService.INSTANCE
                    .getLongPropertyByName("global.default.max.app.latch.wait.time.minutes")
                    .orElse(60L);
            log.info("Main countdown latch wait time  = " +waitTime +" minutes");
            taskLatch.await(waitTime,TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }

    }

    private Runnable initializeTaskRequestProducer() {
        return () -> {
            try {
                Integer taskRequestCount = PropertiesService.INSTANCE
                        .getIntegerPropertyByName("orphan.default.number.task.requests")
                        .orElse(NUM_TASK_REQUESTS);
                Long minProcessingDuration = PropertiesService.INSTANCE
                        .getLongPropertyByName("orphan.default.task.min.processing.duration")
                        .orElse(MIN_TASK_PROCESSING_DURATION);
                Long maxProcessingDuration = PropertiesService.INSTANCE
                        .getLongPropertyByName("orphan.default.task.max.processing.duration=1200")
                        .orElse(MAX_TASK_PROCESSING_DURATION);
                new TaskRequestProducer.Builder().requestCount(taskRequestCount)
                        .minProcessingDuration(minProcessingDuration)
                        .maxProcessingDuration(maxProcessingDuration).build()
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
            }
            taskLatch.countDown();
        };
    }

    private Runnable initializeIntervalProcessors(){
     return () -> {
            Integer numQAThreads = PropertiesService.INSTANCE.getIntegerPropertyByName("qa.default.num.threads")
                    .orElse(4);
            CountDownLatch latch = new CountDownLatch(numQAThreads);
        // generate a List of DataQualityProducer instances to run on distinct threads
         List<Runnable> runList =Lists.newArrayList();
         IntStream.range(1,numQAThreads+1).forEach((i)->
           runList.add(new DataQualityProducer.Builder()
                   .intervalCount(PropertiesService.INSTANCE.getIntegerPropertyByName("qa.default.num.intervals").get())
                   .latch(latch)
                   .minProcessingDuration(PropertiesService.INSTANCE.getLongPropertyByName("qa.default.min.processing.duration").get())
                   .maxProcessingDuration(PropertiesService.INSTANCE.getLongPropertyByName("qa.default.max.processing.duration").get())
                   .build()));

            // start the threads
            final List<Thread> threads = runList
                    .stream()
                    .map(runnable -> new Thread(runnable))
                    .peek(Thread::start)
                    .collect(Collectors.toList());
            try {
                Long latchWaitTime = PropertiesService.INSTANCE
                        .getLongPropertyByName("global.default.max.internal.latch.wait.time.minutes")
                        .orElse(30L);
                latch.await(latchWaitTime, TimeUnit.MINUTES);
                log.info("Time limit reached");
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally{
                threads.forEach(Thread::interrupt);
                taskLatch.countDown();
            }
        };
    }

    private void initializeWorkflowServices() {
        Integer numTaskProcessors = PropertiesService.INSTANCE.getIntegerPropertyByName("orphan.default.number.task.processors")
                .orElse(NUM_TASK_PROCESSORS);
        this.taskProcessorThreadList = this.initializeTaskProcessors(numTaskProcessors);
        this.messageConsumer = new TaskMessageConsumer(this.resolveMessageConsumerOutputPath());
    }

    private Path resolveMessageConsumerOutputPath() {
        String fileName = PropertiesService.INSTANCE.getStringPropertyByName("message.consumer.output.path")
                .orElse(DEFAULT_OUTPUT_FILENAME);
        log.info("Message output file: " + fileName);
        return Paths.get(fileName);
    }

    private void shutdownWorkflowServices(){
        this.taskProcessorThreadList.forEach(Thread::interrupt);
        this.messageConsumer.shutdown();
    }

    private List<Thread> initializeTaskProcessors(Integer nProcessors){
        // instantiate a List of TaskProcessors
        List<Runnable> processorList = Lists.newArrayList();
        IntStream.range(1,nProcessors).forEach((i) ->
         processorList.add(new TaskProcessor()));
        // start the threads
       return processorList
                .stream()
                .map(runnable -> new Thread(runnable))
                .peek(Thread::start)
                .collect(Collectors.toList());
    }
}
