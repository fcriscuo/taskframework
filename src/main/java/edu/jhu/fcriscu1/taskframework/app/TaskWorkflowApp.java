package edu.jhu.fcriscu1.taskframework.app;

import com.google.common.collect.Lists;
import edu.jhu.fcriscu1.taskframework.process.DataQualityProducer;
import edu.jhu.fcriscu1.taskframework.process.TaskMessageConsumer;
import edu.jhu.fcriscu1.taskframework.process.TaskProcessor;
import edu.jhu.fcriscu1.taskframework.service.TaskQueueService;
import edu.jhu.fcriscu1.taskframework.simulation.TaskRequestProducer;
import lombok.extern.log4j.Log4j;

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
    private static final Integer NUM_TASK_PROCESSORS = 4;
    private static final Integer NUM_TASK_REQUESTS = 500;
    private static final Long MIN_TASK_PROCESSING_DURATION = 250L;
    private static final Long MAX_TASK_PROCESSING_DURATION = 1200L;
    private List<Thread> taskProcessorThreadList;
    private CountDownLatch taskLatch = new CountDownLatch(2);

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
            taskLatch.await(5L,TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }

    }

    private Runnable initializeTaskRequestProducer() {
        return () -> {
            try {
                new TaskRequestProducer.Builder().requestCount(NUM_TASK_REQUESTS)
                        .minProcessingDuration(MIN_TASK_PROCESSING_DURATION)
                        .maxProcessingDuration(MAX_TASK_PROCESSING_DURATION).build()
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
            CountDownLatch latch = new CountDownLatch(3);
            List<Runnable> runList = Arrays.asList(new DataQualityProducer.Builder().intervalCount(100)
                            .latch(latch)
                            .minProcessingDuration(300L).maxProcessingDuration(1200L).build(),
                    new DataQualityProducer.Builder().intervalCount(400).minProcessingDuration(100L)
                            .latch(latch).maxProcessingDuration(500L).build(),
                    new DataQualityProducer.Builder().intervalCount(1000).minProcessingDuration(200L)
                            .latch(latch).maxProcessingDuration(800L).build());
            // start the threads
            final List<Thread> threads = runList
                    .stream()
                    .map(runnable -> new Thread(runnable))
                    .peek(Thread::start)
                    .collect(Collectors.toList());
            try {
                latch.await(10L, TimeUnit.MINUTES);
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
        this.taskProcessorThreadList = this.initializeTaskProcessors(NUM_TASK_PROCESSORS);
        TaskMessageConsumer messageConsumer = new TaskMessageConsumer();
    }

    private void shutdownWorkflowServices(){

        this.taskProcessorThreadList.forEach(Thread::interrupt);
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
