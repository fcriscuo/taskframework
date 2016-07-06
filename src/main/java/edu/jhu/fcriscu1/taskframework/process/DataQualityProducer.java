package edu.jhu.fcriscu1.taskframework.process;

import edu.jhu.fcriscu1.taskframework.datastructure.TaskMessageQueue;
import edu.jhu.fcriscu1.taskframework.model.TaskMessage;
import edu.jhu.fcriscu1.taskframework.model.TaskRequest;
import edu.jhu.fcriscu1.taskframework.service.DatabaseService;
import edu.jhu.fcriscu1.taskframework.service.PropertiesService;
import lombok.extern.log4j.Log4j;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Created by fcriscuo on 7/4/16.
 * Responsible for generating data quality tasks and messages at random intervals
 */
@Log4j
public class DataQualityProducer implements Runnable {
    private static final Integer DEFAULT_INTERVAL = 2000;
    private static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;
    private static final String TASK_ID_PREFIX = "DataQualityTask_";
    private Integer inervalCount;
    private Long minProcessingDuration;
    private Long maxProcessingDuration;
    private CountDownLatch latch;
    private final Random random = new Random();
    private static final AtomicInteger COUNT = new AtomicInteger(0);
    private static final Integer MAX_INTERVAL_DELTA = 1000;
    private static final Long DEFAULT_MIN_INTERVAL = 1000L;
    private static final Long DEFAULT_MAX_QUEUE_WAIT_TIME = 2000L;

    private DataQualityProducer(Builder builder) {
        this.inervalCount = builder.inervalCount;
        this.minProcessingDuration = builder.minProcessingDuration;
        this.maxProcessingDuration = builder.maxProcessingDuration;
        this.latch = builder.latch;
    }


    private void generateAndProcessTaskRequest() {
        Long queueWaitTime = PropertiesService.INSTANCE.getLongPropertyByName("global.default.max.queue.wait.time")
                .orElse(DEFAULT_MAX_QUEUE_WAIT_TIME);
        random.longs(1, minProcessingDuration, maxProcessingDuration + 1)
                .mapToObj((dur) -> {
                            COUNT.incrementAndGet();
                            return new TaskRequest.Builder().duration(Duration.ofMillis(dur)).id(TASK_ID_PREFIX + COUNT).build();
                        }
                ).findFirst().ifPresent((request) -> {
            TaskMessage message = DatabaseService.INSTANCE.completeDatabaseOperation(request);
            log.info("Task: " + message.getTaskRequest().getTaskId() + "completed in " + message.resolveProcessingDuration().toMillis() + " milliseconds");
            try {

                TaskMessageQueue.INSTANCE.getTaskMessageQueue().offer(message, queueWaitTime, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

    }

    private Supplier<Long> intervalGenerator = () ->
            DEFAULT_MIN_INTERVAL + random.nextInt(MAX_INTERVAL_DELTA);


    @Override
    public void run() {
        log.info("Thread " + Thread.currentThread().getName() + " invoked");
        ExecutorService executor = Executors.newFixedThreadPool(4);

        while (!Thread.currentThread().isInterrupted() && inervalCount > 0) {
            this.generateAndProcessTaskRequest();
            this.inervalCount--;
            try {
                TimeUnit.MILLISECONDS.sleep(intervalGenerator.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        log.info("Thread " + Thread.currentThread().getName() + " completed");
        latch.countDown();
    }

    public static class Builder {
        private Integer inervalCount;
        private Long minProcessingDuration;
        private Long maxProcessingDuration;
        private CountDownLatch latch;

        public Builder intervalCount(Integer count) {
            this.inervalCount = count;
            return this;
        }

        public Builder minProcessingDuration(Long duration) {
            this.minProcessingDuration = duration;
            return this;
        }

        public Builder maxProcessingDuration(Long duration) {
            this.maxProcessingDuration = duration;
            return this;
        }

        public Builder latch(CountDownLatch latch) {
            this.latch = latch;
            return this;
        }

        public DataQualityProducer build() {
            return new DataQualityProducer(this);
        }
    }

    // main method for standalone testing
    public static void main(String... args) {
        CountDownLatch latch = new CountDownLatch(3);
        // start the Message consumer
        TaskMessageConsumer tmc = new TaskMessageConsumer(Paths.get("/tmp/data_quality_test_log.txt"));
        List<Runnable> runList = Arrays.asList(new DataQualityProducer.Builder().intervalCount(25)
                        .latch(latch)
                        .minProcessingDuration(300L).maxProcessingDuration(1200L).build(),
                new Builder().intervalCount(40).minProcessingDuration(100L)
                        .latch(latch).maxProcessingDuration(500L).build(),
                new DataQualityProducer.Builder().intervalCount(10).minProcessingDuration(200L)
                        .latch(latch).maxProcessingDuration(800L).build());
        // start the threads
        final List<Thread> threads = runList
                .stream()
                .map(runnable -> new Thread(runnable))
                .peek(Thread::start)
                .collect(Collectors.toList());
        try {
            latch.await(60L, TimeUnit.MINUTES);
            log.info(" DataQualityProducer stand alone wait time limit reached");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            threads.forEach(Thread::interrupt);
            tmc.shutdown();
        }

    }
}
