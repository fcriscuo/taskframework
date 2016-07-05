package edu.jhu.fcriscu1.taskframework.process;

import edu.jhu.fcriscu1.taskframework.datastructure.TaskMessageQueue;
import edu.jhu.fcriscu1.taskframework.model.TaskMessage;
import lombok.extern.log4j.Log4j;
import rx.functions.Action1;

/**
 * Created by fcriscuo on 7/4/16.
 * Responsible for subscribing to TaskMessage objects produced by the TaskMessageQueue
 */
@Log4j
public class TaskMessageConsumer {
    public TaskMessageConsumer(){
        log.info("TaskMessageConsumer started");
        TaskMessageQueue.INSTANCE.getTaskMessageQueue().observe()
                .forEach(new Action1<TaskMessage>() {
                    @Override
                    public void call(TaskMessage taskMessage) {
                        log.info(taskMessage.generateReport());
                    }
                });
    }



}
