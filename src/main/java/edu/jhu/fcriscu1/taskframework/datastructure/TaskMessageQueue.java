package edu.jhu.fcriscu1.taskframework.datastructure;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import edu.jhu.fcriscu1.taskframework.model.TaskMessage;
import lombok.extern.log4j.Log4j;

/**
 * Created by fcriscuo on 7/4/16.
 * Represents a global instance of an ObservableQueue responsible for
 * queuing TaskMessages from a variety of producers
 */
@Log4j
public enum TaskMessageQueue {
    INSTANCE;
    private ObservableQueue<TaskMessage> taskMessageQueue = Suppliers.memoize(new TaskMessageQueueSupplier()).get();

    public ObservableQueue<TaskMessage> getTaskMessageQueue() { return this.taskMessageQueue;}


    private class TaskMessageQueueSupplier implements Supplier<ObservableQueue<TaskMessage>>{
        private ObservableQueue<TaskMessage> taskMessageQueue;
        private TaskMessageQueueSupplier() {
            this.taskMessageQueue = new ObservableQueue<>();
        }

        @Override
        public ObservableQueue<TaskMessage> get() {
            return this.taskMessageQueue;
        }
    }


}
