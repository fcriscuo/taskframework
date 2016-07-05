package edu.jhu.fcriscu1.taskframework.process;

import edu.jhu.fcriscu1.taskframework.datastructure.TaskMessageQueue;
import edu.jhu.fcriscu1.taskframework.model.TaskMessage;
import lombok.extern.log4j.Log4j;
import rx.functions.Action1;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Created by fcriscuo on 7/4/16.
 * Responsible for subscribing to TaskMessage objects produced by the TaskMessageQueue
 */
@Log4j
public class TaskMessageConsumer {

    private  FileWriter fw;
    public TaskMessageConsumer(Path messageFilePath){
        try {
            fw = (messageFilePath !=null)?
                    new FileWriter(messageFilePath.toFile())
                    :new FileWriter("/tmp/taskframework_log.txt");
        } catch (IOException e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
        TaskMessageQueue.INSTANCE.getTaskMessageQueue().observe()
                .forEach(new Action1<TaskMessage>() {
                    @Override
                    public void call(TaskMessage taskMessage) {
                        try {
                                fw.write(taskMessage.generateReport());
                            } catch (IOException e) {
                               // output to log as backup
                            log.info(taskMessage.generateReport());
                            log.error(e.getMessage());
                            }
                        }


                });
    }


    // need to close the output file
    public void shutdown(){
        if(this.fw != null){
            try {
                fw.close();
            } catch (IOException e) {
                log.error(e.getMessage());
            }
        }
    }


}
