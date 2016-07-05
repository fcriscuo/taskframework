package edu.jhu.fcriscu1.taskframework.service;

import edu.jhu.fcriscu1.taskframework.model.DatabaseResource;
import edu.jhu.fcriscu1.taskframework.model.TaskMessage;
import edu.jhu.fcriscu1.taskframework.model.TaskRequest;

/**
 * Created by fcriscuo on 7/4/16.
 * Responsible for providing global access to a shared DatabaseResource instance
 */
public enum DatabaseService {
    INSTANCE;
    private final Integer nConnections = 10;
    private DatabaseResource databaseResource = new DatabaseResource(nConnections);

    public TaskMessage completeDatabaseOperation(TaskRequest taskRequest){
        return this.databaseResource.processTask(taskRequest);
    }


}
