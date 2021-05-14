/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.jraft.rhea.metadata;

import java.util.List;

public class RebuildStoreTaskMetaData extends ScheduleTaskMetadata {

    private static final long       serialVersionUID = -6822059115806648547L;
    private Long                    fromStoreId;
    private Long                    toStoreId;
    private Store                   fromStoreMeta;
    private int                     taskStatusCode;
    private ResetStoreSubTask       resetStoreSubTask;
    private List<ChangePeerSubTask> changePeerSubTasks;

    public Store getFromStoreMeta() {
        return fromStoreMeta;
    }

    public void setFromStoreMeta(final Store fromStoreMeta) {
        this.fromStoreMeta = fromStoreMeta;
    }

    public Long getFromStoreId() {
        return fromStoreId;
    }

    public void setFromStoreId(final Long fromStoreId) {
        this.fromStoreId = fromStoreId;
    }

    public Long getToStoreId() {
        return toStoreId;
    }

    public void setToStoreId(final Long toStoreId) {
        this.toStoreId = toStoreId;
    }

    public int getTaskStatusCode() {
        return taskStatusCode;
    }

    public void setTaskStatusCode(final int taskStatusCode) {
        this.taskStatusCode = taskStatusCode;
    }

    public List<ChangePeerSubTask> getChangePeerSubTasks() {
        return changePeerSubTasks;
    }

    public void setChangePeerSubTasks(final List<ChangePeerSubTask> changePeerSubTasks) {
        this.changePeerSubTasks = changePeerSubTasks;
    }

    public ResetStoreSubTask getResetStoreSubTask() {
        return resetStoreSubTask;
    }

    public void setResetStoreSubTask(final ResetStoreSubTask resetStoreSubTask) {
        this.resetStoreSubTask = resetStoreSubTask;
    }

    @Override
    public String toString() {
        return "RebuildStoreTaskMetaData{" + "taskId='" + getTaskId() + '\'' + ", fromStoreId=" + fromStoreId
               + ", toStoreId=" + toStoreId + ", taskStatusCode=" + taskStatusCode + '}';
    }

    public enum TaskStatus {
        INIT(0), RESET_STORE(1), PREPARE_CHANGE_PEER(2), CHANGE_PEER(3), FINISHED(4), ABORT(5);

        private final int code;

        TaskStatus(int code) {
            this.code = code;
        }

        public static TaskStatus codeOf(int code) {
            for (TaskStatus status : values()) {
                if (status.getCode() == code) {
                    return status;
                }
            }
            throw new RuntimeException("Invalid status code");
        }

        public int getCode() {
            return code;
        }
    }

    public static class ResetStoreSubTask {

        public static final String INIT_STATE        = "INIT";
        public static final String RESET_STATE       = "RESET";
        public static final String WAIT_UPDATE_STATE = "WAIT_UPDATE";
        public static final String FINISH_STATE      = "FINISH";
        private String             taskState;                        // INIT -> RESET -> WAIT_UPDATE -> FINISH

        public ResetStoreSubTask(String taskState) {
            this.taskState = taskState;
        }

        public String getTaskState() {
            return taskState;
        }

        public void setTaskState(final String taskState) {
            this.taskState = taskState;
        }

    }

}
