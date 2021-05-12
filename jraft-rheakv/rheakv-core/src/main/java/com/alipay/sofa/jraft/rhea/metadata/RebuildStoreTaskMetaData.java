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

import java.io.Serializable;

public class RebuildStoreTaskMetaData extends ScheduleTaskMetadata {

    private static final long serialVersionUID = -6822059115806648547L;
    private Long              fromStoreId;
    private Long              toStoreId;
    private int               taskStatusCode;

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

    @Override
    public String toString() {
        return "RebuildStoreTaskMetaData{" + "taskId='" + getTaskId() + '\'' + ", fromStoreId=" + fromStoreId
               + ", toStoreId=" + toStoreId + ", taskStatusCode=" + taskStatusCode + '}';
    }

    public enum TaskStatus {
        INIT(0), RESET_STORE(1), CHANGE_PEER(2), FINISHED(3);

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

}
