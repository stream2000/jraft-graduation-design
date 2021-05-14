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

public class ScheduleTaskMetadata implements Serializable {

    private static final long serialVersionUID = -3475650546211662731L;
    private int               clusterId;
    private String            taskId;
    private int               taskType;

    public int getClusterId() {
        return clusterId;
    }

    public void setClusterId(final int clusterId) {
        this.clusterId = clusterId;
    }

    public int getTaskType() {
        return taskType;
    }

    public void setTaskType(final int taskType) {
        this.taskType = taskType;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(final String taskId) {
        this.taskId = taskId;
    }

    public enum ScheduleTaskType {
        REBUILD_STORE(0);

        private final int code;

        ScheduleTaskType(int code) {
            this.code = code;
        }

        public static ScheduleTaskType codeOf(int code) {
            for (ScheduleTaskType status : values()) {
                if (status.getCode() == code) {
                    return status;
                }
            }
            throw new RuntimeException("Invalid task type code");
        }

        public int getCode() {
            return code;
        }
    }
}
