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

public class UpScaleClusterMetadata extends ScheduleTaskMetadata {
    private static final long       serialVersionUID = 1762452908908603860L;

    private int                     taskStatusCode;
    private List<ChangePeerSubTask> changePeerSubTasks;

    public int getTaskStatusCode() {
        return taskStatusCode;
    }

    public void setTaskStatusCode(final int taskStatusCode) {
        this.taskStatusCode = taskStatusCode;
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
}
