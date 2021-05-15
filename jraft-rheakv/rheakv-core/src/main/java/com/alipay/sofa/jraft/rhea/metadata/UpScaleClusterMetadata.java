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
    private MigrationPlan           migrationPlan;
    private List<Store>             modifyStores;
    private List<ChangePeerSubTask> changePeerSubTasks;

    public int getTaskStatusCode() {
        return taskStatusCode;
    }

    public void setTaskStatusCode(final int taskStatusCode) {
        this.taskStatusCode = taskStatusCode;
    }

    public MigrationPlan getMigrationPlan() {
        return migrationPlan;
    }

    public void setMigrationPlan(final MigrationPlan migrationPlan) {
        this.migrationPlan = migrationPlan;
    }

    public List<Store> getModifyStores() {
        return modifyStores;
    }

    public void setModifyStores(final List<Store> modifyStores) {
        this.modifyStores = modifyStores;
    }

    public List<ChangePeerSubTask> getChangePeerSubTasks() {
        return changePeerSubTasks;
    }

    public void setChangePeerSubTasks(final List<ChangePeerSubTask> changePeerSubTasks) {
        this.changePeerSubTasks = changePeerSubTasks;
    }

    public enum TaskStatus {
        INIT(0), RESET_STORE(1), WAIT_RESET_STORE_FINISH(2), PREPARE_CHANGE_PEER(3), CHANGE_PEER(4), FINISHED(5), ABORT(
                                                                                                                        6);

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
