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
package com.alipay.sofa.jraft.rhea.scheduler;

import com.alipay.sofa.jraft.rhea.MetadataStore;
import com.alipay.sofa.jraft.rhea.metadata.UpScaleClusterMetadata;

public class UpscaleClusterScheduler extends Scheduler {
    private final UpScaleClusterMetadata taskMeta;

    public UpscaleClusterScheduler(final MetadataStore metadataStore, UpScaleClusterMetadata taskMeta) {
        super(metadataStore);
        this.taskMeta = taskMeta;
    }

    @Override
    protected void nextStage() {
        if (isStopped) {
            return;
        }
        UpScaleClusterMetadata.TaskStatus status = UpScaleClusterMetadata.TaskStatus.codeOf(taskMeta
            .getTaskStatusCode());
        switch (status) {
            case INIT:
                processInit();
                break;
            case RESET_STORE:
                processResetStore();
                break;
            case PREPARE_CHANGE_PEER:
                processPrepareChangePeer();
                break;
            case CHANGE_PEER:
                processChangePeer();
                break;
            case FINISHED:
                finishTask();
                this.isStopped = true;
                this.stopHook.run();
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + taskMeta.getTaskStatusCode());
        }
    }

    private void processInit() {

    }

    private void processResetStore() {
    }

    private void processPrepareChangePeer() {
    }

    private void processChangePeer() {
    }

    private void finishTask() {
    }
}
