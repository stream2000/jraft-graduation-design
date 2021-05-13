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

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rhea.DefaultMetadataStore;
import com.alipay.sofa.jraft.rhea.JRaftHelper;
import com.alipay.sofa.jraft.rhea.MetadataKeyHelper;
import com.alipay.sofa.jraft.rhea.MetadataStore;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.metadata.Peer;
import com.alipay.sofa.jraft.rhea.metadata.RebuildStoreTaskMetaData;
import com.alipay.sofa.jraft.rhea.metadata.RebuildStoreTaskMetaData.TaskStatus;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metadata.Store;
import com.alipay.sofa.jraft.rhea.serialization.Serializer;
import com.alipay.sofa.jraft.rhea.serialization.Serializers;
import com.alipay.sofa.jraft.rhea.storage.CASEntry;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.util.BytesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RebuildStoreScheduler extends Scheduler {

    private static final Logger            LOG        = LoggerFactory.getLogger(RebuildStoreScheduler.class);
    private final RebuildStoreTaskMetaData taskMeta;
    private final RheaKVStore              rheaKVStore;
    private final String                   taskKey;
    private final Serializer               serializer = Serializers.getDefault();

    public RebuildStoreScheduler(final MetadataStore metadataStore, RebuildStoreTaskMetaData taskMeta) {
        super(metadataStore);
        this.taskMeta = taskMeta;
        this.taskKey = MetadataKeyHelper.getSchedulerTaskKey(taskMeta.getTaskId());
        this.rheaKVStore = ((DefaultMetadataStore) metadataStore).getRheaKVStore();
    }

    @Override
    public void cancel() {
        if (!isStopped) {
            isStopped = true;
            this.stopHook.run();
        }
    }

    // polling
    @Override
    public void run() {
        nextStage();
    }

    private void nextStage() {
        if (isStopped) {
            return;
        }
        TaskStatus status = TaskStatus.codeOf(taskMeta.getTaskStatusCode());
        switch (status) {
            case INIT:
                processInit();
                break;
            case RESET_STORE:
                processResetStore();
                break;
            case CHANGE_PEER:
                processChangePeer();
                break;
            case FINISHED:
                this.isStopped = true;
                this.stopHook.run();
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + taskMeta.getTaskStatusCode());
        }
    }

    private void processInit() {
        LOG.info("RebuildStoreSchedule task {} entered init state", taskMeta);
        byte[] prevData = this.serializer.writeObject(taskMeta);
        Store fromStoreMeta = metadataStore.getStoreInfo(taskMeta.getClusterId(), taskMeta.getFromStoreId());
        taskMeta.setTaskStatusCode(TaskStatus.RESET_STORE.getCode());
        taskMeta.setFromStoreMeta(fromStoreMeta);
        taskMeta.setResetStoreSubTask(new RebuildStoreTaskMetaData.ResetStoreSubTask(
            RebuildStoreTaskMetaData.ResetStoreSubTask.INIT_STATE));
        boolean result = this.rheaKVStore.bCompareAndPut(taskKey, prevData, this.serializer.writeObject(taskMeta));
        if (!result) {
            // someone modified it, return directly
            LOG.warn("Task {} was modified by another process", taskMeta.getTaskId());
            cancel();
            return;
        }
        nextStage();
    }

    private void processChangePeer() {
        LOG.info("RebuildStoreSchedule task {} entered change peer state", taskMeta);
    }

    // to reset the target store to the conf we need
    private void processResetStore() {
        LOG.info("RebuildStoreSchedule task {} entered reset store state", taskMeta);
        String storeMetaKey = MetadataKeyHelper.getStoreInfoKey(taskMeta.getClusterId(), taskMeta.getToStoreId());

        // step 1, overwrite the store
        if (taskMeta.getResetStoreSubTask().getTaskState()
                .equalsIgnoreCase(RebuildStoreTaskMetaData.ResetStoreSubTask.INIT_STATE)) {
            Store toStoreMeta = metadataStore.getStoreInfo(taskMeta.getClusterId(), taskMeta.getToStoreId());
            Store fromStoreMeta = taskMeta.getFromStoreMeta();
            // record prevData
            byte[] taskMetaExpect = this.serializer.writeObject(taskMeta);
            byte[] storeMetaExpect = this.serializer.writeObject(toStoreMeta);

            toStoreMeta.setRegions(fromStoreMeta.getRegions());
            // add toStore peer to conf
            for (final Region region : toStoreMeta.getRegions()) {
                Peer toStorePeer = new Peer(region.getId(), toStoreMeta.getId(), toStoreMeta.getEndpoint());
                PeerId toStorePeerId = JRaftHelper.toJRaftPeerId(toStorePeer);
                Configuration newConf = new Configuration();
                region.getPeers().forEach(p -> newConf.addPeer(JRaftHelper.toJRaftPeerId(p)));
                if (!newConf.contains(toStorePeerId)) {
                    region.getPeers().add(toStorePeer);
                }
            }
            toStoreMeta.setNeedOverwrite(true);

            taskMeta.getResetStoreSubTask().setTaskState(RebuildStoreTaskMetaData.ResetStoreSubTask.RESET_STATE);

            // atomic update two meta data
            CASEntry storeMetaCASEntry = new CASEntry(BytesUtil.writeUtf8(storeMetaKey), storeMetaExpect,
                    serializer.writeObject(toStoreMeta));
            CASEntry taskMetaCASEntry = new CASEntry(BytesUtil.writeUtf8(taskKey), taskMetaExpect,
                    serializer.writeObject(taskMeta));
            boolean ok = this.rheaKVStore.bCompareAndPutAll(Lists.newArrayList(storeMetaCASEntry, taskMetaCASEntry));
            if (!ok) {
                LOG.warn("Task {} was modified by another process", taskMeta.getTaskId());
                cancel();
                return;
            }
        }

        if (taskMeta.getResetStoreSubTask().getTaskState()
                .equalsIgnoreCase(RebuildStoreTaskMetaData.ResetStoreSubTask.RESET_STATE)) {
            byte[] taskMetaExpect = this.serializer.writeObject(taskMeta);
            int sleepDuration = 1000;
            while (!isStopped) {
                try {
                    Store currentStore = metadataStore.getStoreInfo(taskMeta.getClusterId(), taskMeta.getToStoreId());
                    if (!currentStore.isNeedOverwrite()) {
                        taskMeta.getResetStoreSubTask()
                                .setTaskState(RebuildStoreTaskMetaData.ResetStoreSubTask.WAIT_UPDATE_STATE);
                        boolean ok = rheaKVStore
                                .bCompareAndPut(taskKey, taskMetaExpect, this.serializer.writeObject(taskMeta));
                        if (!ok) {
                            LOG.warn("Task {} was modified by another process", taskMeta.getTaskId());
                            cancel();
                            return;
                        }
                        break;
                    }
                    if (sleepDuration < 5000) {
                        sleepDuration += 1000;
                    }
                    Thread.sleep(sleepDuration);
                } catch (InterruptedException e) {
                    LOG.warn("task {} is interrupted", taskMeta);
                    cancel();
                    return;
                }
            }
        }

        if (taskMeta.getResetStoreSubTask().getTaskState()
                .equalsIgnoreCase(RebuildStoreTaskMetaData.ResetStoreSubTask.WAIT_UPDATE_STATE)) {
            byte[] taskMetaExpect = this.serializer.writeObject(taskMeta);
            taskMeta.getResetStoreSubTask().setTaskState(RebuildStoreTaskMetaData.ResetStoreSubTask.FINISH_STATE);
            taskMeta.setTaskStatusCode(TaskStatus.CHANGE_PEER.getCode());
            boolean ok = rheaKVStore.bCompareAndPut(taskKey, taskMetaExpect, this.serializer.writeObject(taskMeta));
            if (!ok) {
                LOG.warn("Task {} was modified by another process", taskMeta.getTaskId());
                cancel();
                return;
            }
        }

        nextStage();
    }

    // TODO: use CAS to abort task
    private void abortTask() {
        LOG.warn("Task {}  is aborted ", taskMeta);
        taskMeta.setTaskStatusCode(TaskStatus.ABORT.getCode());
        this.rheaKVStore.bPut(taskKey, serializer.writeObject(taskMeta));
        this.rheaKVStore.delete(taskKey);
        cancel();
    }
}
