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
import com.alipay.sofa.jraft.rhea.JRaftHelper;
import com.alipay.sofa.jraft.rhea.MetadataKeyHelper;
import com.alipay.sofa.jraft.rhea.MetadataStore;
import com.alipay.sofa.jraft.rhea.metadata.ChangePeerSubTask;
import com.alipay.sofa.jraft.rhea.metadata.Cluster;
import com.alipay.sofa.jraft.rhea.metadata.Peer;
import com.alipay.sofa.jraft.rhea.metadata.RebuildStoreTaskMetaData;
import com.alipay.sofa.jraft.rhea.metadata.RebuildStoreTaskMetaData.TaskStatus;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metadata.RegionStats;
import com.alipay.sofa.jraft.rhea.metadata.Store;
import com.alipay.sofa.jraft.rhea.storage.CASEntry;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.Pair;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Requires;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class RebuildStoreScheduler extends Scheduler {

    private static final Logger            LOG = LoggerFactory.getLogger(RebuildStoreScheduler.class);
    private final RebuildStoreTaskMetaData taskMeta;
    private final String                   taskKey;

    public RebuildStoreScheduler(final MetadataStore metadataStore, RebuildStoreTaskMetaData taskMeta) {
        super(metadataStore);
        this.taskMeta = taskMeta;
        this.taskKey = MetadataKeyHelper.getSchedulerTaskKey(taskMeta.getTaskId());
    }

    @Override
    protected void nextStage() {
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
        LOG.info("RebuildStoreSchedule task {} entered init state", taskMeta);
        byte[] taskMetaExpect = this.serializer.writeObject(taskMeta);
        Store fromStoreMeta = metadataStore.getStoreInfo(taskMeta.getClusterId(), taskMeta.getFromStoreId());
        taskMeta.setTaskStatusCode(TaskStatus.RESET_STORE.getCode());
        taskMeta.setFromStoreMeta(fromStoreMeta);
        taskMeta.setResetStoreSubTask(new RebuildStoreTaskMetaData.ResetStoreSubTask(
            RebuildStoreTaskMetaData.ResetStoreSubTask.INIT_STATE));
        if (!CASUpdateTaskMeta(taskMetaExpect)) {
            return;
        }
        nextStage();
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
                        if (!CASUpdateTaskMeta(taskMetaExpect)) {
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
            taskMeta.setTaskStatusCode(TaskStatus.PREPARE_CHANGE_PEER.getCode());
            // prepare for change peer
            if (!CASUpdateTaskMeta(taskMetaExpect)) {
                return;
            }
        }

        nextStage();
    }

    private void processPrepareChangePeer() {
        LOG.info("RebuildStoreSchedule task {} entered prepare change peer state", taskMeta);
        byte[] taskMetaExpect = this.serializer.writeObject(taskMeta);
        // STEP1 record sub tasks
        // generate sub tasks, store it and change state of task meta
        Store toStoreMeta = metadataStore.getStoreInfo(taskMeta.getClusterId(), taskMeta.getToStoreId());
        List<ChangePeerSubTask> changePeerSubTasks = new ArrayList<>();
        for (final Region region : toStoreMeta.getRegions()) {
            Configuration newConf = new Configuration();
            PeerId fromStorePeer = new PeerId(taskMeta.getFromStoreMeta().getEndpoint(), 0);
            region.getPeers().forEach(p -> newConf.addPeer(JRaftHelper.toJRaftPeerId(p)));
            newConf.removePeer(fromStorePeer);
            ChangePeerSubTask changePeerSubTask = new ChangePeerSubTask();
            changePeerSubTask.setTaskId(UUID.randomUUID().toString());
            changePeerSubTask.setParentTaskId(taskMeta.getTaskId());
            changePeerSubTask.setNewConfiguration(newConf);
            changePeerSubTask.setRegionId(region.getId());
            changePeerSubTask.setClusterId(taskMeta.getClusterId());
            changePeerSubTask.setRegion(region);
            changePeerSubTasks.add(changePeerSubTask);
        }
        taskMeta.setTaskStatusCode(TaskStatus.CHANGE_PEER.getCode());
        taskMeta.setChangePeerSubTasks(changePeerSubTasks);
        if (!CASUpdateTaskMeta(taskMetaExpect)) {
            return;
        }
        nextStage();
    }

    private void processChangePeer() {
        LOG.info("RebuildStoreSchedule task {} entered change peer state", taskMeta);
        if (taskMeta.getChangePeerSubTasks() == null) {
            abortTask();
            return;
        }

        // record expect
        byte[] taskMetaExpect = this.serializer.writeObject(taskMeta);

        // step1: add the task to in memory cache
        Set<Long> unfinishedTaskSet = new HashSet<>();
        Map<Long, Region> modifyRegionsMap = new HashMap<>();
        for (ChangePeerSubTask subTask : taskMeta.getChangePeerSubTasks()) {
            this.metadataStore.addChangePeerSubTask(subTask);
            unfinishedTaskSet.add(subTask.getRegionId());
            Region newRegion = subTask.getRegion().copy();
            newRegion.setPeers(JRaftHelper.toPeerList(subTask.getNewConfiguration().listPeers()));
            modifyRegionsMap.put(newRegion.getId(), newRegion);
        }

        // step2: wait update
        while (!unfinishedTaskSet.isEmpty()) {
            try {
                for (ChangePeerSubTask subTask : taskMeta.getChangePeerSubTasks()) {
                    if (!unfinishedTaskSet.contains(subTask.getRegionId())) {
                        continue;
                    }
                    Pair<Region, RegionStats> stats = this.metadataStore
                            .getRegionStats(subTask.getClusterId(), subTask.getRegion());
                    if (stats == null) {
                        continue;
                    }
                    Configuration regionCurrentConf = new Configuration();
                    stats.getValue().getPeers().forEach(regionCurrentConf::addPeer);
                    if (regionCurrentConf.equals(subTask.getNewConfiguration())) {
                        LOG.info("RebuildStoreTask with id {} Region {} finished conf change", taskMeta.getTaskId(),
                                stats.getKey());
                        unfinishedTaskSet.remove(stats.getKey().getId());
                        subTask.setFinished(true);
                        this.metadataStore.deleteChangePeerSubTask(stats.getKey().getId());
                    } else {
                        LOG.info("RebuildStoreTask with id {} Region {} haven't finished conf change",
                                taskMeta.getTaskId(), stats.getKey());
                    }
                }
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                cancel();
                return;
            }
        }

        taskMeta.setTaskStatusCode(TaskStatus.FINISHED.getCode());

        List<CASEntry> casEntries = new ArrayList<>();

        casEntries
                .add(new CASEntry(BytesUtil.writeUtf8(taskKey), taskMetaExpect, this.serializer.writeObject(taskMeta)));
        Cluster cluster = metadataStore.getClusterInfo(taskMeta.getClusterId());
        Requires.requireNonNull(cluster);

        for (Store store : cluster.getStores()) {
            byte[] storeExpect = this.serializer.writeObject(store);
            String storeKey = MetadataKeyHelper.getStoreInfoKey(cluster.getClusterId(), store.getId());
            for (Region region : store.getRegions()) {
                if (modifyRegionsMap.containsKey(region.getId())) {
                    region.setPeers(modifyRegionsMap.get(region.getId()).getPeers());
                }
            }
            casEntries
                    .add(new CASEntry(BytesUtil.writeUtf8(storeKey), storeExpect, this.serializer.writeObject(store)));
        }

        if (!this.rheaKVStore.bCompareAndPutAll(casEntries)) {
            LOG.warn("Task {} was modified by another process", taskMeta.getTaskId());
            cancel();
            return;
        }

        nextStage();

    }

    boolean CASUpdateTaskMeta(byte[] expect) {
        boolean ok = rheaKVStore.bCompareAndPut(taskKey, expect, this.serializer.writeObject(taskMeta));
        if (!ok) {
            LOG.warn("Task {} was modified by another process", taskMeta.getTaskId());
            cancel();
        }
        return ok;
    }

    private void finishTask() {
        LOG.info("Task {}  is finished ", taskMeta);
        this.rheaKVStore.delete(taskKey);
    }

    // TODO: use CAS to abort task
    private void abortTask() {
        LOG.info("Task {}  is aborted ", taskMeta);
        taskMeta.setTaskStatusCode(TaskStatus.ABORT.getCode());
        // try to set task status to aborted and archive it. If failed, return directly
        this.rheaKVStore.bPut(taskKey, serializer.writeObject(taskMeta));
        this.rheaKVStore.delete(taskKey);
        cancel();
    }
}
