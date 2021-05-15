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
import com.alipay.sofa.jraft.rhea.metadata.MigrationPlan.MigrationPlanEntry;
import com.alipay.sofa.jraft.rhea.metadata.RebuildStoreTaskMetaData;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metadata.RegionStats;
import com.alipay.sofa.jraft.rhea.metadata.Store;
import com.alipay.sofa.jraft.rhea.metadata.UpScaleClusterMetadata;
import com.alipay.sofa.jraft.rhea.storage.CASEntry;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.Pair;
import com.alipay.sofa.jraft.util.BytesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class UpscaleClusterScheduler extends Scheduler {
    private static final Logger LOG = LoggerFactory.getLogger(UpscaleClusterScheduler.class);

    private final UpScaleClusterMetadata taskMeta;
    private final String taskKey;

    public UpscaleClusterScheduler(final MetadataStore metadataStore, UpScaleClusterMetadata taskMeta) {
        super(metadataStore);
        this.taskMeta = taskMeta;
        this.taskKey = MetadataKeyHelper.getSchedulerTaskKey(taskMeta.getTaskId());
    }

    @Override
    protected void nextStage() {
        if (isStopped) {
            return;
        }
        UpScaleClusterMetadata.TaskStatus status = UpScaleClusterMetadata.TaskStatus
                .codeOf(taskMeta.getTaskStatusCode());
        switch (status) {
            case INIT:
                processInit();
                break;
            case RESET_STORE:
                processResetStore();
                break;
            case WAIT_RESET_STORE_FINISH:
                processWaitResetStore();
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
        LOG.info("UpScaleCluster task {} entered init state", taskMeta);
        byte[] taskMetaExpect = this.serializer.writeObject(taskMeta);
        if (taskMeta.getMigrationPlan() == null || taskMeta.getMigrationPlan().getMigrationPlanEntries() == null
            || taskMeta.getMigrationPlan().getMigrationPlanEntries().isEmpty()) {
            abortTask();
            return;
        }
        // do integrity check
        for (MigrationPlanEntry entry : taskMeta.getMigrationPlan().getMigrationPlanEntries()) {
            Store fromStoreMeta = this.metadataStore.getStoreInfo(taskMeta.getClusterId(), entry.getFromStoreId());
            Store toStoreMeta = this.metadataStore.getStoreInfo(taskMeta.getClusterId(), entry.getToStoreId());
            Region region = new Region();
            region.setId(entry.getRegionId());
            Pair<Region, RegionStats> pair = this.metadataStore.getRegionStats(taskMeta.getClusterId(), region);
            if (fromStoreMeta == null || toStoreMeta == null || pair == null || pair.getKey() == null) {
                abortTask();
                return;
            }
            entry.setFromStoreMeta(fromStoreMeta);
            entry.setToStoreMeta(toStoreMeta);
            entry.setRegion(pair.getKey());
        }
        // next stage
        taskMeta.setTaskStatusCode(RebuildStoreTaskMetaData.TaskStatus.RESET_STORE.getCode());
        if (!CASUpdateTaskMeta(taskMetaExpect)) {
            return;
        }
        nextStage();
    }

    private void processResetStore() {
        LOG.info("UpScaleCluster task {} entered reset store state", taskMeta);
        byte[] taskMetaExpect = this.serializer.writeObject(taskMeta);

        List<CASEntry> casEntries = new ArrayList<>();
        Map<Long, Store> modifyStores = new HashMap<>();
        Map<Long, byte[]> modifyStoreExpects = new HashMap<>();

        for (MigrationPlanEntry entry : taskMeta.getMigrationPlan().getMigrationPlanEntries()) {
            if (!modifyStores.containsKey(entry.getToStoreId())) {
                modifyStores.put(entry.getToStoreMeta().getId(), entry.getToStoreMeta());
                modifyStoreExpects
                        .put(entry.getToStoreMeta().getId(), this.serializer.writeObject(entry.getToStoreMeta()));
            }
            Store toStoreMeta = modifyStores.get(entry.getToStoreId());
            if (toStoreMeta.getRegions() == null) {
                toStoreMeta.setRegions(new ArrayList<>());
            }
            toStoreMeta.getRegions().add(entry.getRegion());
        }

        for (Store toStoreMeta : modifyStores.values()) {
            String storeKey = MetadataKeyHelper.getStoreInfoKey(taskMeta.getClusterId(), toStoreMeta.getId());
            byte[] toStoreExpect = modifyStoreExpects.get(toStoreMeta.getId());
            addToStorePeerToRegionConf(toStoreMeta);
            toStoreMeta.setNeedOverwrite(true);
            casEntries.add(new CASEntry(BytesUtil.writeUtf8(storeKey), toStoreExpect,
                    this.serializer.writeObject(toStoreMeta)));
        }

        taskMeta.setModifyStores(Lists.newArrayList(modifyStores.values()));
        taskMeta.setTaskStatusCode(UpScaleClusterMetadata.TaskStatus.WAIT_RESET_STORE_FINISH.getCode());
        casEntries
                .add(new CASEntry(BytesUtil.writeUtf8(taskKey), taskMetaExpect, this.serializer.writeObject(taskMeta)));

        boolean ok = this.rheaKVStore.bCompareAndPutAll(casEntries);
        if (!ok) {
            LOG.warn("Task {} was modified by another process", taskMeta.getTaskId());
            cancel();
            return;
        }

        nextStage();

    }

    private void processWaitResetStore() {
        LOG.info("UpScaleCluster task {} entered reset store state", taskMeta);
        byte[] taskMetaExpect = this.serializer.writeObject(taskMeta);
        int sleepDuration = 1000;
        while (!isStopped) {
            try {
                // TODO: use a set to optimize it
                boolean allFinished = true;
                for (Store store : taskMeta.getModifyStores()) {
                    Store currentStore = metadataStore.getStoreInfo(taskMeta.getClusterId(), store.getId());
                    if (currentStore.isNeedOverwrite()) {
                        allFinished = false;
                        break;
                    }
                }
                if (allFinished) {
                    taskMeta.setTaskStatusCode(UpScaleClusterMetadata.TaskStatus.PREPARE_CHANGE_PEER.getCode());
                    if (!CASUpdateTaskMeta(taskMetaExpect)) {
                        return;
                    }
                    nextStage();
                    return;
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

    private void processPrepareChangePeer() {
        LOG.info("UpScaleCluster task {} entered reset store state", taskMeta);
        byte[] taskMetaExpect = this.serializer.writeObject(taskMeta);
        Map<Long, ChangePeerSubTask> changePeerSubTasks = new HashMap<>();
        for (MigrationPlanEntry entry : taskMeta.getMigrationPlan().getMigrationPlanEntries()) {
            Region region = entry.getRegion();
            if (!changePeerSubTasks.containsKey(region.getId())) {
                ChangePeerSubTask changePeerSubTask = new ChangePeerSubTask();
                Configuration newConf = new Configuration();
                region.getPeers().forEach(p -> newConf.addPeer(JRaftHelper.toJRaftPeerId(p)));
                changePeerSubTask.setNewConfiguration(newConf);
                changePeerSubTasks.put(region.getId(), changePeerSubTask);
            }
            ChangePeerSubTask changePeerSubTask = changePeerSubTasks.get(region.getId());
            Configuration newConf = changePeerSubTask.getNewConfiguration();
            PeerId fromStorePeer = new PeerId(entry.getFromStoreMeta().getEndpoint(), 0);
            PeerId toStorePeer = new PeerId(entry.getToStoreMeta().getEndpoint(), 0);
            if (!newConf.contains(toStorePeer)) {
                newConf.addPeer(toStorePeer);
            }
            if (newConf.contains(fromStorePeer)) {
                newConf.removePeer(fromStorePeer);
            }
            changePeerSubTask.setTaskId(UUID.randomUUID().toString());
            changePeerSubTask.setParentTaskId(taskMeta.getTaskId());
            changePeerSubTask.setNewConfiguration(newConf);
            changePeerSubTask.setRegionId(region.getId());
            changePeerSubTask.setClusterId(taskMeta.getClusterId());
            changePeerSubTask.setRegion(region);
        }
        taskMeta.setTaskStatusCode(UpScaleClusterMetadata.TaskStatus.CHANGE_PEER.getCode());
        taskMeta.setChangePeerSubTasks(Lists.newArrayList(changePeerSubTasks.values()));
        if (!CASUpdateTaskMeta(taskMetaExpect)) {
            return;
        }
        nextStage();
    }

    private void processChangePeer() {
        LOG.info("UpScale task {} entered change peer state", taskMeta);
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
        while (!unfinishedTaskSet.isEmpty() && !isStopped) {
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
                        LOG.info("UpScaleClusterTask with id {} Region {} finished conf change", taskMeta.getTaskId(),
                                stats.getKey());
                        unfinishedTaskSet.remove(stats.getKey().getId());
                        subTask.setFinished(true);
                        this.metadataStore.deleteChangePeerSubTask(stats.getKey().getId());
                    } else {
                        LOG.info("UpScaleClusterTask with id {} Region {} haven't finished conf change",
                                taskMeta.getTaskId(), stats.getKey());
                    }
                }
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                cancel();
                return;
            }
        }

        taskMeta.setTaskStatusCode(UpScaleClusterMetadata.TaskStatus.FINISHED.getCode());

        if (!CASUpdateTaskMeta(taskMetaExpect)) {
            return;
        }

        nextStage();
    }

    private void finishTask() {
        LOG.info("Task {}  is finished ", taskMeta);
        this.rheaKVStore.delete(taskKey);
    }

    // TODO: add abort cause
    private void abortTask() {
        LOG.info("Task {}  is aborted ", taskMeta);
        // try to set task status to aborted and archive it. If failed, return directly
        this.rheaKVStore.delete(taskKey);
        cancel();
    }

    private boolean CASUpdateTaskMeta(final byte[] expect) {
        boolean ok = rheaKVStore.bCompareAndPut(taskKey, expect, this.serializer.writeObject(taskMeta));
        if (!ok) {
            LOG.warn("Task {} was modified by another process", taskMeta.getTaskId());
            cancel();
        }
        return ok;
    }
}
