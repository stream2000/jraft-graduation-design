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

import com.alipay.sofa.jraft.rhea.LeaderStateListener;
import com.alipay.sofa.jraft.rhea.MetadataStore;
import com.alipay.sofa.jraft.rhea.metadata.RebuildStoreTaskMetaData;
import com.alipay.sofa.jraft.rhea.metadata.ScheduleTaskMetadata;
import com.alipay.sofa.jraft.rhea.serialization.Serializers;
import com.alipay.sofa.jraft.rhea.util.Pair;
import com.alipay.sofa.jraft.rhea.util.concurrent.CallerRunsPolicyWithReport;
import com.alipay.sofa.jraft.rhea.util.concurrent.NamedThreadFactory;
import com.alipay.sofa.jraft.util.ExecutorServiceHelper;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

public class SchedulerManager implements LeaderStateListener {

    private final ThreadPoolExecutor     executor;
    private final MetadataStore          metadataStore;
    private final Map<String, Scheduler> schedulerMap = new ConcurrentHashMap<>();
    private boolean                      isLeader;

    public SchedulerManager(final MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
        this.executor = createSchedulerExecutor();
    }

    public void loadSchedulersFromMetadata() {
        Set<String> taskIds = this.metadataStore.getUnfinishedScheduleTaskIds();
        for (String taskId : taskIds) {
            Pair<ScheduleTaskMetadata, byte[]> pair = metadataStore.getScheduleTaskMetadata(taskId);
            if (pair == null) {
                // the task is deleted
                continue;
            }
            ScheduleTaskMetadata taskMetadata = pair.getKey();
            ScheduleTaskMetadata.ScheduleTaskType taskType = ScheduleTaskMetadata.ScheduleTaskType.codeOf(taskMetadata
                .getTaskType());
            switch (taskType) {
                case REBUILD_STORE:
                    RebuildStoreTaskMetaData rebuildStoreTaskMetaData = Serializers.getDefault().readObject(
                        pair.getValue(), RebuildStoreTaskMetaData.class);
                    RebuildStoreScheduler rebuildStoreScheduler = new RebuildStoreScheduler(metadataStore,
                        rebuildStoreTaskMetaData);
                    registerScheduler(rebuildStoreScheduler);
                    break;
                default:
                    throw new RuntimeException("Invalid task type");
            }
        }
    }

    public boolean registerScheduler(Scheduler scheduler) {
        if (!isLeader) {
            return false;
        }
        String taskId = UUID.randomUUID().toString();
        schedulerMap.put(taskId, scheduler);
        // auto remove the task
        scheduler.onStop(() -> schedulerMap.remove(taskId));
        executor.submit(scheduler);
        return true;
    }

    public void cancelAllSchedulers() {
        isLeader = false;
        for (Scheduler scheduler : schedulerMap.values()) {
            scheduler.cancel();
        }
    }

    public void shutdown() {
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.executor);
    }

    private ThreadPoolExecutor createSchedulerExecutor() {
        final int corePoolSize = 4;
        final String name = "rheakv-pd-scheduler-task-executor";
        return ThreadPoolUtil.newBuilder() //
            .poolName(name) //
            .enableMetric(false) //
            .coreThreads(corePoolSize) //
            .maximumThreads(corePoolSize << 2) //
            .keepAliveSeconds(120L) //
            .workQueue(new ArrayBlockingQueue<>(1024)) //
            .threadFactory(new NamedThreadFactory(name, true)) //
            .rejectedHandler(new CallerRunsPolicyWithReport(name, name)) //
            .build();
    }

    @Override
    public void onLeaderStart(final long newTerm) {
        isLeader = true;
        loadSchedulersFromMetadata();
    }

    @Override
    public void onLeaderStop(final long oldTerm) {
        isLeader = false;
        cancelAllSchedulers();
    }
}
