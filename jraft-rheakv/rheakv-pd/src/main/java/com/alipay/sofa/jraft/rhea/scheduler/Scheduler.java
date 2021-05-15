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
import com.alipay.sofa.jraft.rhea.MetadataStore;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.metadata.Peer;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metadata.Store;
import com.alipay.sofa.jraft.rhea.serialization.Serializer;
import com.alipay.sofa.jraft.rhea.serialization.Serializers;

public abstract class Scheduler implements Runnable {

    MetadataStore               metadataStore;
    StopHook                    stopHook;
    volatile boolean            isStopped;
    protected final Serializer  serializer = Serializers.getDefault();
    protected final RheaKVStore rheaKVStore;

    Scheduler(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
        this.rheaKVStore = ((DefaultMetadataStore) metadataStore).getRheaKVStore();
    }

    @Override
    public void run() {
        nextStage();
        if (!isStopped) {
            isStopped = true;
            this.stopHook.run();
        }
    }

    protected abstract void nextStage();

    void cancel() {
        if (!isStopped) {
            isStopped = true;
            this.stopHook.run();
        }
    }

    public static void addToStorePeerToRegionConf(Store toStore) {
        for (final Region region : toStore.getRegions()) {
            Peer toStorePeer = new Peer(region.getId(), toStore.getId(), toStore.getEndpoint());
            PeerId toStorePeerId = JRaftHelper.toJRaftPeerId(toStorePeer);
            Configuration newConf = new Configuration();
            region.getPeers().forEach(p -> newConf.addPeer(JRaftHelper.toJRaftPeerId(p)));
            if (!newConf.contains(toStorePeerId)) {
                region.getPeers().add(toStorePeer);
            }
        }
    }

    void onStop(StopHook stopHook) {
        this.stopHook = stopHook;
    }

    interface StopHook extends Runnable {
    }

}
