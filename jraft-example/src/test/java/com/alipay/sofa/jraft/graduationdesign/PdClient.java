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
package com.alipay.sofa.jraft.graduationdesign;

import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.graduationdesign.client.Client;
import com.alipay.sofa.jraft.rhea.JRaftHelper;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.client.pd.PlacementDriverClient;
import com.alipay.sofa.jraft.rhea.client.pd.RemotePlacementDriverClient;
import com.alipay.sofa.jraft.rhea.metadata.Cluster;
import com.alipay.sofa.jraft.rhea.metadata.MigrationPlan;
import com.alipay.sofa.jraft.rhea.metadata.Store;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Endpoint;

import java.util.ArrayList;
import java.util.List;

public class PdClient {
    public static void main(String[] args) {
        final Client client = new Client();
        client.init();
        //        submitRebuildRequest(client.getRheaKVStore());
        submitUpscaleClusterRequest(client.getRheaKVStore());
        listClusterInfo(client.getRheaKVStore());
        client.shutdown();
    }

    public static void listClusterInfo(final RheaKVStore rheaKVStore) {
        PlacementDriverClient pdClient = rheaKVStore.getPlacementDriverClient();
        RemotePlacementDriverClient remotePdClient = (RemotePlacementDriverClient) pdClient;
        Endpoint leaderEndpoint = remotePdClient.getPdLeader(true, 10000);
        System.out.println(leaderEndpoint);
        rheaKVStore.get(BytesUtil.writeUtf8("hello"));
        Cluster cluster = remotePdClient.getMetadataRpcClient().getClusterInfo(111);
        printCluster(cluster);
    }

    public static void submitRebuildRequest(final RheaKVStore rheaKVStore) {
        PlacementDriverClient pdClient = rheaKVStore.getPlacementDriverClient();
        RemotePlacementDriverClient remotePdClient = (RemotePlacementDriverClient) pdClient;
        String taskId = remotePdClient.getClusterManagementRpcClient().submitRebuildStoreRequest(111, 0, 3);
        System.out.println(taskId);
    }

    public static void submitUpscaleClusterRequest(final RheaKVStore rheaKVStore) {
        PlacementDriverClient pdClient = rheaKVStore.getPlacementDriverClient();
        RemotePlacementDriverClient remotePdClient = (RemotePlacementDriverClient) pdClient;
        MigrationPlan plan = new MigrationPlan();
        plan.setClusterId(111);
        List<MigrationPlan.MigrationPlanEntry> migrationPlanEntries = new ArrayList<>();
        migrationPlanEntries.add(new MigrationPlan.MigrationPlanEntry(0, 5, 1));
        migrationPlanEntries.add(new MigrationPlan.MigrationPlanEntry(1, 6, 1));
        migrationPlanEntries.add(new MigrationPlan.MigrationPlanEntry(2, 4, 1));
        plan.setMigrationPlanEntries(migrationPlanEntries);
        String taskId = remotePdClient.getClusterManagementRpcClient().submitUpscaleClusterRequest(111,
            Lists.newArrayList(4L, 5L, 6L), plan);
        System.out.println(taskId);
    }

    public static void addPeer(final RheaKVStore rheaKVStore) {
        PlacementDriverClient pdClient = rheaKVStore.getPlacementDriverClient();
        final PeerId newPeer = new PeerId("127.0.0.1", 18184);
        pdClient.refreshRouteConfiguration(1);
        boolean result = pdClient.addReplica(1, JRaftHelper.toPeer(newPeer), true);
        pdClient.addReplica(1, JRaftHelper.toPeer(newPeer), true);
        System.out.println("region1 add peer result " + result);
        result = pdClient.addReplica(2, JRaftHelper.toPeer(newPeer), true);
        System.out.println("region2 add peer result " + result);
    }

    static void printCluster(Cluster cluster) {
        if (cluster == null) {
            return;
        }
        cluster.getStores().sort((s1, s2) -> (int) (s1.getId() - s2.getId()));
        System.out.println("cluster id " + cluster.getClusterId());
        for (Store store : cluster.getStores()) {
            System.out.println(store);
        }
    }
}
