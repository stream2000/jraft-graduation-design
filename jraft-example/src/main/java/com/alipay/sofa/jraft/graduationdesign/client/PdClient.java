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
package com.alipay.sofa.jraft.graduationdesign.client;

import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.client.pd.PlacementDriverClient;
import com.alipay.sofa.jraft.rhea.client.pd.RemotePlacementDriverClient;
import com.alipay.sofa.jraft.rhea.metadata.Cluster;
import com.alipay.sofa.jraft.util.Endpoint;

public class PdClient {
    public static void main(String[] args) {
        final Client client = new Client();
        client.init();
        listClusterInf(client.getRheaKVStore());
        client.shutdown();
    }

    public static void listClusterInf(final RheaKVStore rheaKVStore) {
        PlacementDriverClient pdClient = rheaKVStore.getPlacementDriverClient();
        RemotePlacementDriverClient remotePdClient = (RemotePlacementDriverClient) pdClient;
        Endpoint leaderEndpoint = remotePdClient.getPdLeader(true, 10000);
        System.out.println(leaderEndpoint);
        Cluster cluster = remotePdClient.getMetadataRpcClient().getClusterInfo(111);
        if (cluster == null) {
            System.out.println("the cluster info is null");
        }
        System.out.println(cluster);
    }

}
