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
package com.alipay.sofa.jraft.rhea.client.pd;

import com.alipay.sofa.jraft.rhea.client.FutureHelper;
import com.alipay.sofa.jraft.rhea.client.failover.FailoverClosure;
import com.alipay.sofa.jraft.rhea.client.failover.RetryRunner;
import com.alipay.sofa.jraft.rhea.client.failover.impl.FailoverClosureImpl;
import com.alipay.sofa.jraft.rhea.cmd.pd.RebuildStoreRequest;
import com.alipay.sofa.jraft.rhea.errors.Errors;

import java.util.concurrent.CompletableFuture;

public class ClusterManagementRpcClient {
    private final PlacementDriverRpcService pdRpcService;
    private final int                       failoverRetries;

    public ClusterManagementRpcClient(final PlacementDriverRpcService pdRpcService, final int failoverRetries) {
        this.pdRpcService = pdRpcService;
        this.failoverRetries = failoverRetries;
    }

    public String submitRebuildStoreRequest(final long clusterId, final long fromStoreId, final long toStoreId) {
        final CompletableFuture<String> future = new CompletableFuture<>();
        internalSubmitRebuildStoreRequest(clusterId, fromStoreId, toStoreId, future, this.failoverRetries, null);
        return FutureHelper.get(future);
    }

    public void internalSubmitRebuildStoreRequest(final long clusterId, final long fromStoreId, final long toStoreId,
                                                  final CompletableFuture<String> future, final int retriesLeft,
                                                  final Errors lastCause) {
        final RetryRunner retryRunner = retryCause -> internalSubmitRebuildStoreRequest(clusterId, fromStoreId,
                toStoreId, future, retriesLeft - 1, retryCause);
        final FailoverClosure<String> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        final RebuildStoreRequest   request = new RebuildStoreRequest();
        request.setClusterId(clusterId);
        request.setFromStoreId(fromStoreId);
        request.setToStoreId(toStoreId);
        this.pdRpcService.callPdServerWithRpc(request, closure, lastCause);
    }
}
