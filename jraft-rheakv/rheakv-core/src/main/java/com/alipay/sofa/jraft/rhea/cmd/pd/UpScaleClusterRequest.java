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
package com.alipay.sofa.jraft.rhea.cmd.pd;

import com.alipay.sofa.jraft.rhea.metadata.MigrationPlan;

import java.util.List;

public class UpScaleClusterRequest extends BaseRequest {

    private static final long serialVersionUID = -4152455264960650123L;
    private MigrationPlan     migrationPlan;
    private List<Long>        newStoreIds;

    public MigrationPlan getMigrationPlan() {
        return migrationPlan;
    }

    public void setMigrationPlan(final MigrationPlan migrationPlan) {
        this.migrationPlan = migrationPlan;
    }

    public List<Long> getNewStoreIds() {
        return newStoreIds;
    }

    public void setNewStoreIds(final List<Long> newStoreIds) {
        this.newStoreIds = newStoreIds;
    }

    @Override
    public byte magic() {
        return UPSCALE_CLUSTER;
    }
}
