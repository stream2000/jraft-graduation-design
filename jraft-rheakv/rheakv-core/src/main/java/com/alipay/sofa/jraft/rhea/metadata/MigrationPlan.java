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
package com.alipay.sofa.jraft.rhea.metadata;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

public class MigrationPlan implements Serializable {
    private static final long        serialVersionUID = 1028850471119902092L;

    private int                      clusterId;
    private List<MigrationPlanEntry> migrationPlanEntries;
    private Set<Long>                newStoreIds;

    public int getClusterId() {
        return clusterId;
    }

    public void setClusterId(final int clusterId) {
        this.clusterId = clusterId;
    }

    public List<MigrationPlanEntry> getMigrationPlanEntries() {
        return migrationPlanEntries;
    }

    public void setMigrationPlanEntries(final List<MigrationPlanEntry> migrationPlanEntries) {
        this.migrationPlanEntries = migrationPlanEntries;
    }

    public Set<Long> getNewStoreIds() {
        return newStoreIds;
    }

    public void setNewStoreIds(final Set<Long> newStoreIds) {
        this.newStoreIds = newStoreIds;
    }

    public static class MigrationPlanEntry implements Serializable {

        private static final long serialVersionUID = 3226978323120124772L;
        private final long        fromStoreId;
        private final long        toStoreId;
        private final long        regionId;
        private Store             fromStoreMeta;
        private Store             toStoreMeta;
        private Region            region;

        public long getFromStoreId() {
            return fromStoreId;
        }

        public long getToStoreId() {
            return toStoreId;
        }

        public long getRegionId() {
            return regionId;
        }

        public Store getFromStoreMeta() {
            return fromStoreMeta;
        }

        public void setFromStoreMeta(final Store fromStoreMeta) {
            this.fromStoreMeta = fromStoreMeta;
        }

        public Store getToStoreMeta() {
            return toStoreMeta;
        }

        public void setToStoreMeta(final Store toStoreMeta) {
            this.toStoreMeta = toStoreMeta;
        }

        public Region getRegion() {
            return region;
        }

        public void setRegion(final Region region) {
            this.region = region;
        }

        public MigrationPlanEntry(final int fromStoreId, final int toStoreId, final long regionId) {
            this.fromStoreId = fromStoreId;
            this.toStoreId = toStoreId;
            this.regionId = regionId;
        }
    }

}
