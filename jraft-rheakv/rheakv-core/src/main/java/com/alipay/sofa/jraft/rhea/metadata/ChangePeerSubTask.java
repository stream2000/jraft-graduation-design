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

import com.alipay.sofa.jraft.conf.Configuration;

import java.io.Serializable;

public class ChangePeerSubTask implements Serializable {
    private static final long serialVersionUID = 6577850395813314311L;

    private String            parentTaskId;
    private String            taskId;
    private Region            region;
    private int               clusterId;
    private long              regionId;
    private boolean           finished;
    private Configuration     newConfiguration;

    public String getParentTaskId() {
        return parentTaskId;
    }

    public void setParentTaskId(final String parentTaskId) {
        this.parentTaskId = parentTaskId;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(final String taskId) {
        this.taskId = taskId;
    }

    public int getClusterId() {
        return clusterId;
    }

    public void setClusterId(final int clusterId) {
        this.clusterId = clusterId;
    }

    public long getRegionId() {
        return regionId;
    }

    public void setRegionId(final long regionId) {
        this.regionId = regionId;
    }

    public Configuration getNewConfiguration() {
        return newConfiguration;
    }

    public void setNewConfiguration(final Configuration newConfiguration) {
        this.newConfiguration = newConfiguration;
    }

    public boolean isFinished() {
        return finished;
    }

    public void setFinished(final boolean finished) {
        this.finished = finished;
    }

    public Region getRegion() {
        return region;
    }

    public void setRegion(final Region region) {
        this.region = region;
    }

    @Override
    public String toString() {
        return "ChangePeerSubTask{" + "parentTaskId='" + parentTaskId + '\'' + ", taskId='" + taskId + '\''
               + ", clusterId=" + clusterId + ", regionId=" + regionId + ", newConfiguration=" + newConfiguration + '}';
    }
}
