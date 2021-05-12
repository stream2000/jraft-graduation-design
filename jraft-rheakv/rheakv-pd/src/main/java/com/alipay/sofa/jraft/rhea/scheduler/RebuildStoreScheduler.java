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

import com.alipay.sofa.jraft.rhea.MetadataStore;
import com.alipay.sofa.jraft.rhea.metadata.RebuildStoreTaskMetaData;

public class RebuildStoreScheduler extends Scheduler {

    private final RebuildStoreTaskMetaData metaData;

    public RebuildStoreScheduler(final MetadataStore metadataStore, RebuildStoreTaskMetaData metaData) {
        super(metadataStore);
        this.metaData = metaData;
    }

    @Override
    public void cancel() {
        if (!isStopped) {
            isStopped = true;
            this.stopHook.run();
        }
    }

    // polling
    @Override
    public void run() {
        while (!isStopped) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void nextStage() {

    }

}
