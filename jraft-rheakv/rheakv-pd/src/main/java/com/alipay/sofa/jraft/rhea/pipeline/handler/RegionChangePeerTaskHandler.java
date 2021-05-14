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
package com.alipay.sofa.jraft.rhea.pipeline.handler;

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.rhea.JRaftHelper;
import com.alipay.sofa.jraft.rhea.MetadataStore;
import com.alipay.sofa.jraft.rhea.cmd.pd.RegionHeartbeatRequest;
import com.alipay.sofa.jraft.rhea.metadata.ChangePeerSubTask;
import com.alipay.sofa.jraft.rhea.metadata.Instruction;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metadata.RegionStats;
import com.alipay.sofa.jraft.rhea.pipeline.event.RegionPingEvent;
import com.alipay.sofa.jraft.rhea.util.Pair;
import com.alipay.sofa.jraft.rhea.util.pipeline.HandlerContext;
import com.alipay.sofa.jraft.rhea.util.pipeline.InboundHandlerAdapter;

public class RegionChangePeerTaskHandler extends InboundHandlerAdapter<RegionPingEvent> {
    @Override
    public void readMessage(final HandlerContext ctx, final RegionPingEvent event) throws Exception {
        if (event.isReady()) {
            return;
        }
        final MetadataStore metadataStore = event.getMetadataStore();
        final RegionHeartbeatRequest request = event.getMessage();
        for (final Pair<Region, RegionStats> regionRegionStatsPair : request.getRegionStatsList()) {
            Region region = regionRegionStatsPair.getKey();
            ChangePeerSubTask changePeerSubTask = metadataStore.getChangePeerSubTask(region.getId());

            Configuration regionCurrentConf = new Configuration();
            region.getPeers().forEach(p -> regionCurrentConf.addPeer(JRaftHelper.toJRaftPeerId(p)));

            if (changePeerSubTask != null && !regionCurrentConf.equals(changePeerSubTask.getNewConfiguration())) {
                Instruction.ChangePeer changePeer = new Instruction.ChangePeer();
                changePeer.setChangePeerSubTask(changePeerSubTask);
                Instruction instruction = new Instruction();
                instruction.setRegion(region);
                instruction.setChangePeer(changePeer);
                event.addInstruction(instruction);
            }
        }

    }
}
