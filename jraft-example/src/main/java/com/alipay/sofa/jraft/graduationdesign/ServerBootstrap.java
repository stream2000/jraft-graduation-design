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

import com.alipay.sofa.jraft.graduationdesign.pd.PdServer;
import com.alipay.sofa.jraft.graduationdesign.rheakv.RheaKVServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class ServerBootstrap {
    private static final Logger LOG = LoggerFactory.getLogger(ServerBootstrap.class);

    public static void main(final String[] args) {
        System.out.println(Arrays.toString(args));
        if (args.length < 1) {
            LOG.error("Invalid args.");
            System.exit(-1);
        }
        final String role = args[2];
        if ("pd".equals(role)) {
            PdServer.main(args);
        } else if ("rheakv".equals(role)) {
            RheaKVServer.main(args);
        } else {
            LOG.error("Invalid args[0]: {}", role);
            System.exit(-1);
        }
    }
}
