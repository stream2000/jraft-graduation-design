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

import java.io.IOException;

public class MainRheaKVClusterBootstrap extends RheaKVTestBootstrap {

    private static final String[] CONF = { "/kv/rhea_test_1.yaml", //
            "/kv/rhea_test_2.yaml", //
            "/kv/rhea_test_3.yaml" //
                                       };

    public static void main(String[] args) throws Exception {
        final MainRheaKVClusterBootstrap server = new MainRheaKVClusterBootstrap();
        server.start(CONF, false);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                server.shutdown();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));
    }
}
