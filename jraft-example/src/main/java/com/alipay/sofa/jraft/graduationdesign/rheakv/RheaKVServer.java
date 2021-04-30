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
package com.alipay.sofa.jraft.graduationdesign.rheakv;

import com.alipay.sofa.jraft.example.rheakv.Node;
import com.alipay.sofa.jraft.graduationdesign.Yaml;
import com.alipay.sofa.jraft.rhea.metrics.KVMetrics;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.codahale.metrics.ConsoleReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class RheaKVServer {
    private static final Logger LOG = LoggerFactory.getLogger(RheaKVServer.class);

    public static void main(String[] args) {
        LOG.info("start rheakv server...");
        LOG.info("rheakv args: {}", Arrays.toString(args));
        if (args.length < 3) {
            LOG.error("[initialServerList], [configPath] are needed.");
        }
        final String initialServerList = args[0];
        final String configPath = args[1];

        final RheaKVStoreOptions opts = Yaml.readRheaKVStoreConfig(configPath);
        opts.setInitialServerList(initialServerList);

        final Node node = new Node(opts);
        node.start();

        ConsoleReporter.forRegistry(KVMetrics.metricRegistry()) //
                .build() //
                .start(30, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(node::stop));
        LOG.info("RheaKV Server start OK, options: {}", opts);
    }
}
