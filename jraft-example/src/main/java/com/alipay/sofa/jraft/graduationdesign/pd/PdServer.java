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
package com.alipay.sofa.jraft.graduationdesign.pd;

import com.alipay.sofa.jraft.graduationdesign.Yaml;
import com.alipay.sofa.jraft.rhea.PlacementDriverServer;
import com.alipay.sofa.jraft.rhea.metrics.KVMetrics;
import com.alipay.sofa.jraft.rhea.options.PlacementDriverServerOptions;
import com.codahale.metrics.ConsoleReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class PdServer {

    private static final Logger LOG = LoggerFactory.getLogger(PdServer.class);

    public static void main(String[] args) {
        LOG.info("start pd server...");
        LOG.info("pd args: {}", Arrays.toString(args));

        final String configPath = args[1];

        final PlacementDriverServerOptions opts = Yaml.readPdConfig(configPath);

        final PlacementDriverServer pdServer = new PlacementDriverServer();
        pdServer.init(opts);
        pdServer.awaitReady(10000);

        ConsoleReporter.forRegistry(KVMetrics.metricRegistry()) //
                .build() //
                .start(30, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(pdServer::shutdown));
        LOG.info("Pd Server start OK, options: {}", opts);
    }
}
