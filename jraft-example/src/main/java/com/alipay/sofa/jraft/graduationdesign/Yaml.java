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

import com.alipay.sofa.jraft.rhea.options.PlacementDriverServerOptions;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

/**
 * @author jiachun.fjc
 */
public class Yaml {

    public static void main(final String[] args) {
        readRheaKVStoreConfig(Paths.get("jraft-example", "config", "benchmark_client.yaml").toString());
        readRheaKVStoreConfig(Paths.get("jraft-example", "config", "benchmark_server.yaml").toString());
    }

    public static RheaKVStoreOptions readRheaKVStoreConfig(final String name) {
        return readConfig(name, RheaKVStoreOptions.class);
    }

    public static <T> T readConfig(final String name, Class<T> clazz) {
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        final T opts;
        try {
            opts = mapper.readValue(new File(name), clazz);
            System.out.println(opts);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return opts;
    }

    public static PlacementDriverServerOptions readPdConfig(final String name) {
        return readConfig(name, PlacementDriverServerOptions.class);
    }

}
