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
package com.alipay.sofa.jraft.rhea.cli;

import com.alipay.sofa.jraft.rhea.client.DefaultRheaKVStore;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.jline.builtins.Completers;
import org.jline.reader.Completer;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.completer.AggregateCompleter;
import org.jline.reader.impl.completer.ArgumentCompleter;
import org.jline.reader.impl.completer.NullCompleter;
import org.jline.reader.impl.completer.StringsCompleter;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;

public class Client implements Runnable {
    private final RheaKVStore rheaKVStore = new DefaultRheaKVStore();
    @CommandLine.Option(names = "--config_path")
    String                    configPath;
    @CommandLine.Option(names = "--init_server_list")
    String                    initialServerList;

    public static RheaKVStoreOptions readConfig(final String name) {
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        final RheaKVStoreOptions opts;
        try {
            opts = mapper.readValue(new File(name), RheaKVStoreOptions.class);
            System.out.println(opts);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return opts;
    }

    private void initRheaKV() {
        final RheaKVStoreOptions opts = readConfig(configPath);
        opts.setInitialServerList(initialServerList);
        if (!rheaKVStore.init(opts)) {
            System.out.println("Fail to init [RheaKVStore]");
            System.exit(-1);
        }
    }

    @Override
    public void run() {
        initRheaKV();
        Terminal terminal = null;
        try {
            terminal = TerminalBuilder.builder().system(true).build();
        } catch (IOException e) {
            e.printStackTrace();
        }

        Completer createCompleter = new ArgumentCompleter(new StringsCompleter("CREATE"),
            new Completers.FileNameCompleter(), NullCompleter.INSTANCE);

        Completer openCompleter = new ArgumentCompleter(new StringsCompleter("OPEN"),
            new Completers.FileNameCompleter(), new StringsCompleter("AS"), NullCompleter.INSTANCE);

        Completer writeCompleter = new ArgumentCompleter(new StringsCompleter("WRITE"), new StringsCompleter("TIME",
            "DATE", "LOCATION"), new StringsCompleter("TO"), NullCompleter.INSTANCE);

        Completer fogCompleter = new AggregateCompleter(createCompleter, openCompleter, writeCompleter);

        LineReader lineReader = LineReaderBuilder.builder().terminal(terminal).completer(fogCompleter).build();

        String prompt = "rheakv> ";
        while (true) {
            String line;
            try {
                line = lineReader.readLine(prompt);
                String[] splits = line.split(" ");
                if (line.startsWith("get")) {
                    String key = splits[1];
                    String result = BytesUtil.readUtf8(rheaKVStore.bGet(BytesUtil.writeUtf8(key)));
                    System.out.println(result);
                } else if (line.startsWith("set")) {
                    String key = splits[1];
                    String value = splits[2];
                    rheaKVStore.bPut(BytesUtil.writeUtf8(key), BytesUtil.writeUtf8(value));
                }
            } catch (UserInterruptException e) {
                // Do nothing
            } catch (EndOfFileException e) {
                System.out.println("\nBye.");
                return;
            }
        }
    }
}
