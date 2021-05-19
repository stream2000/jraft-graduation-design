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
import org.fusesource.jansi.AnsiConsole;
import org.jline.console.SystemRegistry;
import org.jline.console.impl.Builtins;
import org.jline.console.impl.SystemRegistryImpl;
import org.jline.keymap.KeyMap;
import org.jline.reader.Binding;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.MaskingCallback;
import org.jline.reader.Parser;
import org.jline.reader.Reference;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultParser;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.widget.TailTipWidgets;
import picocli.CommandLine;
import picocli.shell.jline3.PicocliCommands;
import picocli.shell.jline3.PicocliCommands.PicocliCommandsFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Supplier;

public class Client {
    private static final RheaKVStore rheaKVStore = new DefaultRheaKVStore();

    public static RheaKVStoreOptions readConfig(final String name) {
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        final RheaKVStoreOptions opts;
        try {
            opts = mapper.readValue(new File(name), RheaKVStoreOptions.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return opts;
    }

    public static void main(String[] args) {
        AnsiConsole.systemInstall();
        try {
            Supplier<Path> workDir = () -> Paths.get(System.getProperty("user.dir"));
            // set up JLine built-in commands
            Builtins builtins = new Builtins(workDir, null, null);
            builtins.rename(Builtins.Command.TTOP, "top");
            builtins.alias("zle", "widget");
            builtins.alias("bindkey", "keymap");
            // set up picocli commands
            CliCommands commands = new CliCommands();

            PicocliCommandsFactory factory = new PicocliCommandsFactory();
            // Or, if you have your own factory, you can chain them like this:
            // MyCustomFactory customFactory = createCustomFactory(); // your application custom factory
            // PicocliCommandsFactory factory = new PicocliCommandsFactory(customFactory); // chain the factories

            CommandLine cmd = new CommandLine(commands, factory);
            PicocliCommands picocliCommands = new PicocliCommands(cmd);

            Parser parser = new DefaultParser();
            try (Terminal terminal = TerminalBuilder.builder().build()) {
                SystemRegistry systemRegistry = new SystemRegistryImpl(parser, terminal, workDir, null);
                systemRegistry.setCommandRegistries(builtins, picocliCommands);
                systemRegistry.register("help", picocliCommands);

                LineReader reader = LineReaderBuilder.builder().terminal(terminal).completer(systemRegistry.completer())
                        .parser(parser).variable(LineReader.LIST_MAX, 50)   // max tab completion candidates
                        .build();
                builtins.setLineReader(reader);
                factory.setTerminal(terminal);
                TailTipWidgets widgets = new TailTipWidgets(reader, systemRegistry::commandDescription, 5,
                        TailTipWidgets.TipType.COMPLETER);
                widgets.enable();
                KeyMap<Binding> keyMap = reader.getKeyMaps().get("main");
                keyMap.bind(new Reference("tailtip-toggle"), KeyMap.alt("s"));

                String prompt = "rheakv> ";
                String rightPrompt = null;

                // start the shell and process input until the user quits with Ctrl-D
                String line;
                while (true) {
                    try {
                        systemRegistry.cleanUp();
                        line = reader.readLine(prompt, rightPrompt, (MaskingCallback) null, null);
                        systemRegistry.execute(line);
                    } catch (UserInterruptException e) {
                        // Ignore
                    } catch (EndOfFileException e) {
                        return;
                    } catch (Exception e) {
                        systemRegistry.trace(e);
                    }
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            AnsiConsole.systemUninstall();
        }
    }

    @CommandLine.Command(name = "", description = {
            "Example interactive shell with completion and autosuggestions. "
                    + "Hit @|magenta <TAB>|@ to see available commands.", "Hit @|magenta ALT-S|@ to toggle tailtips.",
            "" }, footer = { "", "Press Ctrl-D to exit." }, subcommands = { GetCommand.class, SetCommand.class,
            ConnectCommand.class, PicocliCommands.ClearScreen.class, CommandLine.HelpCommand.class })
    public static class CliCommands implements Runnable {
        private final static RheaKVStore rheaKVStore = new DefaultRheaKVStore();
        PrintWriter                      out;

        CliCommands() {
        }

        @Override
        public void run() {
            out.println(new CommandLine(this).getUsageMessage());
        }
    }

    @CommandLine.Command(name = "get", subcommands = { CommandLine.HelpCommand.class })
    public static class GetCommand implements Runnable {
        @CommandLine.ParentCommand
        CliCommands    parent;
        @CommandLine.Option(names = { "-k", "--key" }, description = "the key to get", required = true)
        private String key;

        @Override
        public void run() {
            String result = BytesUtil.readUtf8(rheaKVStore.bGet(BytesUtil.writeUtf8(key)));
            System.out.println(result);
        }
    }

    @CommandLine.Command(name = "connect", subcommands = { CommandLine.HelpCommand.class })
    public static class ConnectCommand implements Runnable {
        @CommandLine.ParentCommand
        CliCommands parent;
        @CommandLine.Option(names = "--config_path")
        String      configPath;
        @CommandLine.Option(names = "--init_server_list", defaultValue = "")
        String      initialServerList;

        @Override
        public void run() {
            final RheaKVStoreOptions opts = readConfig(configPath);
            if (opts.getInitialServerList() == null) {
                opts.setInitialServerList(initialServerList);
            }
            if (!rheaKVStore.init(opts)) {
                System.out.println("Fail to init [RheaKVStore]");
                System.exit(-1);
            }
        }
    }

    @CommandLine.Command(name = "set", subcommands = { CommandLine.HelpCommand.class })
    public static class SetCommand implements Runnable {
        @CommandLine.ParentCommand
        CliCommands    parent;
        @CommandLine.Option(names = { "-k", "--key" }, description = "the key to get", required = true)
        private String key;

        @CommandLine.Option(names = { "-v", "--value" }, description = "the value to set", required = true)
        private String value;

        @Override
        public void run() {
            rheaKVStore.bPut(BytesUtil.writeUtf8(key), BytesUtil.writeUtf8(value));
        }
    }
}
