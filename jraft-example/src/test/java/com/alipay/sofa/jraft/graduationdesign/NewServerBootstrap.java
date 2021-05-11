package com.alipay.sofa.jraft.graduationdesign;

import java.io.IOException;

public class NewServerBootstrap extends RheaKVTestBootstrap {
    private static final String[] CONF = { "/kv/rhea_test_4.yaml", //
    };

    public static void main(String[] args) throws Exception {
        final MainRheaKVBootstrap server = new MainRheaKVBootstrap();
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
