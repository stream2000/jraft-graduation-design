package com.alipay.sofa.jraft.graduationdesign;

import com.alipay.sofa.jraft.rhea.PlacementDriverServer;
import com.alipay.sofa.jraft.rhea.errors.NotLeaderException;
import com.alipay.sofa.jraft.rhea.options.PlacementDriverServerOptions;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CopyOnWriteArrayList;

public class PdTestCluster {
    private static final String[]                       CONF         = { "/pd/pd_1.yaml", "/pd/pd_2.yaml",
                                                                         "/pd/pd_3.yaml"                                         };

    private volatile String                                      tempDbPath;
    private volatile String                                      tempRaftPath;
    private          CopyOnWriteArrayList<PlacementDriverServer> pdServerList = new CopyOnWriteArrayList<>();

    protected void start() throws IOException, InterruptedException {
        System.out.println("PlacementDriverServer init ...");
        File file = new File("pd_db");
        if (file.exists()) {
            FileUtils.forceDelete(file);
        }
        file = new File("pd_db");
        if (file.mkdir()) {
            this.tempDbPath = file.getAbsolutePath();
            System.out.println("make dir: " + this.tempDbPath);
        }
        file = new File("pd_raft");
        if (file.exists()) {
            FileUtils.forceDelete(file);
        }
        file = new File("pd_raft");
        if (file.mkdir()) {
            this.tempRaftPath = file.getAbsolutePath();
            System.out.println("make dir: " + this.tempRaftPath);
        }
        for (final String c : CONF) {
            final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            final InputStream in = PdTestCluster.class.getResourceAsStream(c);
            final PlacementDriverServerOptions opts = mapper.readValue(in, PlacementDriverServerOptions.class);
            final PlacementDriverServer pdServer = new PlacementDriverServer();
            if (pdServer.init(opts)) {
                pdServerList.add(pdServer);
            } else {
                System.err.println("Fail to init [PlacementDriverServer] witch conf: " + c);
            }
        }
        pdServerList.get(0).awaitReady(10000);
        System.out.println("Pd server is ready");
    }

    protected void shutdown() throws IOException {
        System.out.println("PlacementDriverServer shutdown ...");
        for (final PlacementDriverServer server : this.pdServerList) {
            server.shutdown();
        }
        if (this.tempDbPath != null) {
            System.out.println("removing dir: " + this.tempDbPath);
            FileUtils.forceDelete(new File(this.tempDbPath));
        }
        if (this.tempRaftPath != null) {
            System.out.println("removing dir: " + this.tempRaftPath);
            FileUtils.forceDelete(new File(this.tempRaftPath));
        }
        System.out.println("PlacementDriverServer shutdown complete");
    }

    protected PlacementDriverServer getLeaderServer() {
        for (int i = 0; i < 20; i++) {
            for (final PlacementDriverServer server : this.pdServerList) {
                if (server.isLeader()) {
                    return server;
                }
            }
            System.out.println("fail to find leader, try again");
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
                // ignored
            }
        }
        throw new NotLeaderException("no leader");
    }

    public static void main(String[] args) throws Exception {
        final PdTestCluster server = new PdTestCluster();
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                server.shutdown();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));
    }
}
