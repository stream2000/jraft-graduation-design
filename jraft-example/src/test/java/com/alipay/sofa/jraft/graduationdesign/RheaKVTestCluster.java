package com.alipay.sofa.jraft.graduationdesign;

import com.alipay.sofa.jraft.rhea.client.DefaultRheaKVStore;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.client.pd.PlacementDriverClient;
import com.alipay.sofa.jraft.rhea.errors.NotLeaderException;
import com.alipay.sofa.jraft.rhea.options.RegionEngineOptions;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.alipay.sofa.jraft.rhea.util.concurrent.NamedThreadFactory;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.Requires;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;

public class RheaKVTestCluster {
    private static final String[] CONF = { "/kv/rhea_test_1.yaml", //
                                           "/kv/rhea_test_2.yaml", //
                                           "/kv/rhea_test_3.yaml" //
    };
    private static final NamedThreadFactory threadFactory = new NamedThreadFactory("heartbeat_test", true);
    private volatile String                            tempDbPath;
    private volatile String                            tempRaftPath;
    private          CopyOnWriteArrayList<RheaKVStore> stores = new CopyOnWriteArrayList<>();

    public static void main(String[] args) throws Exception {
        final RheaKVTestCluster heartbeatTest = new RheaKVTestCluster();
        heartbeatTest.start();

        final Thread thread = threadFactory.newThread(() -> {
            for (; ; ) {
                heartbeatTest.putAndGetValue();
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                heartbeatTest.shutdown();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));
    }

    protected void start() throws IOException, InterruptedException {
        System.out.println("RheaKVTestCluster init ...");
        File file = new File("rhea_pd_db");
        if (file.exists()) {
            FileUtils.forceDelete(file);
        }
        file = new File("rhea_pd_db");
        if (file.mkdir()) {
            this.tempDbPath = file.getAbsolutePath();
            System.out.println("make dir: " + this.tempDbPath);
        }
        file = new File("rhea_pd_raft");
        if (file.exists()) {
            FileUtils.forceDelete(file);
        }
        file = new File("rhea_pd_raft");
        if (file.mkdir()) {
            this.tempRaftPath = file.getAbsolutePath();
            System.out.println("make dir: " + this.tempRaftPath);
        }

        final Set<Long> regionIds = new HashSet<>();
        for (final String c : CONF) {
            final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            final InputStream in = RheaKVTestCluster.class.getResourceAsStream(c);
            final RheaKVStoreOptions opts = mapper.readValue(in, RheaKVStoreOptions.class);
            for (final RegionEngineOptions rOpts : opts.getStoreEngineOptions().getRegionEngineOptionsList()) {
                regionIds.add(rOpts.getRegionId());
            }
            final RheaKVStore rheaKVStore = new DefaultRheaKVStore();
            if (rheaKVStore.init(opts)) {
                stores.add(rheaKVStore);
            } else {
                System.err.println("Fail to init rhea kv store witch conf: " + c);
            }
        }
        final PlacementDriverClient pdClient = stores.get(0).getPlacementDriverClient();
        for (final Long regionId : regionIds) {
            final Endpoint leader = pdClient.getLeader(regionId, true, 10000);
            System.out.println("The region " + regionId + " leader is: " + leader);
        }
    }

    protected void shutdown() throws IOException {
        System.out.println("RheaKVTestCluster shutdown ...");
        for (RheaKVStore store : stores) {
            store.shutdown();
        }
        if (this.tempDbPath != null) {
            System.out.println("removing dir: " + this.tempDbPath);
            FileUtils.forceDelete(new File(this.tempDbPath));
        }
        if (this.tempRaftPath != null) {
            System.out.println("removing dir: " + this.tempRaftPath);
            FileUtils.forceDelete(new File(this.tempRaftPath));
        }
        System.out.println("RheaKVTestCluster shutdown complete");
    }

    protected RheaKVStore getLeaderStore(long regionId) {
        for (int i = 0; i < 20; i++) {
            for (RheaKVStore store : stores) {
                if (((DefaultRheaKVStore) store).isLeader(regionId)) {
                    return store;
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

    protected RheaKVStore getFollowerStore(long regionId) {
        for (int i = 0; i < 20; i++) {
            for (RheaKVStore store : stores) {
                if (!((DefaultRheaKVStore) store).isLeader(regionId)) {
                    return store;
                }
            }
            System.out.println("fail to find follower, try again");
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
                // ignored
            }
        }
        throw new NotLeaderException("no follower");
    }

    private void putAndGetValue() {
        final RheaKVStore store = getLeaderStore(ThreadLocalRandom.current().nextInt(1, 2));
        final String key = UUID.randomUUID().toString();
        final byte[] value = makeValue(UUID.randomUUID().toString());
        store.bPut(key, value);
        final byte[] newValue = store.bGet(key);
        Assert.assertArrayEquals(value, newValue);
    }

    public static byte[] makeKey(String key) {
        Requires.requireNonNull(key, "key");
        return BytesUtil.writeUtf8(key);
    }

    public static byte[] makeValue(String value) {
        Requires.requireNonNull(value, "value");
        return BytesUtil.writeUtf8(value);
    }
}
