/*
 * Copyright MapR Technologies, $year
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mapr.franz.server;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.mapr.franz.catcher.Client;
import com.mapr.franz.catcher.wire.Catcher;
import com.mapr.storm.Utils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class ClusterStateTest {
    static Logger log = LoggerFactory.getLogger(ClusterStateTest.class);

    @Test
    public void testBasics() throws IOException, InterruptedException {
	log.info("testBasics()");
        final Map<String, byte[]> data = Maps.newConcurrentMap();
        List<Watcher> watchers = Lists.newArrayList();

        new FakeZookeeper(data, watchers);

        Server.Info info = new Server.Info(23, Lists.newArrayList(new Client.HostPort("localhost", 9090)));
        ClusterState cs = new ClusterState("localhost:2181", "/franz", info);

        // is info about this server itself OK?
        assertEquals(info, cs.getLocalInfo());

        assertEquals(1, Iterables.size(cs.getCluster()));

        Catcher.Server server = cs.getCluster().iterator().next();
        assertEquals(info.getId(), server.getServerId());
        Iterator<Catcher.Host> i = server.getHostList().iterator();
        for (Client.HostPort hostPort : info.getAddresses()) {
            Catcher.Host host = i.next();
            assertEquals(hostPort.getHost(), host.getHostName());
            assertEquals(hostPort.getPort(), host.getPort());
        }

        ClusterState.Target foo = cs.directTo("foo");
        assertEquals(ClusterState.Status.LIVE, foo.getStatus());
        assertEquals(1, foo.getGeneration());
        assertEquals(server, foo.getServer());
    }

    /*
    * Verifies that servers learn about each other from the state stored in Zookeeper.
     */
    @Test
    public void clusterMemberPropagation() throws IOException, InterruptedException {
	log.info("clusterMemberPropagation()");
        final Map<String, byte[]> data = Collections.synchronizedSortedMap(Maps.<String, byte[]>newTreeMap());
        List<Watcher> watchers = Lists.newArrayList();

        new FakeZookeeper(data, watchers);

        // two servers should find out about each other
        Server.Info info1 = new Server.Info(23, Lists.newArrayList(new Client.HostPort("host1", 9090)));
        ClusterState cs1 = new ClusterState("host1:2181", "/franz", info1);

        Server.Info info2 = new Server.Info(25, Lists.newArrayList(new Client.HostPort("host2", 9090)));
        ClusterState cs2 = new ClusterState("host1:2181", "/franz", info2);

        assertEquals(2, Iterables.size(cs1.getCluster()));
        assertEquals(2, Iterables.size(cs2.getCluster()));

        // and what they learn should be essentially identical
        assertEquals(0, Sets.symmetricDifference(
                Sets.newHashSet(Iterables.transform(cs1.getCluster(), new Function<Catcher.Server, Long>() {
                    @Override
                    public Long apply(Catcher.Server input) {
                        return input.getServerId();
                    }
                }
                )),
                Sets.newHashSet(Iterables.transform(cs2.getCluster(), new Function<Catcher.Server, Long>() {
                    @Override
                    public Long apply(Catcher.Server input) {
                        return input.getServerId();
                    }
                }
                ))).size());

        // verify that both servers agree on who handles what and that each server
        // handles half the topics
        int redirect1 = 0;
        int redirect2 = 0;
        Multiset<String> counts = HashMultiset.create();
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 1000; i++) {
                ClusterState.Target k = cs1.directTo(i + "");
                assertEquals(ClusterState.Status.LIVE, k.getStatus());
                counts.add("i=" + i + ", to=" + k.getServer());

                redirect1 += k.isRedirect() ? 1 : 0;

                k = cs1.directTo(i + "");
                assertEquals(ClusterState.Status.LIVE, k.getStatus());
                counts.add("i=" + i + ", to=" + k.getServer());

                redirect2 += k.isRedirect() ? 1 : 0;
            }
        }

        for (String s : counts.elementSet()) {
            assertEquals(20, counts.count(s));
        }
        assertEquals(1000, counts.elementSet().size());
        assertEquals(5000, redirect1);
        assertEquals(5000, redirect2);
    }


    @Test
    public void testExit() throws IOException, InterruptedException {
	log.info("testExit()");
        final Map<String, byte[]> data = Collections.synchronizedSortedMap(Maps.<String, byte[]>newTreeMap());
        List<Watcher> watchers = Lists.newArrayList();

        new FakeZookeeper(data, watchers);

        // two servers should find out about each other
        Server.Info info1 = new Server.Info(23, Lists.newArrayList(new Client.HostPort("host1", 9090)));
        ClusterState cs1 = new ClusterState("host1:2181", "/franz", info1);

        Server.Info info2 = new Server.Info(25, Lists.newArrayList(new Client.HostPort("host2", 9090)));
        ClusterState cs2 = new ClusterState("host1:2181", "/franz", info2);

        assertEquals(2, Iterables.size(cs1.getCluster()));
        assertEquals(2, Iterables.size(cs2.getCluster()));

        cs1.exit();

        assertEquals(1, Iterables.size(cs2.getCluster()));
        assertEquals(ClusterState.Status.LIVE, cs2.directTo("foo").getStatus());

        // verify that cs2 doesn't redirect but cs1 always does
        int redirect1 = 0;
        int redirect2 = 0;
        Multiset<String> counts = HashMultiset.create();
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 1000; i++) {
		// This seems to me to be an invalid use of the API.  calling any method after exit() should result in failure.
		// Expecting that we get any kind of meaningful results out of cs1 seems incorrect.  It should at most throw an
		// exception saying it was already closed.
                ClusterState.Target k = cs1.directTo(i + "");
                assertEquals(ClusterState.Status.FAILED, k.getStatus());
                counts.add("i=" + i + ", to=" + k.getServer());

                redirect1 += k.isRedirect() ? 1 : 0;

                k = cs2.directTo(i + "");
                assertEquals(ClusterState.Status.LIVE, k.getStatus());
                counts.add("i=" + i + ", to=" + k.getServer());

                redirect2 += k.isRedirect() ? 1 : 0;
            }
        }

        for (String s : counts.elementSet()) {
            assertEquals(20, counts.count(s));
        }
        assertEquals(1000, counts.elementSet().size());
        assertEquals(10000, redirect1);
        assertEquals(0, redirect2);
    }

    @Test
    public void testIdCollision() throws IOException, InterruptedException {
	log.info("testIdCollision()");
        final Map<String, byte[]> data = Collections.synchronizedSortedMap(Maps.<String, byte[]>newTreeMap());
        List<Watcher> watchers = Lists.newArrayList();

        new FakeZookeeper(data, watchers);

        // two servers should find out about each other
        Server.Info info1 = new Server.Info(23, Lists.newArrayList(new Client.HostPort("host1", 9090)));
        ClusterState cs1 = new ClusterState("host1:2181", "/franz", info1);
        assertEquals(1, Iterables.size(cs1.getCluster()));

        Server.Info info2 = new Server.Info(23, Lists.newArrayList(new Client.HostPort("host2", 9090)));
        try {
            new ClusterState("host1:2181", "/franz", info2);
            fail("Should have noticed id duplication");
        } catch (IOException e) {
            assertTrue("Wanted correct message", e.getMessage().startsWith("Server status node"));
        }

        // verify that cs1 still works
        assertEquals(1, Iterables.size(cs1.getCluster()));

        // verify that cs1 now never redirects
        int redirect1 = 0;
        Multiset<String> counts = HashMultiset.create();
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 1000; i++) {
                ClusterState.Target k = cs1.directTo(i + "");
                assertEquals(ClusterState.Status.LIVE, k.getStatus());
                counts.add("i=" + i + ", to=" + k.getServer());

                redirect1 += k.isRedirect() ? 1 : 0;
            }
        }

        for (String s : counts.elementSet()) {
            assertEquals(10, counts.count(s));
        }
        assertEquals(1000, counts.elementSet().size());
        assertEquals(0, redirect1);
    }

    @Test
    public void testDisconnect() throws IOException, InterruptedException {
	log.info("testDisconnect()");
        final Map<String, byte[]> data = Collections.synchronizedMap(Maps.<String, byte[]>newTreeMap());
        List<Watcher> watchers = Lists.newArrayList();

        FakeZookeeper fake = new FakeZookeeper(data, watchers);

        // two servers should find out about each other
        Server.Info info1 = new Server.Info(23, Lists.newArrayList(new Client.HostPort("host1", 9090)));
        ClusterState cs1 = new ClusterState("host1:2181", "/franz", info1);

        Server.Info info2 = new Server.Info(25, Lists.newArrayList(new Client.HostPort("host2", 9090)));
        ClusterState cs2 = new ClusterState("host1:2181", "/franz", info2);

        fake.disconnect(0);

        // nothing fishy yet
        assertEquals(2, Iterables.size(cs1.getCluster()));
        assertEquals(2, Iterables.size(cs2.getCluster()));

        // but cs1 won't give valid redirects
        assertEquals(ClusterState.Status.UNKNOWN, cs1.directTo("foo").getStatus());

        // TODO verify that both servers give best effort, but cs1 acknowledges ignorance
        int redirect1 = 0;
        int redirect2 = 0;
        Multiset<String> counts = HashMultiset.create();
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 1000; i++) {
                ClusterState.Target k = cs1.directTo(i + "");
                assertEquals(ClusterState.Status.UNKNOWN, k.getStatus());
                counts.add("i=" + i + ", to=" + k.getServer());

                redirect1 += k.isRedirect() ? 1 : 0;

                k = cs2.directTo(i + "");
                assertEquals(ClusterState.Status.LIVE, k.getStatus());
                counts.add("i=" + i + ", to=" + k.getServer());

                redirect2 += k.isRedirect() ? 1 : 0;
            }
        }

        for (String s : counts.elementSet()) {
            assertEquals(20, counts.count(s));
        }
        assertEquals(1000, counts.elementSet().size());
        assertEquals(5000, redirect1);
        assertEquals(5000, redirect2);

        // test reconnect
        fake.reconnect(0);

        // verify that both servers work again normally
        redirect1 = 0;
        redirect2 = 0;
        counts = HashMultiset.create();
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 1000; i++) {
                ClusterState.Target k = cs1.directTo(i + "");
                assertEquals(ClusterState.Status.LIVE, k.getStatus());
                counts.add("i=" + i + ", to=" + k.getServer());

                redirect1 += k.isRedirect() ? 1 : 0;

                k = cs2.directTo(i + "");
                assertEquals(ClusterState.Status.LIVE, k.getStatus());
                counts.add("i=" + i + ", to=" + k.getServer());

                redirect2 += k.isRedirect() ? 1 : 0;
            }
        }

        for (String s : counts.elementSet()) {
            assertEquals(20, counts.count(s));
        }
        assertEquals(1000, counts.elementSet().size());
        assertEquals(5000, redirect1);
        assertEquals(5000, redirect2);

        fake.disconnect(0);
        // TODO this breaks because the mocking causes the update not to propagate from one thread to another
        // very mysterious, but essentially undebuggable without some help from the author of jmockit.

        Watcher w = fake.expirePart1(0);

        // the other (unexpired) server should now know it is the only one
        assertEquals(1, Iterables.size(cs2.getCluster()));

        fake.expirePart2(w);

        // notifying the wandering node will cause a reconnection
        assertEquals(2, Iterables.size(cs1.getCluster()));
        assertEquals(2, Iterables.size(cs2.getCluster()));
    }

    private static final int PORT = 45613;

    /*
     * Test session expiration without mocking ZK
     */
    @Test
    public void testExpiration() throws IOException, InterruptedException {
	log.info("testExpiration()");
        ZKS zks = new ZKS();
        // two servers should find out about each other
        Server.Info info1 = new Server.Info(23, Lists.newArrayList(new Client.HostPort("host1", 9090)));
        ClusterState cs1 = new ClusterState(zks.connectString(), "/franz", info1);

        Server.Info info2 = new Server.Info(25, Lists.newArrayList(new Client.HostPort("host2", 9090)));
        ClusterState cs2 = new ClusterState(zks.connectString(), "/franz", info2);

        long id = cs2.getZk().getSessionId();
//        byte[] pass = cs2.getZk().getSessionPasswd();

        Thread.sleep(3000);
        System.out.printf("\n\n\n\n\n\n\n");

        log.info("Ending session {}", id);
        zks.zks.closeSession(id);
//        ZooKeeper hammer = new ZooKeeper("localhost", PORT, null, id, pass);
//        hammer.close();


        Thread.sleep(5000);

//        cs1.exit();

        assertEquals(2, Iterables.size(cs1.getCluster()));

        zks.shutdown();
    }


    public static class ZKS {

        private final File logdir;
        private final File snapdir;
        private final ZooKeeperServer zks;

        public ZKS() throws IOException, InterruptedException {
            snapdir = Files.createTempDir();
            logdir = Files.createTempDir();
            zks = new ZooKeeperServer(snapdir, logdir, 100);
            NIOServerCnxn.Factory f = new NIOServerCnxn.Factory(new InetSocketAddress(PORT));
            f.startup(zks);
            waitForServerUp(PORT, 1000);
        }

        public void shutdown() throws IOException {
            zks.shutdown();
            waitForServerDown(PORT, 1000);

            Utils.deleteRecursively(logdir);
            assertFalse(logdir.exists());

            Utils.deleteRecursively(snapdir);
            assertFalse(snapdir.delete());
        }

        public String connectString() {
            return "localhost:" + zks.getClientPort();
        }

        public static boolean waitForServerUp(int port, long timeout) {
            long start = System.currentTimeMillis();
            while (true) {
                try {
                    // if there are multiple hostports, just take the first one
                    String result = send4LetterWord("localhost", port, "stat");
                    if (result.startsWith("Zookeeper version:")) {
                        return true;
                    }
                } catch (IOException e) {
                    // ignore as this is expected
                    log.warn("server " + "localhost:" + port + " not up " + e);
                }

                if (System.currentTimeMillis() > start + timeout) {
                    break;
                }
                try {
                    Thread.sleep(250);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            return false;
        }

        public static boolean waitForServerDown(int port, long timeout) {
            long start = System.currentTimeMillis();
            while (true) {
                try {
                    send4LetterWord("localhost", port, "stat");
                } catch (IOException e) {
                    return true;
                }

                if (System.currentTimeMillis() > start + timeout) {
                    break;
                }
                try {
                    Thread.sleep(250);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            return false;
        }

        public static String send4LetterWord(String host, int port, String cmd) throws IOException {
            log.warn("connecting to " + host + " " + port);
            Socket sock = new Socket(host, port);
            BufferedReader reader = null;
            try {
                OutputStream outstream = sock.getOutputStream();
                outstream.write(cmd.getBytes());
                outstream.flush();
                // this replicates NC - close the output stream before reading
                sock.shutdownOutput();

                reader = new BufferedReader(
                        new InputStreamReader(sock.getInputStream()));
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line).append("\n");
                }
                return sb.toString();
            } finally {
                sock.close();
                if (reader != null) {
                    reader.close();
                }
            }
        }
    }

    // TODO test redirect

    // TODO failed connection

    // TODO test that failures propagate in good order

    /**
     * This is a basic implementation that works kind of like Zookeeper.  Some huge simplifying assumptions have
     * been made:
     * <p/>
     * - only a few calls are made and those are made with only a few possible values of parameters
     * <p/>
     * - only getChildren is used with watchers and all clients watch the same directories.
     */
    private static class FakeZookeeper extends MockUp<ZooKeeper> {
        private static List<Watcher> watchers;
        private static List<String> serverIds;
        private static Map<String, byte[]> data;
        private static Set<String> watched = Sets.newHashSet();

        private ZooKeeper it;

        private FakeZookeeper(Map<String, byte[]> data, List<Watcher> watchers) {
            this.data = data;
            this.watchers = watchers;
            this.serverIds = Lists.newArrayList();
        }

        @Mock
        public void $init(String connectString, int sessionTimeout, Watcher watcher) {
            this.watchers.add(watcher);
        }

        @Mock
        public String create(String znode, byte[] content, List<ACL> acl, CreateMode createMode) throws KeeperException.NodeExistsException {
            if (!znode.equals("/franz")) {
                serverIds.add(znode);
            }
            if (data.containsKey(znode)) {
                throw new KeeperException.NodeExistsException(znode + " already exists");
            } else {
                data.put(znode, Arrays.copyOf(content, content.length));

                String dir = znode.replaceAll("^(.*?)(/[^/]*)?$", "$1");
                if (watched.contains(dir)) {
                    for (Watcher watcher : watchers) {
                        watcher.process(new WatchedEvent(Watcher.Event.EventType.NodeChildrenChanged, Watcher.Event.KeeperState.SyncConnected, dir));
                    }
                }
                return znode;
            }
        }

        @Mock
        public byte[] getData(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
            assertFalse(watch);
            assertNull(stat);

            if (data.containsKey(path)) {
                return data.get(path);
            } else {
                throw new KeeperException.NoNodeException(path + " does not exist");
            }
        }

        @Mock
        public void delete(final String path, int version) throws InterruptedException, KeeperException {
            if (data.containsKey(path)) {
                if (watched.contains(path)) {
                    watched.remove(path);
                    throw new RuntimeException("Can't watch files with FakeZookeeper");
                }
                data.remove(path);
                String dir = path.replaceAll("^(.*?)(/[^/]*)?$", "$1");
                if (watched.contains(dir)) {
                    for (Watcher watcher : watchers) {
                        watcher.process(new WatchedEvent(Watcher.Event.EventType.NodeChildrenChanged, Watcher.Event.KeeperState.SyncConnected, dir));
                    }
                }
            } else {
                throw new KeeperException.NoNodeException(path + " not found");
            }
        }

        @Mock
        public List<String> getChildren(String path, boolean watch) throws KeeperException.NoNodeException {
            assertTrue(watch);
            watched.add(path);
            if (data.containsKey(path)) {
                final String dirName = path.replaceAll("^(.*?)/?$", "$1/");
                synchronized (this) {
                    System.out.printf("%s[%s] = %s\n", Thread.currentThread().getId(), System.identityHashCode(data), data);
                    List<String> r = Lists.newArrayList(Iterables.transform(
                            Iterables.filter(Sets.newTreeSet(data.keySet()), new Predicate<String>() {
                                @Override
                                public boolean apply(String input) {
                                    return input.startsWith(dirName);
                                }
                            }), new Function<String, String>() {
                        @Override
                        public String apply(String s) {
                            return s.substring(s.lastIndexOf("/") + 1, s.length());
                        }
                    }));
                    return r;
                }
            } else {
                throw new KeeperException.NoNodeException(path + " does not exist");
            }
        }

        @Mock
        public synchronized void close() throws InterruptedException {
        }

        public void disconnect(int which) {
            watchers.get(which).process(new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.Disconnected, null));
        }

        public void reconnect(int which) {
            watchers.get(which).process(new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.SyncConnected, null));
        }

        public Watcher expirePart1(int which) {
            String path = serverIds.get(which);
            System.out.printf("before %s[%s] = %s\n", Thread.currentThread().getId(), System.identityHashCode(data), data);
            data.remove(path);
            System.out.printf("after %s[%s] = %s\n", Thread.currentThread().getId(), System.identityHashCode(data), data);
            Watcher oldWatch = watchers.remove(which);

            // notify all other nodes that node "which" has disappeared
            String dir = path.replaceAll("^(.*?)(/[^/]*)?$", "$1");
            if (watched.contains(dir)) {
                for (Watcher watcher : Lists.newArrayList(watchers)) {
                    watcher.process(new WatchedEvent(Watcher.Event.EventType.NodeChildrenChanged, Watcher.Event.KeeperState.SyncConnected, dir));
                }
            }
            return oldWatch;
        }

        public void expirePart2(Watcher oldWatch) {
            // now process the expiration notice itself on node "which".  This happens after the other notification to
            // emulate what happens when a partitioned node comes back.
            oldWatch.process(new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.Expired, null));
        }

        @Mock
        public String toString() {
            return Iterables.transform(data.keySet(), new Function<String, Object>() {
                @Override
                public Object apply(String s) {
                    if (s == null) {
                        return "";
                    } else if (s.length() > 6) {
                        return s.substring(s.length() - 5, s.length());
                    } else {
                        return s;
                    }
                }
            }).toString();
        }
    }
}
