package com.mapr.franz.server;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.mapr.franz.catcher.Client;
import com.mapr.franz.catcher.wire.Catcher;
import mockit.Mock;
import mockit.MockUp;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class ClusterStateTest {
    @Test
    public void testBasics() throws IOException, InterruptedException {
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

    @Test
    public void testClusterMemberPropagation() throws IOException, InterruptedException {
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

        // TODO check that both servers redirect consistently
    }


    @Test
    public void testExit() throws IOException, InterruptedException {
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

        // TODO verify that cs2 now never redirects
    }

    @Test
    public void testIdCollision() throws IOException, InterruptedException {
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

        // TODO verify that cs1 now never redirects
    }

    @Test
    public void testDisconnect() throws IOException, InterruptedException {
        final Map<String, byte[]> data = Collections.synchronizedSortedMap(Maps.<String, byte[]>newTreeMap());
        List<Watcher> watchers = Lists.newArrayList();

        FakeZookeeper fake = new FakeZookeeper(data, watchers);

        // two servers should find out about each other
        Server.Info info1 = new Server.Info(23, Lists.newArrayList(new Client.HostPort("host1", 9090)));
        ClusterState cs1 = new ClusterState("host1:2181", "/franz", info1);

        Server.Info info2 = new Server.Info(25, Lists.newArrayList(new Client.HostPort("host2", 9090)));
        ClusterState cs2 = new ClusterState("host1:2181", "/franz", info2);

        fake.disconnect(0);

        // nothing up yet
        assertEquals(2, Iterables.size(cs1.getCluster()));
        assertEquals(2, Iterables.size(cs2.getCluster()));

        // TODO but cs1 won't give valid redirects
        assertEquals(ClusterState.Status.UNKNOWN, cs1.directTo("foo").getStatus());

    }

    // TODO test disconnect, reconnect
    // TODO test session expiration

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
        private List<Watcher> watchers;
        private Map<String, byte[]> data;
        private Set<String> watched = Sets.newHashSet();

        private ZooKeeper it;

        private FakeZookeeper(Map<String, byte[]> data, List<Watcher> watchers) {
            this.data = data;
            this.watchers = watchers;
        }

        @Mock
        public void $init(String connectString, int sessionTimeout, Watcher watcher) {
            this.watchers.add(watcher);
        }

        @Mock
        public String create(String znode, byte[] content, List<ACL> acl, CreateMode createMode) throws KeeperException.NodeExistsException {
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
                return Lists.newArrayList(Iterables.filter(Sets.newTreeSet(data.keySet()), new Predicate<String>() {
                    @Override
                    public boolean apply(String input) {
                        return input.startsWith(dirName);
                    }
                }));
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

        public void expire(int which) {
            watchers.get(which).process(new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.Expired, null));
        }
    }
}
