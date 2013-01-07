package com.mapr.franz.server;

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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class ClusterStateTest {
    @Test
    public void testBasics() throws Exception {
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
    public void testClusterBasics() throws Exception {
        final Map<String, byte[]> data = Maps.newConcurrentMap();
        List<Watcher> watchers = Lists.newArrayList();

        new FakeZookeeper(data, watchers);

        Server.Info info1 = new Server.Info(23, Lists.newArrayList(new Client.HostPort("host1", 9090)));
        ClusterState cs1 = new ClusterState("host1:2181", "/franz", info1);

        Server.Info info2 = new Server.Info(25, Lists.newArrayList(new Client.HostPort("host2", 9090)));
        ClusterState cs2 = new ClusterState("host1:2181", "/franz", info2);

        // is info about this server itself OK?
//        assertEquals(info, cs.getLocalInfo());
//
//        assertEquals(1, Iterables.size(cs.getCluster()));
//
//        Catcher.Server server = cs.getCluster().iterator().next();
//        assertEquals(info.getId(), server.getServerId());
//        Iterator<Catcher.Host> i = server.getHostList().iterator();
//        for (Client.HostPort hostPort : info.getAddresses()) {
//            Catcher.Host host = i.next();
//            assertEquals(hostPort.getHost(), host.getHostName());
//            assertEquals(hostPort.getPort(), host.getPort());
//        }
//
//        ClusterState.Target foo = cs.directTo("foo");
//        assertEquals(ClusterState.Status.LIVE, foo.getStatus());
//        assertEquals(1, foo.getGeneration());
//        assertEquals(server, foo.getServer());
    }

    private static class FakeZookeeper extends MockUp<ZooKeeper> {
        private List<Watcher> watchers;
        private Map<String, byte[]> data;
        private Set<String> watched = Sets.newHashSet();

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
        public List<String> getChildren(String path, boolean watch) throws KeeperException.NoNodeException {
            assertTrue(watch);
            watched.add(path);
            if (data.containsKey(path)) {
                final String dirName = path.replaceAll("^(.*?)/?$", "$1/");
                return Lists.newArrayList(Iterables.filter(data.keySet(), new Predicate<String>() {
                    @Override
                    public boolean apply(String input) {
                        return input.startsWith(dirName);
                    }
                }));
            } else {
                throw new KeeperException.NoNodeException(path + " does not exist");
            }
        }
    }
}
