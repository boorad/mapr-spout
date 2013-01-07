package com.mapr.franz.catcher;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.mapr.franz.catcher.wire.Catcher;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class ClientTest {
    // check for failure if we can't get to any server
    @Test
    public void noServer() throws ServiceException, IOException {
        Client c = new Client(new ConnectionFactory() {
            @Override
            public CatcherConnection create(PeerInfo server) {
                return null;
            }
        }, Lists.newArrayList(new PeerInfo("foo", 0)));
        try {
            c.sendMessage("topic", "message");
            fail("Should have failed with IOException");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("No catcher servers"));
        }
    }

    // check that we try a second time and that we get a good result the second time
    @Test
    public void oneServerRetry() throws ServiceException, IOException {
        final ServerFarm farm = new ServerFarm();
        new MockUp<CatcherConnection>() {
            CatcherConnection it;

            @Mock(maxInvocations = 10)
            public void $init(PeerInfo host) throws IOException {
                it.setServer(host);
            }

            @Mock
            public Catcher.CatcherService.BlockingInterface getService() {
                return new FarmedServer(new Client.HostPort(new PeerInfo("foo", 123)), new SecureRandom().nextLong(), farm);
            }

            @Mock
            public String toString() {
                return "MockConnection(" + it.getServer() + ")";
            }
        };

        Client c = new Client(new ConnectionFactory() {
            int retry = 0;

            @Override
            public CatcherConnection create(PeerInfo server) {
                if (retry++ == 0) {
                    return null;
                } else {
                    return CatcherConnection.connect(server);
                }
            }
        }, Lists.newArrayList(new PeerInfo("foo", 0)));

        c.sendMessage("3", "message");
        assertEquals(1, farm.getMessages().size());
        assertEquals("message", farm.getMessages().get(0));
    }

    // verifies redirects are remembered by the client
    // also verifies that server failures are dealt with only a few redirects
    // also verifies that overall transaction counts are evenly distributed
    @Test
    public void redirects() throws ServiceException, IOException {
        final ServerFarm farm = new ServerFarm();
        final Map<CatcherConnection, FarmedServer> servermap = Maps.newHashMap();
        final Map<CatcherConnection, PeerInfo> hostmap = Maps.newHashMap();

        new MockUp<CatcherConnection>() {
            CatcherConnection it;

            @Mock(maxInvocations = 10)
            public void $init(PeerInfo host) throws IOException {
                servermap.put(it, farm.newServer(new Client.HostPort(host)));
                hostmap.put(it, host);
                it.setServer(host);
            }

            @Mock
            public Catcher.CatcherService.BlockingInterface getService() {
                return servermap.get(it);
            }

            @Mock
            public String toString() {
                return "MockConnection(" + hostmap.get(it) + ")";
            }
        };

        List<PeerInfo> hosts = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            hosts.add(new PeerInfo(Integer.toString(i), 100));
        }
//        for (int i = 0; i < 10; i++) {
//            farm.newServer(new Client.HostPort(Integer.toString(i), 100));
//        }

        Client c = new Client(hosts);

        for (int i = 0; i < 600; i++) {
            int topic = i % 30;
            c.sendMessage(Integer.toString(topic), "message " + i);
        }

        assertEquals(600, countMessages(farm.servers));
        assertEquals(600, farm.messageCount);
        int firstPassRedirects = farm.redirectCount;
        assertTrue("Should have few redirects, got " + firstPassRedirects, firstPassRedirects <= 30);
        assertEquals(10, farm.helloCount);

        // assert rough balance of traffic
        for (FarmedServer server : farm.servers) {
            assertEquals(600 / 10, server.getMessageCount(), 15);
        }

        // kill 4 servers.  That leaves 6 which still divides 30 topics evenly.
        List<FarmedServer> dead = Lists.newArrayList();
        dead.addAll(farm.servers.subList(6, 10));
        farm.servers = farm.servers.subList(0, 6);
        int deadCount = countMessages(dead);
        for (FarmedServer server : dead) {
            server.emulateServerFailure();
        }

        for (int i = 0; i < 600; i++) {
            int topic = i % 30;
            c.sendMessage(Integer.toString(topic), "message " + i);
        }

        assertEquals(2 * 600, farm.messageCount);
        assertEquals(deadCount, countMessages(dead));
        assertEquals(2 * 600, countMessages(Iterables.concat(farm.servers, dead)));

        // and again, should have rough balance.
        for (FarmedServer server : farm.servers) {
            assertEquals(600 / 10 + 600 / 6, server.getMessageCount(), 24);
        }

        assertEquals(2 * 600, farm.messageCount);
        assertTrue("Should have few redirects", farm.redirectCount - firstPassRedirects <= 30);
        assertEquals(10, farm.helloCount);

        c.close();
    }

    // TODO add test to see how new hosts discovered during hello are handled.

    private int countMessages(Iterable<FarmedServer> servers) {
        int count = 0;
        for (FarmedServer server : servers) {
            count += server.getMessageCount();
        }
        return count;
    }

    /**
     * Keeps track of a bunch of FarmedServer's and the associated transaction counts.
     */
    private static class ServerFarm implements Iterable<FarmedServer> {
        private Logger logger = LoggerFactory.getLogger(String.class);

        private List<FarmedServer> servers = Lists.newArrayList();
        private int redirectCount = 0;
        private int messageCount = 0;
        private int helloCount = 0;
        final List<String> messages = Lists.newArrayList();
        private Set<Client.HostPort> deadHosts = Sets.newHashSet();

        public FarmedServer newServer(Client.HostPort hostPort) throws IOException {
            if (!deadHosts.contains(hostPort)) {
                FarmedServer r = new FarmedServer(hostPort, servers.size(), this);
                servers.add(r);
                return r;
            } else {
                throw new IOException("Connection refused :-)");
            }
        }

        public int size() {
            return servers.size();
        }

        public void notifyHello(long id) {
            logger.debug("Hello {}", id);
            helloCount++;
        }

        public void notifyClose(long id) {
            logger.debug("Closing {}", id);
        }

        public void notifyMessage(long id, String topic, String message) {
            logger.debug("Message received at {} on topic {}", id, topic);
            messages.add(message);
            messageCount++;
        }

        public void notifyRedirect(long from, long to) {
            logger.debug("Redirect from {} to {}", from, to);
            redirectCount++;
        }

        @Override
        public Iterator<FarmedServer> iterator() {
            return servers.iterator();
        }

        public List<String> getMessages() {
            return messages;
        }

        public void recordDeadHost(Client.HostPort hostPort) {
            deadHosts.add(hostPort);
        }
    }

    /**
     * Emulates minimal server function and allows emulation of failures.
     */
    private static class FarmedServer implements Catcher.CatcherService.BlockingInterface {
        protected int helloCount = 0;
        protected int redirectCount = 0;
        protected int messageCount = 0;
        private Client.HostPort hostPort;
        private final long id;
        private final ServerFarm farm;
        private boolean isDead = false;

        private FarmedServer(Client.HostPort hostPort, long id, ServerFarm farm) {
            this.hostPort = hostPort;
            this.id = id;
            this.farm = farm;
        }

        @Override
        public Catcher.HelloResponse hello(RpcController controller, Catcher.Hello request) throws ServiceException {
            checkForFailure();
            helloCount++;
            if (farm != null) {
                farm.notifyHello(id);
            }

            Catcher.HelloResponse.Builder r = Catcher.HelloResponse
                    .newBuilder().setServerId(id);

            for (FarmedServer server : farm) {
                Catcher.Server.Builder s = r.addClusterBuilder();
                s.setServerId(server.getId());
                s.addHostBuilder().setHostName(server.getHost()).setPort(server.getPort()).build();
                s.build();
            }
            return r.build();
        }

        @Override
        public Catcher.LogMessageResponse log(RpcController controller, Catcher.LogMessage request) throws ServiceException {
            Preconditions.checkArgument(request.hasPayload());
            checkForFailure();


            String topic = request.getTopic();
            farm.notifyMessage(id, topic, request.getPayload().toStringUtf8());

            int topicNumber = Integer.parseInt(topic);
            messageCount++;
            Catcher.LogMessageResponse.Builder r = Catcher.LogMessageResponse.newBuilder()
                    .setServerId(id)
                    .setSuccessful(true);

            if (farm.size() != 0) {
                FarmedServer redirectServer = farm.servers.get(topicNumber % farm.size());
                long redirectTo = redirectServer.getId();
                if (redirectTo != getId()) {
                    redirectCount++;
                    getFarm().notifyRedirect(getId(), redirectTo);
                    Catcher.TopicMapping.Builder redirect = r.getRedirectBuilder();
                    redirect.setTopic(Integer.toString(topicNumber));
                    Catcher.Server.Builder server = redirect.getServerBuilder().setServerId(redirectTo);
                    server.addHostBuilder()
                            .setHostName(redirectServer.getHost())
                            .setPort(redirectServer.getPort())
                            .build();
                    server.build();
                }
            }
            return r.build();
        }

        @Override
        public Catcher.CloseResponse close(RpcController controller, Catcher.Close request) throws ServiceException {
            checkForFailure();
            farm.notifyClose(id);
            return Catcher.CloseResponse.newBuilder().setServerId(id).build();
        }

        public ServerFarm getFarm() {
            return farm;
        }

        public long getId() {
            return id;
        }

        public void checkForFailure() throws ServiceException {
            if (isDead) {
                throw new ServiceException("Simulated server shutdown");
            }
        }

        public void emulateServerFailure() {
            farm.recordDeadHost(hostPort);
            isDead = true;
        }

        public String getHost() {
            return hostPort.getHost();
        }

        public int getPort() {
            return hostPort.getPort();
        }

        public int getMessageCount() {
            return messageCount;
        }
    }

    // TODO two server topic redirects work correctly (two servers, 100 requests on 10 topics, each topic has at most 1 request to wrong server)
    // TODO three server connect, cluster shrinks, send remaining topics to remaining nodes, minimal redirects
    // TODO three server connect, cluster shrinks, then expands, minimal redirects
}
