package com.mapr.franz.catcher;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
        Client c = new Client(new ConnectionFactory() {
            int attempt = 0;

            @Override
            public CatcherConnection create(PeerInfo server) throws IOException {
                if (attempt++ == 0) {
                    // first try fails
                    return null;
                } else {
                    // second retry works
                    return new FakeConnection() {
                        public Catcher.CatcherService.BlockingInterface getService() {
                            return new BasicServer(new SecureRandom().nextLong(), farm);
                        }
                    };
                }
            }
        }, Lists.newArrayList(new PeerInfo("foo", 0)));

        c.sendMessage("topic", "message");
        assertEquals(1, farm.getMessages().size());
        assertEquals("message", farm.getMessages().get(0));
    }

    // verifies redirects are remembered by the client
    // also verifies that server failures are dealt with only a few redirects
    @Test
    public void redirects() throws ServiceException, IOException {
        final ServerFarm farm = new ServerFarm();
        final Map<CatcherConnection, FarmedServer> servermap = Maps.newHashMap();
        final Map<CatcherConnection, PeerInfo> hostmap = Maps.newHashMap();

        new MockUp<RealCatcherConnection>() {
            CatcherConnection it;

            @Mock(maxInvocations = 10)
            public void $init(PeerInfo host1) {
                servermap.put(it, farm.newServer());
                hostmap.put(it, host1);
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

        Client c = new Client(hosts);

        for (int i = 0; i < 1000; i++) {
            int topic = i % 31;
            c.sendMessage(Integer.toString(topic), "message " + i);
        }
        assertEquals(1000, farm.messageCount);
        int firstPassRedirects = farm.redirectCount;
        assertTrue("Should have few redirects", firstPassRedirects < 31);
        assertEquals(10, farm.helloCount);

        FarmedServer x = farm.servers.remove(farm.servers.size() - 1);
        x.emulateServerFailure();

        for (int i = 0; i < 1000; i++) {
            int topic = i % 31;
            c.sendMessage(Integer.toString(topic), "message " + i);
        }

        assertEquals(2000, farm.messageCount);
        assertTrue("Should have few redirects", farm.redirectCount - firstPassRedirects < 31);
        assertEquals(10, farm.helloCount);

        c.close();

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

        public FarmedServer newServer() {
            FarmedServer r = new FarmedServer(servers.size(), this);
            servers.add(r);
            return r;
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
    }

    /**
     * Emulates minimal server function and allows emulation of failures.
     */
    private static class BasicServer implements Catcher.CatcherService.BlockingInterface {
        private final long id;
        private final ServerFarm farm;
        private boolean isDead = false;

        private BasicServer(long id, ServerFarm farm) {
            this.id = id;
            this.farm = farm;
        }

        @Override
        public Catcher.HelloResponse hello(RpcController controller, Catcher.Hello request) throws ServiceException {
            checkForFailure();
            farm.notifyHello(id);

            return Catcher.HelloResponse
                    .newBuilder().setServerId(id)
                    .build();
        }

        @Override
        public Catcher.LogMessageResponse log(RpcController controller, Catcher.LogMessage request) throws ServiceException {
            Preconditions.checkArgument(request.hasPayload());
            checkForFailure();

            String topic = request.getTopic();
            farm.notifyMessage(id, topic, request.getPayload().toStringUtf8());

            Catcher.LogMessageResponse.Builder r = Catcher.LogMessageResponse.newBuilder()
                    .setServerId(id)
                    .setSuccessful(true);
            return r.build();
        }

        public void checkForFailure() throws ServiceException {
            if (isDead) {
                throw new ServiceException("Simulated server shutdown");
            }
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

        public void emulateServerFailure() {
            isDead = true;
        }
    }

    /**
     * Emulates topic redirection.
     */
    private static class FarmedServer extends BasicServer {
        private FarmedServer(long id, ServerFarm farm) {
            super(id, farm);
        }

        @Override
        public Catcher.LogMessageResponse log(RpcController controller, Catcher.LogMessage request) throws ServiceException {
            Preconditions.checkArgument(request.hasPayload());
            checkForFailure();

            String topic = request.getTopic();
            int topicNumber = Integer.parseInt(topic);
            getFarm().notifyMessage(getId(), topic, request.getPayload().toStringUtf8());
            Catcher.LogMessageResponse.Builder r = Catcher.LogMessageResponse.newBuilder()
                    .setServerId(getId())
                    .setSuccessful(true);

            int redirectTo = topicNumber % getFarm().size();
            if (redirectTo != getId()) {
                getFarm().notifyRedirect(getId(), redirectTo);
                Catcher.TopicMapping.Builder redirect = r.getRedirectBuilder();
                redirect.setTopic(Integer.toString(topicNumber));
                redirect.setServerId(redirectTo);
                redirect.addHostBuilder()
                        .setHostName(Long.toString(redirectTo))
                        .setPort(100)
                        .build();
            }
            return r.build();
        }
    }

    // TODO two server topic redirects work correctly (two servers, 100 requests on 10 topics, each topic has at most 1 request to wrong server)
    // TODO three server connect, cluster shrinks, send remaining topics to remaining nodes, minimal redirects
    // TODO three server connect, cluster shrinks, then expands, minimal redirects
}
