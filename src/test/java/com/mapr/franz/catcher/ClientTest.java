package com.mapr.franz.catcher;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.mapr.franz.catcher.wire.Catcher;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ClientTest {
    // check for failure if we can't get to any server
    @Test
    public void noServer() throws ServiceException, IOException {
        new MockUp<CatcherConnection>() {
            @Mock
            public CatcherConnection connect(PeerInfo ignore) {
                return null;
            }
        };

        Client c = new Client(Lists.newArrayList(new PeerInfo("foo", 0)));
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
        final List<String> messages = Lists.newArrayList();
        new MockUp<CatcherConnection>() {
            int attempt = 0;

            @Mock
            public void $init(PeerInfo server) {
                Assert.assertNotNull(server);
            }

            @Mock
            public CatcherConnection connect(PeerInfo server) throws IOException {
                if (attempt++ == 0) {
                    return null;
                } else {
                    return new CatcherConnection(server);
                }
            }

            @Mock
            public Catcher.CatcherService.BlockingInterface getService() {
                return new Catcher.CatcherService.BlockingInterface() {
                    private final long id = new SecureRandom().nextLong();

                    @Override
                    public Catcher.HelloResponse hello(RpcController controller, Catcher.Hello request) throws ServiceException {
                        return Catcher.HelloResponse
                                .newBuilder().setServerId(id)
                                .build();
                    }

                    @Override
                    public Catcher.LogMessageResponse log(RpcController controller, Catcher.LogMessage request) throws ServiceException {
                        Preconditions.checkArgument(request.hasPayload());
                        messages.add(request.getPayload().toStringUtf8());

                        return Catcher.LogMessageResponse.newBuilder()
                                .setServerId(id)
                                .setSuccessful(true)
                                .build();
                    }

                    @Override
                    public Catcher.CloseResponse close(RpcController controller, Catcher.Close request) throws ServiceException {
                        throw new UnsupportedOperationException("Default operation");
                    }
                };
            }
        };

        Client c = new Client(Lists.newArrayList(new PeerInfo("foo", 0)));
        c.sendMessage("topic", "message");
        assertEquals(1, messages.size());
        assertEquals("message", messages.get(0));
    }


    // verifies redirects are respected by the client
    @Test
    public void redirects() throws ServiceException, IOException {
        final ServerFarm farm = new ServerFarm();

        new MockUp<CatcherConnection>() {
            public FarmedServer server;

            @Mock
            public void $init(PeerInfo host) {
                server = farm.newServer();
            }

            @Mock
            public Catcher.CatcherService.BlockingInterface getService() {
                return server;
            }
        };

        List<PeerInfo> hosts = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            hosts.add(new PeerInfo(Integer.toString(i), 100));
        }

        Client c = new Client(hosts);

        for (int i = 0; i < 10000; i++) {
            int topic = i % 31;
            c.sendMessage(Integer.toString(topic), "message " + i);
        }

        c.close();

        assertEquals(10000, farm.messageCount);
        assertTrue("Should have few redirects", farm.redirectCount < 35);
        assertEquals(10, farm.helloCount);
        assertEquals(10, farm.closeCount);
    }

    private static class ServerFarm implements Iterable<FarmedServer> {
        private Logger logger = LoggerFactory.getLogger(String.class);

        private List<FarmedServer> servers = Lists.newArrayList();
        private int redirectCount = 0;
        private int messageCount = 0;
        private int helloCount = 0;
        private int closeCount = 0;

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
            closeCount++;
        }

        public void notifyMessage(long id, int topic) {
            logger.debug("Message received at {} on topic {}", id, topic);
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
    }

    private static class FarmedServer implements Catcher.CatcherService.BlockingInterface {
        private final long id;
        private final ServerFarm farm;

        private FarmedServer(long id, ServerFarm farm) {
            this.id = id;
            this.farm = farm;
        }

        @Override
        public Catcher.HelloResponse hello(RpcController controller, Catcher.Hello request) throws ServiceException {
            farm.notifyHello(id);

            return Catcher.HelloResponse
                    .newBuilder().setServerId(id)
                    .build();
        }

        @Override
        public Catcher.LogMessageResponse log(RpcController controller, Catcher.LogMessage request) throws ServiceException {
            Preconditions.checkArgument(request.hasPayload());

            int topic = Integer.parseInt(request.getTopic());
            farm.notifyMessage(id, topic);
            Catcher.LogMessageResponse.Builder r = Catcher.LogMessageResponse.newBuilder()
                    .setServerId(id)
                    .setSuccessful(true);

            int redirectTo = topic % farm.size();
            if (redirectTo != id) {
                farm.notifyRedirect(id, redirectTo);
                Catcher.TopicMapping.Builder redirect = r.getRedirectBuilder();
                redirect.setTopic(Integer.toString(redirectTo));
                redirect.setServerId(redirectTo);
                redirect.addHostBuilder()
                        .setHostName(Long.toString(redirectTo))
                        .setPort(100)
                        .build();
            }
            return r.build();
        }

        @Override
        public Catcher.CloseResponse close(RpcController controller, Catcher.Close request) throws ServiceException {
            farm.notifyClose(id);
            return Catcher.CloseResponse.newBuilder().setServerId(id).build();
        }
    }

    // TODO single server log message works right
    // TODO two server topic redirects work correctly (two servers, 100 requests on 10 topics, each topic has at most 1 request to wrong server)
    // TODO three server connect, cluster shrinks, send remaining topics to remaining nodes, minimal redirects
    // TODO three server connect, cluster shrinks, then expands, minimal redirects
}
