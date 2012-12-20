package com.mapr.franz.catcher;

import com.google.common.collect.Lists;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.mapr.franz.catcher.wire.Catcher;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.fail;

public class ClientTest {
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
            Assert.assertTrue(e.getMessage().contains("No catcher servers"));
        }
    }

    @Test
    public void oneServerRetry() throws ServiceException, IOException {
        new MockUp<CatcherConnection>() {
            int attempt = 0;
            @Mock
            public void $init(PeerInfo server) {
                Assert.assertNotNull(server);
            }

            @Mock
            public CatcherConnection connect(PeerInfo server) throws IOException {
                if (attempt == 0) {
                    return null;
                } else {
                    return new CatcherConnection(server);
                }
            }

            @Mock
            public Catcher.CatcherService.BlockingInterface getService() {
                return new Catcher.CatcherService.BlockingInterface() {
                    @Override
                    public Catcher.HelloResponse hello(RpcController controller, Catcher.Hello request) throws ServiceException {
                        throw new UnsupportedOperationException("Default operation");
                    }

                    @Override
                    public Catcher.LogMessageResponse log(RpcController controller, Catcher.LogMessage request) throws ServiceException {
                        throw new UnsupportedOperationException("Default operation");
                    }

                    @Override
                    public Catcher.CloseResponse close(RpcController controller, Catcher.Close request) throws ServiceException {
                        throw new UnsupportedOperationException("Default operation");
                    }
                };
            }
        };

        Client c = new Client(Lists.newArrayList(new PeerInfo("foo", 0)));
        try {
            c.sendMessage("topic", "message");
            fail("Should have failed with IOException");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().contains("No catcher servers"));
        }
    }


    @Test
    public void singleServer() throws ServiceException, IOException {
        new MockUp<CatcherConnection>() {
            @Mock
            public Catcher.CatcherService.BlockingInterface getService() {
                return new Catcher.CatcherService.BlockingInterface() {
                    @Override
                    public Catcher.HelloResponse hello(RpcController controller, Catcher.Hello request) throws ServiceException {
                        throw new UnsupportedOperationException("Default operation");
                    }

                    @Override
                    public Catcher.LogMessageResponse log(RpcController controller, Catcher.LogMessage request) throws ServiceException {
                        throw new UnsupportedOperationException("Default operation");
                    }

                    @Override
                    public Catcher.CloseResponse close(RpcController controller, Catcher.Close request) throws ServiceException {
                        throw new UnsupportedOperationException("Default operation");
                    }
                };
            }
        };

        Client c = new Client(Lists.newArrayList(new PeerInfo("foo", 0)));
        try {
            c.sendMessage("topic", "message");
            fail("Should have failed with IOException");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().contains("No catcher servers"));
        }
    }


    // TODO single server log message works right
    // TODO two server topic redirects work correctly (two servers, 100 requests on 10 topics, each topic has at most 1 request to wrong server)
    // TODO three server connect, cluster shrinks, send remaining topics to remaining nodes, minimal redirects
    // TODO three server connect, cluster shrinks, then expands, minimal redirects
}
