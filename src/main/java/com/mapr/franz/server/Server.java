package com.mapr.franz.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.listener.TcpConnectionEventListener;
import com.googlecode.protobuf.pro.duplex.server.DuplexTcpServerBootstrap;
import com.mapr.franz.catcher.Client;
import com.mapr.franz.catcher.wire.Catcher;
import org.apache.zookeeper.KeeperException;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.List;
import java.util.concurrent.Executors;

/**
 * Server process for catching log messages.
 *
 * A server is required to do a number of things:
 *
 * a) catch messages for topics it is handling.
 *
 * b) catch and forward messages for servers it is not forwarding
 *
 * c) respond to hello messages with a list of the catchers in service
 *
 * d) report traffic statistics on topics every few seconds
 *
 * e) clean up old queue files when starting a new file.
 *
 * Tasks (a), (b) and (c) are handled by the server implementation.
 *
 * Task (d) by the statistics reporter.
 *
 * Task (e) is handled as part of the message appender.
 */
public class Server {
    private static Logger logger = LoggerFactory.getLogger(Server.class);
    private static final String ZK_CONNECT_STRING = "localhost:2108";
    private static final String FRANZ_BASE = "/franz";
    private static final int FRANZ_PORT = 9013;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        PeerInfo serverInfo = new PeerInfo("serverHostname", 8080);
        //You need then to create a DuplexTcpServerBootstrap and provide it an RpcCallExecutor.


        DuplexTcpServerBootstrap bootstrap = new DuplexTcpServerBootstrap(
                serverInfo,
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool())
        );

        bootstrap.setRpcServerCallExecutor(new ThreadPoolCallExecutor(10, 10));


        // set up request logging
//        final CategoryPerServiceLogger logPolicy = new CategoryPerServiceLogger();
//        logPolicy.setLogRequestProto(true);
//        logPolicy.setLogResponseProto(true);
//        bootstrap.setLogger(logPolicy);

        //Finally binding the bootstrap to the TCP port will start off the socket accepting and clients can start to connect.
        long serverId = new SecureRandom().nextLong();
        ClusterState zkState = new ClusterState(ZK_CONNECT_STRING, FRANZ_BASE, new Info(serverId, ImmutableList.of(new Client.HostPort(InetAddress.getLocalHost().getHostAddress(), FRANZ_PORT))));

        bootstrap.getRpcServiceRegistry().registerBlockingService(Catcher.CatcherService.newReflectiveBlockingService(new CatcherServiceImpl(serverId, zkState)));

        //If you want to track the RPC peering events with clients, use a RpcClientConnectionRegistry or a TcpConnectionEventListener for TCP connection events. This is the mechanism you can use to "discover" RPC clients before they "call" any service.
        TcpConnectionEventListener listener = new TcpConnectionEventListener() {
            @Override
            public void connectionClosed(RpcClientChannel clientChannel) {
                logger.debug("Disconnect from {}", clientChannel.getPeerInfo());
            }

            @Override
            public void connectionOpened(RpcClientChannel clientChannel) {
                logger.debug("Connect with {}", clientChannel.getPeerInfo());
            }
        };
        bootstrap.registerConnectionEventListener(listener);

        bootstrap.bind();
    }

    public static class Info {
        private static final int SERVER_PORT = 9013;
        private long id;
        private List<Client.HostPort> addresses;

        public Info(long id) throws UnknownHostException {
            this(id, ImmutableList.of(new Client.HostPort(InetAddress.getLocalHost().getHostAddress(), SERVER_PORT)));
            this.id = id;
        }

        public Info(long id, List<Client.HostPort> addresses) {
            this.addresses = addresses;
            this.id = id;
        }

        public long getId() {
            return id;
        }

        public List<Client.HostPort> getAddresses() {
            return addresses;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Info)) return false;

            Info info = (Info) o;
            if (id == info.id) {
                return addresses.size() == Sets.intersection(Sets.newHashSet(addresses), Sets.newHashSet(info.addresses)).size();
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            int result = (int) (id ^ (id >>> 32));
            result = 31 * result + addresses.hashCode();
            return result;
        }
    }
}
