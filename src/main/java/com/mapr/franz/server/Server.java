package com.mapr.franz.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;

import org.apache.zookeeper.KeeperException;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.listener.TcpConnectionEventListener;
import com.googlecode.protobuf.pro.duplex.server.DuplexTcpServerBootstrap;
import com.mapr.franz.catcher.Client;
import com.mapr.franz.catcher.wire.Catcher;

/**
 * Server process for catching log messages.
 *
 * A server is required to do a number of things:
 *
 * a) catch messages for topics it is handling.
 *
 * b) catch and forward messages for servers it is not handling
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
    private static Logger log = LoggerFactory.getLogger(Server.class);
    private static final String PROPERTIES_FILE = "franz-server.properties";

    private static final String ZK_CONNECT_STRING = "localhost:2108";
    private static final String FRANZ_BASE = "/franz";
    private static final int FRANZ_PORT = 9013;

    public static Properties loadProperties() {
        Properties props = new Properties();
        try {
            InputStream base = Resources.getResource("base.properties").openStream();
            props.load(base);
            base.close();

            File propFile = new File(PROPERTIES_FILE);
            if (propFile.exists()) {
                log.debug("Adding additional properties from {}", propFile.getCanonicalPath());

                FileInputStream in = new FileInputStream(PROPERTIES_FILE);
                props.load(in);
                in.close();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return props;
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        if (args.length < 2) {
            System.out.println("Usage: java -cp <classpath> com.mapr.franz.server.Server <hostname> <port> [zkhost:port]");
            System.exit(1);
        }

        Properties props = loadProperties();

        int port = Integer.parseInt(args[1]);
        PeerInfo serverInfo = new PeerInfo(args[0], port);
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
        String zk_str = props.getProperty("zookeeper.connection.string", ZK_CONNECT_STRING);
        if (args.length == 3) {
            zk_str = args[2];
        }

        List<Client.HostPort> addresses = Lists.newArrayList();
        Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
        while (networkInterfaces.hasMoreElements()) {
            NetworkInterface ifc = networkInterfaces.nextElement();
            if (!ifc.isLoopback()) {
                for (InterfaceAddress address : ifc.getInterfaceAddresses()) {
                    addresses.add(new Client.HostPort(address.getAddress().getHostAddress(), port));
                }
            }
        }
        ClusterState zkState = new ClusterState(zk_str, FRANZ_BASE, new Info(serverId, addresses));

        bootstrap.getRpcServiceRegistry().registerBlockingService(Catcher.CatcherService.newReflectiveBlockingService(new com.mapr.franz.server.CatcherServiceImpl(serverId, zkState)));

        //If you want to track the RPC peering events with clients, use a RpcClientConnectionRegistry or a TcpConnectionEventListener for TCP connection events. This is the mechanism you can use to "discover" RPC clients before they "call" any service.
        TcpConnectionEventListener listener = new TcpConnectionEventListener() {
            @Override
            public void connectionClosed(RpcClientChannel clientChannel) {
                log.debug("Disconnect from {}", clientChannel.getPeerInfo());
            }

            @Override
            public void connectionOpened(RpcClientChannel clientChannel) {
                log.debug("Connect with {}", clientChannel.getPeerInfo());
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
