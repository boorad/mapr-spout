package com.mapr.franz.server;

import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.listener.TcpConnectionEventListener;
import com.googlecode.protobuf.pro.duplex.logging.CategoryPerServiceLogger;
import com.googlecode.protobuf.pro.duplex.server.DuplexTcpServerBootstrap;
import com.mapr.franz.catcher.wire.Catcher;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;

/**
 * Server process for catching log messages.
 */
public class Server {
    private static Logger logger = LoggerFactory.getLogger(Server.class);

    public static void main(String[] args) {
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
        final CategoryPerServiceLogger logPolicy = new CategoryPerServiceLogger();
        logPolicy.setLogRequestProto(true);
        logPolicy.setLogResponseProto(true);
        bootstrap.setLogger(logPolicy);

        //Finally binding the bootstrap to the TCP port will start off the socket accepting and clients can start to connect.
        bootstrap.getRpcServiceRegistry().registerBlockingService(Catcher.CatcherService.newReflectiveBlockingService(new CatcherServiceImpl()));

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
}
