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

package com.mapr.franz.catcher;

import com.google.protobuf.RpcController;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.client.DuplexTcpClientBootstrap;
import com.mapr.franz.catcher.wire.Catcher;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;

/**
 * Handles connecting to a server with standard options.
 */
public class NetworkCatcherConnection implements CatcherConnection {
    private final Logger logger = LoggerFactory.getLogger(NetworkCatcherConnection.class);

    private final Catcher.CatcherService.BlockingInterface catcherService;
    private final RpcController controller;
    private final DuplexTcpClientBootstrap bootstrap;
    private final RpcClientChannel channel;
    private PeerInfo  server;

    public static CatcherConnection connect(PeerInfo server) {
        final Logger logger = LoggerFactory.getLogger(NetworkCatcherConnection.class);
        final CatcherConnection r;
        try {
            r = new NetworkCatcherConnection(server);
        } catch (IOException e) {
            logger.warn("Cannot connect to {}", server, e);
            return null;
        }
        return r;
    }

    NetworkCatcherConnection(PeerInfo server) throws IOException {
        logger.info("Connecting to {}", server);
        this.server = server;
        PeerInfo client = new PeerInfo("clientHostname", 9999);
        bootstrap = new DuplexTcpClientBootstrap(
                client,
                new NioClientSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));
        bootstrap.setCompression(false);

        bootstrap.setOption("connectTimeoutMillis", 1000);
        bootstrap.setOption("connectResponseTimeoutMillis", 1000);
        bootstrap.setOption("receiveBufferSize", 1048576);
        bootstrap.setOption("tcpNoDelay", true);

        channel = bootstrap.peerWith(server);

        catcherService = Catcher.CatcherService.newBlockingStub(channel);
        controller = channel.newRpcController();
    }

    @Override
    public Catcher.CatcherService.BlockingInterface getService() {
        return catcherService;
    }

    @Override
    public RpcController getController() {
        return controller;
    }

    @Override
    public PeerInfo getServer() {
        return server;
    }

    @Override
    public void close() {
        // these can be null in mocked versions of this class
        if (channel != null) {
            channel.close();
        }
        if (bootstrap != null) {
            bootstrap.releaseExternalResources();
        }
    }

    @Override
    public String toString() {
        return "CatcherConnection{" + "server=" + server + '}';
    }

    @Override
    public void setServer(PeerInfo host) {
        server = host;
    }
}
