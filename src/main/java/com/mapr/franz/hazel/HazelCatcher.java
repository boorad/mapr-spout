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

package com.mapr.franz.hazel;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.server.DuplexTcpServerBootstrap;
import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.mapr.franz.catcher.Client;
import com.mapr.franz.catcher.wire.Catcher;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.security.SecureRandom;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;

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
public class HazelCatcher {
    private static Logger logger = LoggerFactory.getLogger(HazelCatcher.class);

    private static int id = new SecureRandom().nextInt();

    public static void main(String[] args) throws FileNotFoundException, SocketException {
        run(parseOptions(args));


    }

    public static void run(Options opts) throws SocketException, FileNotFoundException {
        logger.warn("Starting server {}", opts);

        // start this first so that Hazel has to take second pickings
        DuplexTcpServerBootstrap bootstrap = startProtoServer(opts.port);

        HazelcastInstance instance = setupHazelCast(opts.hosts);
        Server us = recordServerInstance(opts, instance);

        bootstrap.getRpcServiceRegistry().registerBlockingService(
                Catcher.CatcherService.newReflectiveBlockingService(new CatcherImpl(us, instance, opts.basePath)));
        bootstrap.bind();
    }


    private static DuplexTcpServerBootstrap startProtoServer(int port) {
        PeerInfo serverInfo = new PeerInfo("0.0.0.0", port);
        DuplexTcpServerBootstrap bootstrap = new DuplexTcpServerBootstrap(
                serverInfo,
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool())
        );
        bootstrap.setRpcServerCallExecutor(new ThreadPoolCallExecutor(10, 10));
        return bootstrap;
    }

    private static Server recordServerInstance(Options opts, HazelcastInstance instance) throws SocketException {
        List<Client.HostPort> addresses = Lists.newArrayList();
        Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
        while (networkInterfaces.hasMoreElements()) {
            NetworkInterface ifc = networkInterfaces.nextElement();
            if (!ifc.isLoopback()) {
                for (InterfaceAddress address : ifc.getInterfaceAddresses()) {
                    addresses.add(new Client.HostPort(address.getAddress().getHostAddress(), opts.port));
                }
            }
        }
        long serverId = new SecureRandom().nextLong();

        Set<Server> servers = instance.getSet("servers");
        Server r = new Server(serverId, addresses);
        servers.add(r);
        logger.warn("Currently have these servers: {}", servers);
        return r;
    }

    private static HazelcastInstance setupHazelCast(String hosts) {
        Config config = new ClasspathXmlConfig("cluster.xml");

        Splitter onCommas = Splitter.on(",").omitEmptyStrings().trimResults();
        for (String host : onCommas.split(hosts)) {
            config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
            config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
            config.getNetworkConfig().getJoin().getTcpIpConfig().addMember(host);
        }

        logger.warn("config = {}", config);
        return Hazelcast.newHazelcastInstance(config);
    }

    private static Options parseOptions(String[] args) {
        Options opts = new Options();
        CmdLineParser parser = new CmdLineParser(opts);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            e.printStackTrace(System.err);
            parser.printUsage(System.err);
            System.exit(1);
        }
        if (opts.hosts == null) {
            parser.printUsage(System.err);
            System.exit(1);
        }
        return opts;
    }

    public static class Options {
        public Options host(String hosts) {
            this.hosts = hosts;
            return this;
        }

        public Options base(String basePath) {
            this.basePath = basePath;
            return this;
        }

        public Options port(int port) {
            this.port = port;
            return this;
        }

        @Option(name = "-cluster", usage = "Comma separated list of at least one node's hostname or IP address in the cluster.  IP and port ranges are acceptable here.")
        String hosts;

        @Option(name = "-port", usage = "Port number for the catcher server to listen to")
        int port = 5900;

        @Option(name = "-base", usage = "Home directory for recording topics")
        String basePath = "/tmp/mapr-spout";

        @Override
        public String toString() {
            return "Options{" +
                    "basePath='" + basePath + '\'' +
                    ", hosts='" + hosts + '\'' +
                    ", port=" + port +
                    '}';
        }
    }
}