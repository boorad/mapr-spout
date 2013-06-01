/*
 * Copyright MapR Technologies, 2013
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

package com.mapr.franz.simple;

import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.server.DuplexTcpServerBootstrap;
import com.mapr.franz.catcher.wire.Catcher;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.net.SocketException;
import java.util.concurrent.Executors;

/**
 * Single machine process for catching log messages.
 * <p/>
 * This server is required to do a number of things:
 * <p/>
 * a) catch messages for topics it is handling.
 * <p/>
 * b) respond to hello messages with a list containing our own network addresses
 * <p/>
 * c) report traffic statistics on topics every few seconds
 * <p/>
 * d) clean up old queue files when starting a new file.
 * <p/>
 * Tasks (a), and (b) are handled by the server implementation.
 * <p/>
 * Task (c) by the statistics reporter.
 * <p/>
 * Task (d) is handled as part of the message appender.
 */
public class SimpleCatcher {
    private static Logger logger = LoggerFactory.getLogger(SimpleCatcher.class);

    public static void main(String[] args) throws FileNotFoundException, SocketException {
        run(parseOptions(args));
    }

    public static void run(Options opts) throws SocketException, FileNotFoundException {
        logger.warn("Starting server {}", opts);

        SimpleCatcherService service;
        try {
            service = new SimpleCatcherService(opts.port, opts.basePath);
        } catch (FileNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return;
        }

        DuplexTcpServerBootstrap bootstrap = startProtoServer(opts.port);
        bootstrap.getRpcServiceRegistry().registerBlockingService(
                Catcher.CatcherService.newReflectiveBlockingService(service));

        try {
            bootstrap.bind();
        } catch (ChannelException e) {
            // releasing resources allows the server to exit
            bootstrap.releaseExternalResources();
            throw e;
        }
    }

    private static DuplexTcpServerBootstrap startProtoServer(int port) {
        PeerInfo serverInfo = new PeerInfo("0.0.0.0", port);
        DuplexTcpServerBootstrap bootstrap = new DuplexTcpServerBootstrap(
                serverInfo,
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool())
        );
        ThreadPoolCallExecutor pool = new ThreadPoolCallExecutor(10, 10);
        bootstrap.setRpcServerCallExecutor(pool);
        return bootstrap;
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
        return opts;
    }

    public static class Options {
        public Options base(String basePath) {
            this.basePath = basePath;
            return this;
        }

        public Options port(int port) {
            this.port = port;
            return this;
        }

        @Option(name = "-port", usage = "Port number for the catcher server to listen to")
        int port = 5900;

        @Option(name = "-base", usage = "Home directory for recording topics")
        String basePath = "/tmp/mapr-spout";

        @Override
        public String toString() {
            return "Options{" +
                    "basePath='" + basePath + '\'' +
                    ", port=" + port +
                    '}';
        }
    }
}