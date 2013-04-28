package com.mapr.franz.hazel;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.server.DuplexTcpServerBootstrap;
import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MemberLeftException;
import com.mapr.franz.catcher.Client;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.security.SecureRandom;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
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

    public static void main(String[] args) throws SocketException, ExecutionException, InterruptedException, CmdLineException {
        Options opts = parseOptions(args);

        HazelcastInstance instance = setupHazelCast(opts);
        PeerInfo serverInfo = new PeerInfo("0.0.0.0", opts.port);

        DuplexTcpServerBootstrap bootstrap = new DuplexTcpServerBootstrap(
                serverInfo,
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool())
        );

        bootstrap.setRpcServerCallExecutor(new ThreadPoolCallExecutor(10, 10));

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
        long serverId = new SecureRandom().nextLong();

        Set<Server> servers = instance.getSet("servers");
        servers.add(new Server(serverId, addresses));

                boolean retry;
                do {
                    retry = false;
                    try {
                        DistributedTask<Integer> task = new DistributedTask<Integer>(new Doit(), s);
                        ExecutorService executorService = instance.getExecutorService();
                        executorService.execute(task);
                        System.out.printf("%s %d\n", s, task.get());
                    } catch (MemberLeftException e) {
                        System.out.printf("oopsie\n");
                        retry = true;
                    }
                } while (retry);

        System.exit(0);
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

    private static HazelcastInstance setupHazelCast(Options opts) {
        Config config = new ClasspathXmlConfig("cluster.xml");

        for (String host : Splitter.on(",").omitEmptyStrings().trimResults().split(opts.hosts)) {
            config.getNetworkConfig().getJoin().getTcpIpConfig().addMember(host);
        }
        System.out.printf("config = %s\n\n", config);

        return Hazelcast.newHazelcastInstance(config);
    }

    public static class Doit implements Callable<Integer>, Serializable {
        public Integer call() throws Exception {
            return HazelCatcher.id;
        }
    }

    public static class Options {
        @Option(name = "-host", usage = "Comma separated list of at least one node's hostname or IP address in the cluster")
        String hosts;
    }
}