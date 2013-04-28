package com.mapr.franz.catcher;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.mapr.franz.catcher.wire.Catcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Handles sending log messages to the array of catcher servers.  In doing so, we need
 * to be as robust as possible and recover from previous errors.
 * <p/>
 * The basic idea is that when we originally connect, we give a list of as many servers
 * as we can in the form of hostname, port.  These servers are connected to and they
 * are queried to find out about any other servers that might be in the cloud.  Servers
 * might have multiple routes, but we resolve this because each server returns a unique
 * identifier.
 * <p/>
 * When it comes time to log, we look in a topic => server cache.  If we find a preferred
 * server there, we use it.  Otherwise, we pick some server at random.
 * <p/>
 * When we send a log message, the response may include a redirect to a different server.
 * If so, we make sure that we have connected to all forms of that server's address and
 * cache that for later.  Then we retry the send to the new server.  We may also cache that
 * message for later retry.
 * <p/>
 * If a log request results in an error, we try to get rid of the connection that caused
 * us this grief.  This might ultimately cause us to forget a host or even a cache entry,
 * but we will attempt to re-open these connections later, usually due to a referral back
 * to that host.  During server cloud reorganizations, we may not re-open the same server.
 */
public class Client {
    private static final int MAX_SERVER_RETRIES_BEFORE_BLACKLISTING = 4;
    private static final int MAX_REDIRECTS_BEFORE_LIVING_WITH_INDIRECTS = 100;

    private final Logger logger = LoggerFactory.getLogger(Client.class);

    private final ConnectionFactory connector;

    private final long myUniqueId;
    private static final long BIG_PRIME = 213887370601841L;

    // cache of topic => server preferences
    // updates to this race against accesses with little effect.  Mostly, entries
    // will be added and never removed.  Under failure modes, entries may be deleted
    // as well, but the use of an entry that is about to be deleted is not a problem
    // since all that can happen is that an attempt might be made to delete it again.
    private Map<String, Long> topicMap = Maps.newHashMap();

    // mapping from server id to all known connections for that server
    // updates to this race accesses, but this is safe since these should almost
    // always be added at program start and never changed.  Under failure modes,
    // connections may be deleted, but using a connection that is due for deletion
    // is not a big deal.
    private Multimap<Long, CatcherConnection> hostConnections = Multimaps.synchronizedMultimap(HashMultimap.<Long, CatcherConnection>create());

    // history of all connections
    // this is retained so that we can be sure to call close on everything that we
    // ever open.  It also lets us have a back-stop of servers if the topic map points us
    // at a dead server.
    private Set<CatcherConnection> allConnections = Collections.newSetFromMap(new ConcurrentHashMap<CatcherConnection, Boolean>());

    // history of all host,port => server mappings.  Useful for reverse engineering in failure modes
    private Map<HostPort, Long> knownServers = Maps.newConcurrentMap();

    // TODO should periodically send a Hello to update the list of servers

    // TODO should reset this periodically so we try servers again in case network is repaired
    // list of server connections that have been persistently bad which we now ignore
    private Multiset<HostPort > serverBlackList = ConcurrentHashMultiset.create();

    // TODO should decrement this periodically so that we start trying to handle redirects again
    private volatile int redirectCount = 0;

    // TODO should periodically attempt to reconnect to any of these that have gotten lost
    // collects all of the servers we have tried to connect with
    private final Set<HostPort> allKnownServers = Collections.newSetFromMap(new ConcurrentHashMap<HostPort, Boolean>());

    // how many messages has this Client sent (used to generate message UUID)
    private AtomicLong messageCount = new AtomicLong(0);

    public Client(Iterable<PeerInfo> servers) throws IOException, ServiceException {
        this(new ConnectionFactory(), servers);
    }
    public Client(ConnectionFactory connector, Iterable<PeerInfo> servers) throws IOException, ServiceException {
        this.connector = connector;

        // SecureRandom can cause delays if over-used.  Pulling 8 bytes shouldn't be a big deal.
        this.myUniqueId = new SecureRandom().nextLong();
        connectAll(Iterables.transform(servers, new Function<PeerInfo, HostPort>() {
            @Override
            public HostPort apply(PeerInfo input) {
                return new HostPort(input);
            }
        }));

        // every so often, we bring back a black-listed server
        ScheduledExecutorService background = Executors.newSingleThreadScheduledExecutor();
        background.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                if (serverBlackList.size() > 0) {
                    serverBlackList.elementSet().remove(serverBlackList.iterator().next());
                }
            }
        }, 10, 2, TimeUnit.SECONDS);

        Queue<Object> retry = Queues.newConcurrentLinkedQueue();
        background.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                Object message = retry.poll();
                if (message != null) {
                    sendMessage(message.topic, message.content);
                }
            }
        }, 1, 1, TimeUnit.SECONDS);

    }

    private static class MessageRetry {
        String topic;
        String content;
        long firstSendTime = System.nanoTime() / 1000000;
        int retryCount = 0;

        private MessageRetry(String topic, String content) {
            this.content = content;
            this.topic = topic;
        }
    }

    public void sendMessage(String topic, String message) throws IOException, ServiceException {
        sendMessage(topic, message.getBytes(Charsets.UTF_8));
    }

    public void sendMessage(String topic, byte[] message) throws IOException, ServiceException {
        sendMessage(topic, ByteBuffer.wrap(message));
    }

    public void sendMessage(String topic, ByteBuffer message) throws IOException {
        long messageId = (myUniqueId ^ (messageCount.getAndIncrement() * BIG_PRIME)) >>> 1;
        logger.info("Starting {} to topic {}", messageId, topic);
        // first find a good server to talk to
        final Long preferredServer = topicMap.get(topic);
        Collection<CatcherConnection> servers;
        if (preferredServer == null) {
            // unknown topic, pick server at random
            // TODO possibly make this more efficient if it turns out to be common
            List<CatcherConnection> s = Lists.newArrayList(hostConnections.values());
            if (s.size() == 0) {
                logger.error("Found no live catcher connections... will try to reopen");
                connectAll(allKnownServers);
            }
            // balance load when we don't know where the topic goes
            Collections.shuffle(s);
            servers = s;
        } else {

            // we have a preference... now we need to connect to same
            servers = hostConnections.get(preferredServer);
            logger.info("Topic {} to {}", topic, servers);

            // shouldn't happen, but it could depending on other threads
            if (servers == null) {
                servers = Lists.newArrayList(hostConnections.values());
                if (servers.size() == 0) {
                    logger.error("Found no live catcher connections... will try to reopen");
                    connectAll(allKnownServers);
                }
            }
        }
        if (servers.size() == 0 && allConnections.isEmpty()) {
            logger.error("No live catchers even after trying to re-open everything");
            throw new IOException("No catcher servers to connect to");
        }

        // now try to send the message keeping track of errors
        List<CatcherConnection> pendingConnectionRemovals = Lists.newArrayList();

        // note that concatenation here is done lazily.  We add everything to the list to be as persistent as possible.
        Catcher.LogMessage request = Catcher.LogMessage.newBuilder()
                .setTopic(topic)
                        // multiplying by a big prime acts to spread out the bit changes between consecutive messages
                .setClientId(messageId)
                .setPayload(ByteString.copyFrom(message))
                .build();

        // try the first line candidates
        boolean done = false;
        for (CatcherConnection s : servers) {
            done = sendInternal(s, topic, messageId, request, pendingConnectionRemovals);
            if (done) {
                break;
            }
        }
        // TODO if this was a redirect, we should send it there (or do it in sendInternal)

        // if that didn't do the job, try all other servers in random order
        if (!done) {
            // TODO we shouldn't do this in a client redirects world.  SHould just let the retry have it.
            List<CatcherConnection> tmp = Lists.newArrayList(allConnections);
            Collections.shuffle(tmp);
            for (CatcherConnection s : tmp) {
                done = sendInternal(s, topic, messageId, request, pendingConnectionRemovals);
                if (done) {
                    break;
                }
            }
        }

        // remove all connections that cause an error
        if (pendingConnectionRemovals.size() > 0) {
            allConnections.removeAll(pendingConnectionRemovals);
            for (CatcherConnection connection : pendingConnectionRemovals) {
                PeerInfo pi = connection.getServer();
                if (pi != null) {
                    HostPort hostPort = new HostPort(pi);
                    Long serverId = knownServers.get(hostPort);
                    knownServers.remove(hostPort);

                    if (serverId != null) {
                        // forget any topic mappings for this host
                        List<String> toRemove = Lists.newArrayList();
                        for (String t : topicMap.keySet()) {
                            if (topicMap.get(t).equals(serverId)) {
                                toRemove.add(t);
                            }
                        }
                        for (String t : toRemove) {
                            topicMap.remove(t);
                        }
                        hostConnections.remove(serverId, connection);
                    }
                }
                connection.close();
            }
        }
    }

    private boolean sendInternal(CatcherConnection s, String topic, long messageId, Catcher.LogMessage request, List<CatcherConnection> pendingConnectionRemovals) throws IOException {
        Catcher.LogMessageResponse r;
        try {
            r = s.getService().log(s.getController(), request);

            // server responded at least
            if (r.getSuccessful()) {
                logger.info("Success {} to {}", messageId, s.getServer());
                // repeated TopicMapping redirects = 3;
                if (r.hasRedirect()) {
                    // don't natter on about this forever.  It could be we can only see a few hosts
                    if (redirectCount < MAX_REDIRECTS_BEFORE_LIVING_WITH_INDIRECTS) {
                        Catcher.TopicMapping redirect = r.getRedirect();

                        long redirectId = redirect.getServer().getServerId();
                        logger.info("redirect {} to {}", messageId, redirectId);

                        // connect to all possible address of this redirected host
                        List<HostPort> newHosts = Lists.newArrayList();
                        for (Catcher.Host h : redirect.getServer().getHostList()) {
                            newHosts.add(new HostPort(h.getHostName(), h.getPort()));
                        }
                        connectAll(newHosts);

                        // we should now know about this host
                        // if so, cache the topic mapping
                        if (hostConnections.containsKey(redirectId)) {
                            topicMap.put(redirect.getTopic(), redirectId);
                        } else {
                            // if not, things are a bit odd.  Probably due to black-listing or connection errors.
                            logger.warn("Can't find server {} in connection map after redirect on topic {}", redirectId, topic);
                        }
                        redirectCount++;
                    }
                    // TODO send message to redirect server
                } else {
                    Long id = topicMap.get(topic);
                    if (id == null || id != r.getServerId()) {
                        topicMap.put(topic, r.getServerId());
                    }
                    redirectCount = 0;
                }

                // no more retries
                return true;
            } else {
                // server side failure... don't retry this
                IOException e = new IOException(r.getBackTrace());
                logger.warn(String.format("Server side failure %d to %s", messageId, s.getServer()), e);
                throw e;
            }
        } catch (ServiceException e) {
            // request failed for whatever reason.  Retry and remember the problem child
            logger.warn("Failure {} to {}", messageId, s.getServer());
            pendingConnectionRemovals.add(s);
        }
        return false;
    }

    public void close() {
        for (CatcherConnection connection : allConnections) {
            connection.close();
        }
        allConnections.clear();
        topicMap.clear();
        hostConnections.clear();
        knownServers.clear();
        serverBlackList.clear();
    }

    // TODO maybe should do this again against knownServers.keyset() every 30 seconds or so
    private void connectAll(Iterable<HostPort > servers) throws IOException {
        // all of the hosts we have attempted to contact
        Set<HostPort > attempted = Sets.newHashSet();

        // the ones we are going to try in each iteration
        Iterables.addAll(this.allKnownServers, servers);
        Set<HostPort > newServers = Sets.newHashSet(servers);
        while (newServers.size() > 0) {
            // the novel servers that we hear about during this iteration
            Set<HostPort> discovered = Sets.newHashSet();
            Catcher.Hello request = Catcher.Hello.newBuilder()
                    .setClientId(1)
                    .setApplication("test-client")
                    .build();
            for (HostPort server : newServers) {
                if (!attempted.contains(server) && !knownServers.keySet().contains(server)) {
                    try {
                        if (serverBlackList.count(server) < MAX_SERVER_RETRIES_BEFORE_BLACKLISTING) {
                            logger.info("Connecting to {}", server);
                            CatcherConnection s = connector.create(server.asPortInfo());
                            if (s != null) {
                                Catcher.HelloResponse r = s.getService().hello(s.getController(), request);
                                attempted.add(server);

                                hostConnections.put(r.getServerId(), s);
                                allConnections.add(s);
                                knownServers.put(server, r.getServerId());

                                int n = r.getHostCount();
                                for (int i = 0; i < n; i++) {
                                    Catcher.Host host = r.getHost(i);
                                    HostPort pi = new HostPort(host.getHostName(), host.getPort());
                                    logger.info("Discovered host at {}:{}", host.getHostName(), host.getPort());
                                    if (!attempted.contains(pi)) {
                                        logger.info("Unknown host, adding to discovered list.");
                                        discovered.add(pi);
                                    }
                                    attempted.add(server);
                                }
                                for (Catcher.Server otherServer : r.getClusterList()) {
                                    for (Catcher.Host host : otherServer.getHostList()) {
                                        HostPort pi = new HostPort(host.getHostName(), host.getPort());
                                        logger.info("Discovered host at {}:{} via other server", host.getHostName(), host.getPort());
                                        if (!attempted.contains(pi)) {
                                            logger.info("Unknown host, adding to discovered list.");
                                            discovered.add(pi);
                                        }
                                        attempted.add(server);
                                    }
                                }
                            } else {
                                serverBlackList.add(server);
                                logger.warn("Blacklisting {}", server);
                            }
                        }
                    } catch (ServiceException e) {
                        serverBlackList.add(server);
                        logger.warn("Hello failed", e);
                    }
                }
            }
            // this has to be true ... nice to check during testing, though
            assert Sets.intersection(discovered, attempted).size() == 0;
            newServers.clear();
            newServers.addAll(discovered);
        }
    }

    public static class HostPort {
        private String host;
        private int port;

        public HostPort(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public HostPort(PeerInfo server) {
            this(server.getHostName(), server.getPort());
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof HostPort)) return false;

            HostPort hostPort = (HostPort) o;
            return port == hostPort.port && host.equals(hostPort.host);
        }

        @Override
        public int hashCode() {
            int result = host.hashCode();
            result = 31 * result + port;
            return result;
        }

        public PeerInfo asPortInfo() {
            return new PeerInfo(host, port);
        }

        @Override
        public String toString() {
            return "HostPort{" +
                    "host='" + host + '\'' +
                    ", port=" + port +
                    '}';
        }
    }

    public static void main(String[] args) throws ServiceException, IOException {
        Client c = new Client(ImmutableList.of(new PeerInfo("localhost", 8080)));

        c.sendMessage("this", "hello world");

        c.close();
    }

}
