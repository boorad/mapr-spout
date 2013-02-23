package com.mapr.franz.server;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import com.mapr.franz.catcher.Client;
import com.mapr.franz.catcher.wire.Catcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Keeps a view of cluster state in Zookeeper.  This includes the list of live
 * servers and their ordering.
 * <p/>
 * For simplicity, this is done using a directory with one file per server.  We
 * assume that the number of servers is relatively small (<100 or so) and changes
 * very rare so that very simple approaches to updates such as having all nodes
 * watch the state directory are feasible.
 * <p/>
 * When each server starts, it will establish a local server id and will build
 * a file in Zookeeper named according to that server id name.  The contents of
 * the file will include a description of the server that includes all of the
 * addresses on which it might be reached.
 * <p/>
 * The operations allowable on the state include
 * <p/>
 * - asking when the last change was (for cache invalidation)
 * <p/>
 * - asking whether we are connected
 * <p/>
 * - asking which server an integer hash should be sent to
 * <p/>
 * - asking for all of the connection details for all servers.
 * <p/>
 * - asking for whether operations can proceed at all.
 */
public class ClusterState {
    private final Server.Info info;
    private Logger logger = LoggerFactory.getLogger(ClusterState.class);

    private final long myUniqueId;
    private final String myStateFilePath;
    private final String myStateFileName;
    private final byte[] myDescription;

    private int maxConnectAttempts;

    private int maxRetryDelay = 20000;

    private int maxReadAttempts;
    private final ExecutorService connectThread;

    private final String connectString;
    private final String base;

    private ZooKeeper zk;

    private int generation = 0;
    private List<String> cluster;
    private Map<String, Catcher.Server> servers = Maps.newHashMap();
    private int ourPosition;
    private Status status;

    public Iterable<Catcher.Server> getCluster() {
        return Iterables.unmodifiableIterable(servers.values());
    }

    public enum Status {
        STARTING, UNKNOWN, LIVE, FAILED
    }

    public ClusterState(String connectString, String base, Server.Info info) throws IOException, InterruptedException {
        connectThread = Executors.newSingleThreadExecutor();

        this.connectString = connectString;
        this.base = base;

        this.info = info;
        myUniqueId = info.getId();
        myStateFileName = String.format("%016x", myUniqueId);
        myStateFilePath = String.format("%s/%016x", base, myUniqueId);

        Catcher.Server.Builder addressBuilder = Catcher.Server.newBuilder().setServerId(myUniqueId);
        for (Client.HostPort hostPort : this.info.getAddresses()) {
            addressBuilder.addHostBuilder().setHostName(hostPort.getHost()).setPort(hostPort.getPort()).build();
        }
        myDescription = addressBuilder.build().toByteArray();

        status = Status.STARTING;

        // try forever
        maxConnectAttempts = 0;
        maxReadAttempts = 10;

        try {
            newSession.call();
        } catch (IOException e) {
            throw e;
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            // should be impossible
            throw new RuntimeException("Impossible exception from newSession", e);
        }
    }

    public Server.Info getLocalInfo() {
        return info;
    }

    public void exit() throws InterruptedException {
        try {
            zk.delete(myStateFilePath, -1);
        } catch (KeeperException e) {
            logger.warn(String.format("Cluster status file %s could not be deleted from Zookeeper", myStateFilePath), e);
        }
        zk.close();
    }

    public Target directTo(String topic) {
        int hash = topic.hashCode();
        synchronized (this) {
            int n = cluster.size();
            hash = hash % n;
            if (hash < 0) {
                hash += n;
            }
            Catcher.Server s = servers.get(cluster.get(hash));
            return new Target(status, generation, s, hash != ourPosition);
        }
    }

    public static class Target {
        private Status status;
        private int generation;
        private final Catcher.Server server;
        private final boolean redirect;

        public Target(Status status, int generation, Catcher.Server server, boolean redirect) {
            this.status = status;
            this.generation = generation;
            this.server = server;
            this.redirect = redirect;
        }

        public int getGeneration() {
            return generation;
        }

        public boolean isRedirect() {
            return redirect;
        }

        public Catcher.Server getServer() {
            return server;
        }

        public Status getStatus() {
            return status;
        }
    }

    private class StateWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.None) {
                switch (event.getState()) {
                    case Disconnected:
                        // we have been partitioned away from the ZK cluster.  It may come back.  Or not.
                        // we should handle pending events, but nothing more.  Any incoming events should
                        // be returned as failures.
                        logger.info("Disconnected, stay tuned for reconnect or expiration");
                        disconnect();
                        break;
                    case SyncConnected:
                        // we have been reconnected with the ZK cluster and we can continue service as if nothing has happened
                        logger.info("SyncConnected");
                        reconnect();
                        break;
                    case Expired:
                        // our session expired.  This means that the cluster has forgotten us and we need to reconnect
                        logger.info("Session expired, will try to re-open connection");
                        try {
                            openNewSession();
                        } catch (InterruptedException e) {
                            // ignore
                        } catch (IOException e) {
                            logger.error("Can't re-open session, retrying in background", e);
                            newSession();
                        }
                        break;
                }
            } else {
                switch (event.getType()) {
                    case NodeChildrenChanged:
                        logger.info("Directory changed");
                        readState(base);
                        break;
                    case NodeDeleted:
                        // this is a puzzle ... somebody has presumably deleted our base directory
                        logger.error("Directory deleted ... shouldn't happen");
                        status = Status.FAILED;
                        break;
                }
            }
        }
    }

    private void newSession() {
        connectThread.submit(newSession);
    }

    private void readState(String base) {
        int retryDelay = 0;

        int attempts = 0;

        while (attempts < maxReadAttempts) {
            try {
                synchronized (this) {
                    logger.info("Server {} reading from status directory {}", myUniqueId, base);
                    servers.clear();
                    List<String> tmp = zk.getChildren(base, true);
                    Collections.sort(tmp);
                    cluster = tmp;
                    for (String server : tmp) {
                        try {
                            servers.put(server, Catcher.Server.parseFrom(zk.getData(base + "/" + server, false, null)));
                        } catch (InvalidProtocolBufferException e) {
                            throw new RuntimeException("Invalid state in ZK for server " + server, e);
                        }
                    }
                    ourPosition = cluster.indexOf(myStateFileName);
                    if (ourPosition >= 0) {
                        logger.info("Entries: {}, our file: {}", cluster, myStateFileName);
                        logger.info("Server {} read {} entries in status directory", myUniqueId, cluster.size());
                        logger.info("Server {} has position {} ", myUniqueId, ourPosition);

                        generation++;
                        status = Status.LIVE;
                        return;
                    } else {
                        logger.warn("Server {} not in directory yet", myUniqueId);
                    }
                }
            } catch (KeeperException.ConnectionLossException e) {
                disconnect();
            } catch (KeeperException.SessionExpiredException e) {
                try {
                    zk.close();
                    return;
                } catch (InterruptedException e1) {
                    // ignore
                }
                newSession();
            } catch(KeeperException e) {
                logger.warn("Could not read cluster state", e);
                retryDelay = (int) Math.min(1.5 * retryDelay + 1, 10000);
                try {
                    Thread.sleep(retryDelay);
                } catch (InterruptedException e1) {
                    // ignore
                }
            } catch (InterruptedException e) {
                // ignore
            }
		attempts++;
        }
        status = Status.FAILED;
        logger.error("Cannot read cluster state");
    }

    private void disconnect() {
        synchronized (this) {
            if (status == Status.LIVE) {
                status = Status.UNKNOWN;
            }
        }
    }

    private void reconnect() {
        synchronized (this) {
            if (status == Status.UNKNOWN) {
                status = Status.LIVE;
            }
        }
    }

    private Callable<ZooKeeper> newSession = new Callable<ZooKeeper>() {
        @Override
        public ZooKeeper call() throws IOException, InterruptedException {
            return openNewSession();
        }
    };

    private ZooKeeper openNewSession() throws InterruptedException, IOException {
        int attempts = 0;
        int retryDelay = 10;

        while (true) {
            try {
                logger.warn("connecting to {}", connectString);
                zk = new ZooKeeper(connectString, 5000, new StateWatcher());
                try {
                    zk.create(base, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException e) {
                    // ignore
                }
                zk.create(myStateFilePath, myDescription, ZooDefs.Ids.READ_ACL_UNSAFE, CreateMode.EPHEMERAL);

                readState(base);
                return zk;
            } catch (IOException e) {
                logger.warn("Error connecting to Zookeeper", e);
                attempts++;
                if (maxConnectAttempts != 0 && attempts >= maxConnectAttempts) {
                    throw e;
                }
                retryDelay *= 1.5;
                if (retryDelay > maxRetryDelay) {
                    retryDelay = maxRetryDelay;
                }
                Thread.sleep(retryDelay);
            } catch (KeeperException e) {
                status = Status.FAILED;
                logger.error("Failed to establish state, giving up", e);
                throw new IOException(String.format("Server status node for server %d already exists in Zookeeper", myUniqueId), e);
            }
        }
    }

    public void setMaxRetryDelay(int maxRetryDelay) {
        this.maxRetryDelay = maxRetryDelay;
    }

    public void setMaxConnectAttempts(int maxConnectAttempts) {
        this.maxConnectAttempts = maxConnectAttempts;
    }

    // exposed for testing
    public ZooKeeper getZk() {
        return zk;
    }
}
