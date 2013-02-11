package com.mapr.franz.server;

import java.io.PrintWriter;
import java.io.StringWriter;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.mapr.franz.catcher.Client;
import com.mapr.franz.catcher.wire.Catcher;

/**
 * The kinda broken server.
 */
public class CatcherServiceImpl implements
        Catcher.CatcherService.BlockingInterface {

    private final long serverId;

    private ClusterState state;

    // TODO implement some sort of statistics that records (a) number of
    // clients, (b) transactions per topic, (c) bytes per topic

    public CatcherServiceImpl(long serverId, ClusterState state) {
        this.serverId = serverId;
        this.state = state;
    }

    @Override
    public Catcher.HelloResponse hello(RpcController controller,
            Catcher.Hello request) throws ServiceException {
        Catcher.HelloResponse.Builder r = Catcher.HelloResponse.newBuilder()
                .setServerId(serverId);

        for (Client.HostPort address : state.getLocalInfo().getAddresses()) {
            Catcher.Host.Builder host = r.addHostBuilder();
            host.setHostName(address.getHost());
            host.setPort(address.getPort());
            host.build();
        }

        for (Catcher.Server server : state.getCluster()) {
            Catcher.Server.Builder c = r.addClusterBuilder();
            c.mergeFrom(server);
            c.build();
        }
        return r.build();
    }

    @Override
    public Catcher.LogMessageResponse log(RpcController controller,
            Catcher.LogMessage request) throws ServiceException {

        String topic = request.getTopic();

        Catcher.LogMessageResponse.Builder r = Catcher.LogMessageResponse
                .newBuilder().setServerId(serverId).setSuccessful(true);

        ClusterState.Target directTo = state.directTo(topic);

        if (directTo.getStatus() == ClusterState.Status.LIVE) {
            if (directTo.isRedirect()) {
                // TODO actually forward request to other server
                r.getRedirectBuilder().setTopic(topic)
                        .setServer(directTo.getServer()).build();
            }
        } else {
            ClusterStateException e = new ClusterStateException(
                    "Can't handle request ... can't see rest of cluster");
            StringWriter s = new StringWriter();

            PrintWriter pw = new PrintWriter(s);
            e.printStackTrace(pw);
            pw.close();

            r.setSuccessful(false).setBackTrace(s.toString());
        }

        return r.build();
    }

    @Override
    public Catcher.CloseResponse close(RpcController controller,
            Catcher.Close request) throws ServiceException {
        return Catcher.CloseResponse.newBuilder().build();
    }

    private class ClusterStateException extends Exception {
        private static final long serialVersionUID = -1473457816312999824L;

        public ClusterStateException(String msg) {
            super(msg);
        }
    }
}
