package com.mapr.franz.hazel;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.HazelcastInstance;
import com.mapr.franz.catcher.wire.Catcher;
import com.mapr.franz.server.ClusterState;
import com.mapr.franz.server.GhettoTopicLogger;

/**
 * The kinda broken server.
 */
public class CatcherImpl implements
        Catcher.CatcherService.BlockingInterface {

    // TODO: configuration option
    private final String basePath = "/tmp/mapr-spout-test";

    // TODO: replace with proto based logger
    private final GhettoTopicLogger logger = new GhettoTopicLogger(basePath);

    private final long serverId;
    private Server us;
    private HazelcastInstance instance;

    // TODO implement some sort of statistics that records (a) number of
    // clients, (b) transactions per topic, (c) bytes per topic

    public CatcherImpl(Server us, HazelcastInstance instance) {
        this.serverId = us.getProto().getServerId();
        this.us = us;
        this.instance = instance;
    }

    @Override
    public Catcher.HelloResponse hello(RpcController controller,
                                       Catcher.Hello request) throws ServiceException {
        Catcher.HelloResponse.Builder r = Catcher.HelloResponse.newBuilder()
                .setServerId(serverId);

        for (Server server : instance.<Server>getSet("servers")) {
            r.addCluster(server.getProto());
        }

        r.addAllHost(us.getProto().getHostList());

        return r.build();
    }
    public static class LogMessage extends ProtoSerializable<Catcher.LogMessage>
        implements Callable<LogResponse>
    {
        public LogMessage(Catcher.LogMessage request) {
            this.data = request;
        }

        /**
         * Computes a result, or throws an exception if unable to do so.
         *
         * @return computed result
         * @throws Exception if unable to compute a result
         */
        @Override
        public LogResponse call() throws Exception {
            ByteString payload = data.getPayload();
            String topic = data.getTopic();
            logger.write(topic, payload);
        }

        @Override
        protected Catcher.LogMessage parse(byte[] bytes) throws InvalidProtocolBufferException {
            return Catcher.LogMessage.parseFrom(bytes);
        }
    }

    public static class LogResponse extends ProtoSerializable<Catcher.LogMessageResponse> {
        public LogResponse(Catcher.LogMessageResponse response) {
            data = response;
        }

        @Override
        protected Catcher.LogMessageResponse parse(byte[] bytes) throws InvalidProtocolBufferException {
            return Catcher.LogMessageResponse.parseFrom(bytes);
        }
    }


    @Override
    public Catcher.LogMessageResponse log(RpcController controller,
                                          Catcher.LogMessage request) throws ServiceException {

        String topic = request.getTopic();

        Catcher.LogMessageResponse.Builder r = Catcher.LogMessageResponse
                .newBuilder().setServerId(serverId).setSuccessful(true);

        // forward request, possibly to ourselves via HazelCast
        DistributedTask<LogResponse> task = new DistributedTask<LogResponse>(new LogMessage(request), topic);
        ExecutorService executorService = instance.getExecutorService();
        Future<LogResponse> z = executorService.submit(task);

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
