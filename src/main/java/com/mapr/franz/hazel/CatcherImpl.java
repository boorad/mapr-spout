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

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.HazelcastInstance;
import com.mapr.franz.catcher.wire.Catcher;
import com.mapr.franz.server.ProtoLogger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * The kinda broken server.
 */
public class CatcherImpl implements Catcher.CatcherService.BlockingInterface {

    // TODO: configuration option
    private final String basePath = "/tmp/mapr-spout-test";

    // TODO: replace with proto based logger
    private final ProtoLogger logger;

    private final long serverId;
    private Server us;
    private HazelcastInstance instance;

    // TODO implement some sort of statistics that records (a) number of
    // clients, (b) transactions per topic, (c) bytes per topic

    public CatcherImpl(Server us, HazelcastInstance instance) throws FileNotFoundException {
        this.serverId = us.getProto().getServerId();
        this.us = us;
        this.instance = instance;
        logger = new ProtoLogger(basePath);
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

    public class LogMessage extends ProtoSerializable<Catcher.LogMessage>
            implements Callable<LogResponse> {
        public LogMessage(Catcher.LogMessage request) {
            this.data = request;
        }

        /**
         * Computes a result, or throws an exception if unable to do so.
         *
         * @return computed result
         */
        @Override
        public LogResponse call() throws IOException {
            ByteString payload = data.getPayload();
            String topic = data.getTopic();
            logger.write(topic, payload);
            return new LogResponse(
                    Catcher.LogMessageResponse.newBuilder()
                            .setServerId(serverId)
                            .setSuccessful(true)
                            .build()
            );
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
        try {
            // forward request, possibly to ourselves via HazelCast
            String topic = request.getTopic();
            DistributedTask<LogResponse> task = new DistributedTask<>(new LogMessage(request), topic);
            instance.getExecutorService().execute(task);

            return task.get().data;
        } catch (InterruptedException | ExecutionException e) {
            StringWriter s = new StringWriter();
            PrintWriter pw = new PrintWriter(s);
            new ClusterStateException("Can't handle request ... can't see rest of cluster", e).printStackTrace(pw);
            pw.close();
            return Catcher.LogMessageResponse
                    .newBuilder()
                    .setServerId(serverId)
                    .setSuccessful(false)
                    .setBackTrace(s.toString())
                    .build();
        }
    }

    @Override
    public Catcher.CloseResponse close(RpcController controller,
                                       Catcher.Close request) throws ServiceException {
        return Catcher.CloseResponse.newBuilder().build();
    }

    private class ClusterStateException extends Exception {
        private static final long serialVersionUID = -1473457816312999824L;

        public ClusterStateException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }
}
