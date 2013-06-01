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

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.mapr.franz.Server;
import com.mapr.franz.catcher.Client;
import com.mapr.franz.catcher.metrics.Metrics;
import com.mapr.franz.catcher.wire.Catcher;
import com.mapr.franz.server.ProtoLogger;
import com.mapr.franz.stats.History;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.security.SecureRandom;
import java.util.Enumeration;
import java.util.List;

/**
 * The super simple broken server.
 */
public class SimpleCatcherService implements Catcher.CatcherService.BlockingInterface {
    private Logger log = LoggerFactory.getLogger(SimpleCatcherService.class);

    private static ProtoLogger logger;

    private static long serverId = 1;
    private final Server us;
    private final History recorder;

    // TODO implement some sort of statistics that records (a) number of
    // clients, (b) transactions per topic, (c) bytes per topic

    public SimpleCatcherService(int port, String basePath) throws FileNotFoundException, SocketException {
        logger = new ProtoLogger(basePath);
        recorder = new History(1, 10, 60, 300)
                .logTicks()
                .addListener(new History.Listener() {
                    @Override
                    public void tick(double t, int interval, int uniqueTopics, int messages) {
                        try {
                            logger.write("-metrics-", Metrics.DataPoint.newBuilder()
                                    .setTime(t)
                                    .setInterval(interval)
                                    .setUniqueTopics(uniqueTopics)
                                    .setMessages(messages)
                                    .build().toByteString());
                        } catch (IOException e) {
                            log.warn("Error recording metric data point", e);
                        }
                    }
                });

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
        serverId = new SecureRandom().nextLong();

        us = new Server(serverId, addresses);
    }

    @Override
    public Catcher.HelloResponse hello(RpcController controller,
                                       Catcher.Hello request) throws ServiceException {
        Catcher.HelloResponse.Builder r = Catcher.HelloResponse.newBuilder()
                .setServerId(serverId);
        r.addCluster(us.getProto());
        r.addAllHost(us.getProto().getHostList());

        return r.build();
    }

    @Override
    public Catcher.LogMessageResponse log(RpcController controller, Catcher.LogMessage request) throws ServiceException {
        try {
            String topic = request.getTopic();
            ByteString payload = request.getPayload();
            logger.write(topic, payload);
            recorder.message(topic);
            return Catcher.LogMessageResponse.newBuilder()
                    .setServerId(serverId)
                    .setSuccessful(true)
                    .build();
        } catch (IOException e) {
            StringWriter s = new StringWriter();
            PrintWriter pw = new PrintWriter(s);
            e.printStackTrace(pw);
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
    public Catcher.CloseResponse close(RpcController controller, Catcher.Close request) throws ServiceException {
        log.info("Client {} has closed connection", request.getClientId());
        return Catcher.CloseResponse.newBuilder()
                .setServerId(serverId)
                .build();

    }
}
