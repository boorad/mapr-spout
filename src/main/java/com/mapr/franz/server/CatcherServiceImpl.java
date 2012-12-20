package com.mapr.franz.server;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.mapr.franz.catcher.wire.Catcher;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.SecureRandom;

/**
 * The broken server.
 */
public class CatcherServiceImpl implements Catcher.CatcherService.BlockingInterface {
    private final long serverId = new SecureRandom().nextLong();

    @Override
    public Catcher.HelloResponse hello(RpcController controller, Catcher.Hello request) throws ServiceException {
        return Catcher.HelloResponse.newBuilder().setServerId(serverId).build();
    }

    @Override
    public Catcher.LogMessageResponse log(RpcController controller, Catcher.LogMessage request) throws ServiceException {
        UnsupportedOperationException e = new UnsupportedOperationException("Can't do a log op");
        StringWriter writer = new StringWriter();
        PrintWriter out = new PrintWriter(writer);
        e.printStackTrace(out);
        out.close();
        return Catcher.LogMessageResponse.newBuilder()
                .setServerId(serverId)
                .setSuccessful(false)
                .setBackTrace(writer.toString()).build();
    }

    @Override
    public Catcher.CloseResponse close(RpcController controller, Catcher.Close request) throws ServiceException {
        return Catcher.CloseResponse.newBuilder().build();
    }
}
