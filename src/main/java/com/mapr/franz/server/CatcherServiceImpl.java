package com.mapr.franz.server;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.mapr.franz.catcher.wire.Catcher;

/**
 * The broken server.
 */
public class CatcherServiceImpl implements Catcher.CatcherService.BlockingInterface {
    @Override
    public Catcher.HelloResponse hello(RpcController controller, Catcher.Hello request) throws ServiceException {
        throw new UnsupportedOperationException("Default operation");
    }

    @Override
    public Catcher.LogMessageResponse log(RpcController controller, Catcher.LogMessage request) throws ServiceException {
        throw new UnsupportedOperationException("Default operation");
    }

    @Override
    public Catcher.CloseResponse close(RpcController controller, Catcher.Close request) throws ServiceException {
        throw new UnsupportedOperationException("Default operation");
    }
}
