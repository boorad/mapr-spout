package com.mapr.franz.catcher;

import com.google.protobuf.RpcController;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.mapr.franz.catcher.wire.Catcher;

/**
 * Base implementation of a CatcherConnection to simplify tests
 */
public class FakeConnection implements CatcherConnection {
    @Override
    public Catcher.CatcherService.BlockingInterface getService() {
        throw new UnsupportedOperationException("Default operation");
    }

    @Override
    public RpcController getController() {
        return null;
    }


    @Override
    public PeerInfo getServer() {
        throw new UnsupportedOperationException("Default operation");
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Default operation");
    }
}
