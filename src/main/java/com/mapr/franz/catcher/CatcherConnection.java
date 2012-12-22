package com.mapr.franz.catcher;

import com.google.protobuf.RpcController;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.mapr.franz.catcher.wire.Catcher;

/**
 * Basic server connection.
 */
public interface CatcherConnection {
    Catcher.CatcherService.BlockingInterface getService();

    RpcController getController();

    PeerInfo getServer();

    void close();

    @Override
    String toString();
}
