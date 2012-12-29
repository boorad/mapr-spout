package com.mapr.franz.catcher;

import com.googlecode.protobuf.pro.duplex.PeerInfo;

public class CatcherConnectionFactory implements ConnectionFactory {
    @Override
    public CatcherConnection create(PeerInfo server) {
        return CatcherConnection.connect(server);
    }
}
