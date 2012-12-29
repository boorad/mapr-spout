package com.mapr.franz.catcher;

import com.googlecode.protobuf.pro.duplex.PeerInfo;

/**
 * Encapsulates the connection process so that we can replace it during testing.
 */
public class ConnectionFactory {
    public CatcherConnection create(PeerInfo server) {
        return CatcherConnection.connect(server);
    }
}
