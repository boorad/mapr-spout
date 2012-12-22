package com.mapr.franz.catcher;

import com.googlecode.protobuf.pro.duplex.PeerInfo;

import java.io.IOException;

/**
 * Encapsulates the connection process so that we can replace it during testing.
 */
public interface ConnectionFactory {
    CatcherConnection create(PeerInfo server) throws IOException;
}
