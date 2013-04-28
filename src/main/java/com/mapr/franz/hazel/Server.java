package com.mapr.franz.hazel;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hazelcast.nio.DataSerializable;
import com.mapr.franz.catcher.Client;
import com.mapr.franz.catcher.wire.Catcher;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

/**
 * Hazel serializable state for a server.
 */
public class Server extends ProtoSerializable<Catcher.Server> {

    public Server(long id, Iterable<Client.HostPort> addresses) {
        Catcher.Server.Builder sb = Catcher.Server.newBuilder()
                .setServerId(id);
        for (Client.HostPort address : addresses) {
            sb.addHostBuilder()
                    .setHostName(address.getHost())
                    .setPort(address.getPort())
                    .build();
        }
        data = sb.build();
    }

    public Catcher.Server getProto() {
        return data;
    }

    @Override
    protected Catcher.Server parse(byte[] bytes) throws InvalidProtocolBufferException {
        return Catcher.Server.parseFrom(bytes);
    }
}
