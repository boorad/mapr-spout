package com.mapr.franz.hazel;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: tdunning
 * Date: 4/26/13
 * Time: 5:30 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class ProtoSerializable<T extends GeneratedMessage> implements DataSerializable {
    protected T data;

    @Override
    public void writeData(DataOutput out) throws IOException {
        byte[] bytes = data.toByteArray();
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    @Override
    public void readData(DataInput in) throws IOException {
        int n = in.readInt();
        byte[] bytes = new byte[n];
        data = parse(bytes);
    }

    protected abstract T parse(byte[] bytes) throws InvalidProtocolBufferException;
}
