/*
 * Copyright MapR Technologies, $year
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

package com.mapr.franz.hazel;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;

/**
 * Provides Hazel-style serialization for Protobuf messages.  This lets protobuf messages
 * to be stored in Hazel distributed data structures as well as passed as state in
 * distributed tasks.
 *
 * To use this, all you have to do is define a parse() method to parse your particular
 * brand of protobuf.
 */
public abstract class ProtoSerializable<T extends GeneratedMessage> implements DataSerializable {
    private T data;

    // for use by serialization framework only
    ProtoSerializable() {
    }

    public ProtoSerializable(T data) {
        this.data = data;
    }

    @Override
    public void writeData(DataOutput out) throws IOException {
        byte[] bytes = getProto().toByteArray();
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    @Override
    public void readData(DataInput in) throws IOException {
        int n = in.readInt();
        byte[] bytes = new byte[n];
        in.readFully(bytes);
        data = parse(bytes);
    }

    public T getProto() {
        return data;
    }

    protected void setProto(T proto) {
        this.data = proto;
    }

    protected abstract T parse(byte[] bytes) throws InvalidProtocolBufferException;
}
