/*
 * Copyright MapR Technologies, 2013
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

package com.mapr.franz;

import com.google.protobuf.InvalidProtocolBufferException;
import com.mapr.franz.catcher.Client;
import com.mapr.franz.catcher.wire.Catcher;

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
        setProto(sb.build());
    }

    // for framework use only
    public Server() {
        super();
    }

    @Override
    protected Catcher.Server parse(byte[] bytes) throws InvalidProtocolBufferException {
        return Catcher.Server.parseFrom(bytes);
    }
}
