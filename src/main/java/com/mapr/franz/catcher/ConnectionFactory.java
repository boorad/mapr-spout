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

package com.mapr.franz.catcher;

import com.googlecode.protobuf.pro.duplex.PeerInfo;

import java.io.IOException;

/**
 * Encapsulates the connection process so that we can replace it during testing.
 */
public class ConnectionFactory {
    public CatcherConnection create(PeerInfo server) throws IOException {
        return NetworkCatcherConnection.connect(server);
    }
}
