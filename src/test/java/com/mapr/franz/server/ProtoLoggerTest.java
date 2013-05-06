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

package com.mapr.franz.server;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.mapr.ProtoSpout;
import com.mapr.franz.catcher.wire.MessageQueue;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

public class ProtoLoggerTest {
    @Test
    public void testWrite() throws IOException {
        Path homeDir = Files.createTempDirectory("logger");
        ProtoLogger p = new ProtoLogger(homeDir.toString());
        p.setMaxLogFile(200);

        long t0 = System.currentTimeMillis();

        for (int i = 0; i < 4000; i++) {
            p.write("topic-" + (i % 10), ByteString.copyFromUtf8(1e9 + i + ""));
        }

        long t1 = System.currentTimeMillis();

        assertEquals(10, homeDir.toFile().list().length);

        int shortFileCount = 0;
        File[] topicFiles = homeDir.toFile().listFiles();
        assertNotNull(topicFiles);
        for (File topic : topicFiles) {
            assertTrue(topic.getName().matches("topic-[0-9]+"));
            File[] files = topic.listFiles();
            assertNotNull(files);

            Arrays.sort(files);
            long offset = 0;
            for (File s : files) {
                assertTrue(s.getName().matches("[0-9a-f]+"));
                if (s.length() < 200) {
                    shortFileCount++;
                }
                assertTrue(s.length() < 250);
                assertEquals(String.format("%016x", offset), s.getName());
                offset += s.length();

                DataInputStream in = new DataInputStream(new FileInputStream(s));
                MessageQueue.Message x = getMessage(in);
                while (x != null) {
                    assertTrue(String.format("Time %d should be in [%d,%d]", x.getTime(), t0, t1), x.getTime() >= t0 && x.getTime() <= t1);
                    assertTrue(x.hasPayload());
                    double z = Double.parseDouble(new String(x.getPayload().toByteArray()));
                    assertTrue(z >= 1e9 && z < 1e9 + 4000);
                    x = getMessage(in);
                }
            }
        }
        // only one of the directories have a short file
        assertEquals(1, shortFileCount);
    }

    private MessageQueue.Message getMessage(DataInputStream in) throws IOException {
        try {
            int n = in.readInt();
            byte[] buf = new byte[n];
            in.readFully(buf);
            return MessageQueue.Message.parseFrom(buf);
        } catch (EOFException e) {
            return null;
        }
    }
}
