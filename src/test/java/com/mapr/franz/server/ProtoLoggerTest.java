package com.mapr.franz.server;

import com.google.protobuf.ByteString;
import com.mapr.franz.catcher.wire.MessageQueue;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

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
                FileInputStream in = new FileInputStream(s);
                MessageQueue.Message x = MessageQueue.Message.parseDelimitedFrom(in);
                while (x != null) {
                    assertTrue(String.format("Time %d should be in [%d,%d]", x.getTime(), t0, t1), x.getTime() >= t0 && x.getTime() <= t1);
                    assertTrue(x.hasPayload());
                    double z = Double.parseDouble(new String(x.getPayload().toByteArray()));
                    assertTrue(z >= 1e9 && z < 1e9 + 4000);
                    x = MessageQueue.Message.parseDelimitedFrom(in);
                }
            }
        }
        // all but one of the directories have one short file
        assertEquals(9, shortFileCount);
    }
}
