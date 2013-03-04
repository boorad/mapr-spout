package com.mapr;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.mapr.franz.catcher.wire.MessageQueue;
import com.mapr.storm.streamparser.StreamParser;
import org.junit.Test;

import java.io.*;
import java.nio.file.Files;
import java.util.List;

import static junit.framework.Assert.*;

public class ProtoSpoutTest {
    @Test
    public void testPartialRecord() throws IOException {
        // build a file with 2 and a half records
        File file = Files.createTempFile("foo-", ".data").toFile();
        FileOutputStream out = new FileOutputStream(file);
        byte[] bytes = writePartialFile(out);

        // now verify we read 2 reads cleanly but hold off on the third
        ProtoSpout.MessageParserFactory factory = new ProtoSpout.MessageParserFactory(new ProtoSpout.TupleParser() {
            Splitter onSpace = Splitter.on(" ");

            @Override
            public List<Object> parse(ByteString buffer) {
                return Lists.<Object>newArrayList(onSpace.split(buffer.toStringUtf8()));
            }

            @Override
            public List<String> getOutputFields() {
                throw new UnsupportedOperationException("Default operation");
            }
        });

        StreamParser parser = factory.createParser(new FileInputStream(file));

        assertTrue(file.length() > 30);

        assertEquals(0, parser.currentOffset());
        List<Object> t = parser.nextRecord();

        assertEquals(2, t.size());
        assertEquals("test", t.get(0));
        assertEquals("1", t.get(1));

        t = parser.nextRecord();

        assertEquals(2, t.size());
        assertEquals("test", t.get(0));
        assertEquals("2", t.get(1));

        // time critical section starts here ... delay > 50ms can cause failure

        // first read doesn't see a full record and thus returns null
        t = parser.nextRecord();
        assertNull(t);

        // write the remainder now
        out.write(bytes, 10, bytes.length - 10);

        // so that the repeated read succeeds
        t = parser.nextRecord();
        // end of time critical section

        assertEquals(2, t.size());
        assertEquals("test", t.get(0));
        assertEquals("3", t.get(1));

        assertNull(parser.nextRecord());

        out.close();

        out = new FileOutputStream(file);
        parser = factory.createParser(new FileInputStream(file));

        writePartialFile(out);
        t = parser.nextRecord();
        assertNotNull(t);
        t = parser.nextRecord();
        assertNotNull(t);
        try {
            for (int i = 0; i < 11; i++) {
                t = parser.nextRecord();
                assertNull(t);
            }
            fail("Should have gotten tired of waiting for final bytes");
        } catch (IOException e) {
            assertTrue(e.getMessage().startsWith("Cannot read message"));
        }
    }

    private byte[] writePartialFile(FileOutputStream out) throws IOException {
        MessageQueue.Message m1 = MessageQueue.Message.newBuilder()
                .setTime(1)
                .setPayload(ByteString.copyFromUtf8("test 1"))
                .build();

        m1.writeDelimitedTo(out);

        MessageQueue.Message m2 = MessageQueue.Message.newBuilder()
                .setTime(2)
                .setPayload(ByteString.copyFromUtf8("test 2"))
                .build();

        m2.writeDelimitedTo(out);

        MessageQueue.Message m3 = MessageQueue.Message.newBuilder()
                .setTime(3)
                .setPayload(ByteString.copyFromUtf8("test 3"))
                .build();

        ByteArrayOutputStream bos = new ByteArrayOutputStream(1000);
        m3.writeDelimitedTo(bos);

        bos.close();

        byte[] bytes = bos.toByteArray();

        out.write(bytes, 0, 10);
        out.flush();
        return bytes;
    }
}
