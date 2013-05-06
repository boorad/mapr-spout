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

package com.mapr.storm.test;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.mapr.TailSpout;
import com.mapr.storm.Utils;
import com.mapr.storm.streamparser.CountBlobStreamParserFactory;
import com.mapr.storm.streamparser.StreamParserFactory;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TailSpoutTest {

    private File tempDir, statusDir;

    @After
    public void cleanupFiles() throws IOException {
        File[] files = tempDir.listFiles();
        if (files != null) {
            for (File x : files) {
                assertTrue(x.delete());
            }
            Utils.deleteRecursively(tempDir);
            Utils.deleteRecursively(statusDir);
        }
    }

    @Test(timeout = 2000)
    public void testSimpleFileRoll() throws IOException, InterruptedException {
        tempDir = Files.createTempDir();
        statusDir = Files.createTempDir();

        // set up spout
        StreamParserFactory spf = new CountBlobStreamParserFactory();
        File statusFile = new File(statusDir + "/status");
        Pattern inPattern = Pattern.compile("x-.*");
        TailSpout spout = new TailSpout(spf, statusFile, tempDir, inPattern);
        spout.setReliableMode(true);

        // open spout
        Config conf = new Config();
        TopologyContext context = null;
        TestSpoutOutputCollector collector = new TestSpoutOutputCollector();
        spout.open(conf, context, collector);
        
        // add some files
        FileUtils.writeByteArrayToFile(new File(tempDir, "x-1"), payload("x-1"), true);
        FileUtils.writeByteArrayToFile(new File(tempDir, "x-2"), payload("x-2"), true);

        // verify we read both files in order
        spout.nextTuple();
        List<Object> tuple1 = collector.getTuple();
        assertEquals("x-1", new String((byte[]) tuple1.get(0), Charsets.UTF_8));

        spout.nextTuple();
        List<Object> tuple2 = collector.getTuple();
        Object msgId2 = collector.getMessageId();
        assertEquals("x-2", new String((byte[]) tuple2.get(0), Charsets.UTF_8));

        // verify that we get empty records for a bit
        // the fact that we get tuple2 values is because no 'emit' hits
        // collector, so values are unchanged.
        spout.nextTuple();
        List<Object> tuple3 = collector.getTuple();
        assertEquals("x-2", new String((byte[]) tuple3.get(0), Charsets.UTF_8));
        assertEquals(msgId2, collector.getMessageId());

        // delete an old file without a problem
        assertTrue(new File(tempDir, "x-1").delete());

        spout.nextTuple();
        List<Object> tuple4 = collector.getTuple();
        assertEquals("x-2", new String((byte[]) tuple4.get(0), Charsets.UTF_8));
        assertEquals(msgId2, collector.getMessageId());

        // add a file that doesn't match the pattern without impact
        FileUtils.writeByteArrayToFile(new File(tempDir, "y-1"), 
        		payload("y-1"), true);
        spout.nextTuple();
        List<Object> tuple5 = collector.getTuple();
        assertEquals("x-2", new String((byte[]) tuple5.get(0), Charsets.UTF_8));
        assertEquals(msgId2, collector.getMessageId());

        
        // append content to an existing file
        FileUtils.writeByteArrayToFile(new File(tempDir, "x-1"), payload("x-11"), true);
        spout.nextTuple();
        List<Object> tuple11 = collector.getTuple();
        assertEquals("x-11", new String((byte[]) tuple11.get(0), Charsets.UTF_8));

    }
    
    private byte[] payload(String msg) {
        byte[] content = msg.getBytes(Charsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(4 + content.length);
        buf.putInt(content.length);
        buf.put(content);
        buf.flip();
        return buf.array();
    }
}
