package com.mapr.storm.test;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;

import com.google.common.io.Files;
import com.mapr.TailSpout;
import com.mapr.storm.StreamParserFactory;

public class TailSpoutTest {

    private File tempDir, statusDir;

    @After
    public void cleanupFiles() {
        for (File x : tempDir.listFiles()) {
            x.delete();
        }
        tempDir.delete();
        statusDir.delete();
    }

    @Test
    public void testSimpleFileRoll() throws IOException, InterruptedException {
        tempDir = Files.createTempDir();
        statusDir = Files.createTempDir();

        // set up spout
        StreamParserFactory spf = new TestStreamParserFactory();
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
        FileUtils.writeByteArrayToFile(new File(tempDir, "x-2"), 
        		payload("x-2"), true);
        File file1 = new File(tempDir, "x-1");
        FileUtils.writeByteArrayToFile(file1, payload("x-1"), true);

        // verify we read both files in order
        spout.nextTuple();
        List<Object> tuple1 = collector.getTuple();
        assertEquals("x-1", tuple1.get(0).toString());

        spout.nextTuple();
        List<Object> tuple2 = collector.getTuple();
        Object msgId2 = collector.getMessageId();
        assertEquals("x-2", tuple2.get(0).toString());

        // verify that we get empty records for a bit
        // the fact that we get tuple2 values is because no 'emit' hits
        // collector, so values are unchanged.
        spout.nextTuple();
        List<Object> tuple3 = collector.getTuple();
        assertEquals("x-2", tuple3.get(0).toString());
        assertEquals(msgId2, collector.getMessageId());

        // delete an old file without a problem
        new File(tempDir, "x-1").delete();

        spout.nextTuple();
        List<Object> tuple4 = collector.getTuple();
        assertEquals("x-2", tuple4.get(0).toString());
        assertEquals(msgId2, collector.getMessageId());

        // add a file that doesn't match the pattern without impact
        FileUtils.writeByteArrayToFile(new File(tempDir, "y-1"), 
        		payload("y-1"), true);
        spout.nextTuple();
        List<Object> tuple5 = collector.getTuple();
        assertEquals("x-2", tuple5.get(0).toString());
        assertEquals(msgId2, collector.getMessageId());

        
        // append content to an existing file
        FileUtils.writeByteArrayToFile(file1, payload("x-11"), true);
        spout.nextTuple();
        List<Object> tuple11 = collector.getTuple();
        assertEquals("x-11", tuple11.get(0).toString());

    }
    
    private byte[] payload(String msg) {
		try {
			byte[] content = msg.getBytes("UTF-8");
	    	byte[] size = ByteBuffer.allocate(4).putInt(content.length).array();
	    	int i = content.length + 4;
	    	byte[] ret = new byte[i];
	    	System.arraycopy(size, 0, ret, 0, size.length);
	    	System.arraycopy(content, 0, ret, size.length, content.length);
	    	return ret;
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return null;
    }
}
