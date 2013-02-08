package com.mapr.storm.test;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.mapr.storm.DirectoryScanner;
import com.mapr.storm.PendingMessage;
import com.mapr.storm.SpoutState;
import com.mapr.storm.StreamParser;

/**
 * Test state handling.
 */
public class SpoutStateTest {

    private File inputDir;

    @Test
    public void testSaveRestore() {
    }

    @Test
    public void testSaveRegression() throws IOException {
        inputDir = Files.createTempDir();
        File statusFile = File.createTempFile("state", ".json");
        statusFile.deleteOnExit();

        Map<Long, PendingMessage> emptyAcks = Maps.newHashMap();
        final DirectoryScanner scanner = new DirectoryScanner(inputDir, Pattern.compile(".*\\.log"));

        final File logFile = new File(inputDir, "test");
        Files.write("1\n2\n3\n", logFile, Charsets.UTF_8);
        scanner.forceInput(logFile, 2);

        SpoutState.recordCurrentState(emptyAcks, scanner, new FakeParser(), statusFile);
        SpoutState rs = SpoutState.fromString(Files.toString(statusFile, Charsets.UTF_8));
        assertEquals(0, rs.getOldFiles().size());
        assertEquals(1, rs.getOffsets().size());
        Map.Entry<File, Long> entry = rs.getOffsets().entrySet().iterator().next();
        assertEquals(10247, (long) entry.getValue());
        assertEquals("test", entry.getKey().getName());
        assertEquals(".*\\.log", rs.getFilePattern().toString());

        Queue<PendingMessage> q = Lists.newLinkedList();
        DirectoryScanner s2 = SpoutState.restoreState(q, statusFile);
        assertEquals(1, q.size());
        assertEquals(0, s2.getOldFiles().size());
        assertEquals(scanner.getFileNamePattern().toString(), s2.getFileNamePattern().toString());
        assertEquals(scanner.getInputDirectory().toString(), s2.getInputDirectory().toString());
    }

    @After
    public void cleanup() {
        if (inputDir != null) {
            for (File f: inputDir.listFiles()) {
                f.delete();
            }
            inputDir.delete();
        }
    }

    private class FakeParser extends StreamParser {
        @Override
        public long currentOffset() {
            return 10247;
        }

        @Override
        public List<Object> nextRecord() {
            throw new UnsupportedOperationException("Default operation");
        }
    }
}
