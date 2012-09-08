package com.mapr.com.mapr.storm;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

public class DirectoryScannerTest {

    private File tempDir;

    @After
    public void cleanupFiles() {
        for (File x : tempDir.listFiles()) {
            x.delete();
        }
        tempDir.delete();

    }

    @Test
    public void testSimpleFileRoll() throws IOException, InterruptedException {
        tempDir = Files.createTempDir();

        DirectoryScanner scanner = new DirectoryScanner(tempDir, Pattern.compile("x-.*"));

        // add some files
        Files.append("4\n5\n6\n", new File(tempDir, "x-2"), Charsets.UTF_8);
        Files.append("1\n2\n3\n", new File(tempDir, "x-1"), Charsets.UTF_8);

        // verify we read both files in order
        List<String> lines1 = CharStreams.readLines(new InputStreamReader(scanner.nextInput()));
        assertEquals(3, lines1.size());
        assertEquals("[1, 2, 3]", lines1.toString());

        List<String> lines2 = CharStreams.readLines(new InputStreamReader(scanner.nextInput()));
        assertEquals(3, lines2.size());
        assertEquals("[4, 5, 6]", lines2.toString());

        // verify that we get empty records for a bit
        List<String> empty = CharStreams.readLines(new InputStreamReader(scanner.nextInput()));
        assertEquals(0, empty.size());

        empty = CharStreams.readLines(new InputStreamReader(scanner.nextInput()));
        assertEquals(0, empty.size());

        // add another file
        Files.append("7\n8\n", new File(tempDir, "x-3"), Charsets.UTF_8);

        List<String> lines3 = CharStreams.readLines(new InputStreamReader(scanner.nextInput()));
        assertEquals(2, lines3.size());
        assertEquals("[7, 8]", lines3.toString());

        // delete an old file without a problem
        new File(tempDir, "x-1").delete();

        empty = CharStreams.readLines(new InputStreamReader(scanner.nextInput()));
        assertEquals(0, empty.size());

        // add a file that doesn't match the pattern without impact
        Files.append("9\n10\n", new File(tempDir, "y-1"), Charsets.UTF_8);

        empty = CharStreams.readLines(new InputStreamReader(scanner.nextInput()));
        assertEquals(0, empty.size());

        // out of order collation
        Files.append("9\n10\n", new File(tempDir, "x-10"), Charsets.UTF_8);

        List<String> lines4 = CharStreams.readLines(new InputStreamReader(scanner.nextInput()));
        assertEquals(2, lines4.size());
        assertEquals("[9, 10]", lines4.toString());
    }
}
