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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.regex.Pattern;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import com.mapr.storm.DirectoryScanner;

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
        FileInputStream in = scanner.nextInput();
        Assert.assertNull(in);

        in = scanner.nextInput();
        Assert.assertNull(in);

        // add another file
        Files.append("7\n8\n", new File(tempDir, "x-3"), Charsets.UTF_8);

        List<String> lines3 = CharStreams.readLines(new InputStreamReader(scanner.nextInput()));
        assertEquals(2, lines3.size());
        assertEquals("[7, 8]", lines3.toString());

        // delete an old file without a problem
        new File(tempDir, "x-1").delete();

        in = scanner.nextInput();
        Assert.assertNull(in);

        // add a file that doesn't match the pattern without impact
        Files.append("9\n10\n", new File(tempDir, "y-1"), Charsets.UTF_8);

        in = scanner.nextInput();
        Assert.assertNull(in);

        // out of order collation
        Files.append("9\n10\n", new File(tempDir, "x-10"), Charsets.UTF_8);

        List<String> lines4 = CharStreams.readLines(new InputStreamReader(scanner.nextInput()));
        assertEquals(2, lines4.size());
        assertEquals("[9, 10]", lines4.toString());
    }
}
