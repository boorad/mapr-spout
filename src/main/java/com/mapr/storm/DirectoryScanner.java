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

package com.mapr.storm;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Queue;
import java.util.Set;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;

/**
 * Scans through files in a directory as they appear.
 * <p/>
 * When you have finished processing a file, you can call
 * nextInput() to get a stream to read from the next file.  If there isn't a new file to read
 * from, you will get a null.
 * <p/>
 * Eventually, when you call nextInput() again, if a new file or files has appeared in the meantime
 * you will get streams that read from these new file(s).  You should probably do something to
 * moderate the rate at which you come back for new files, but other than that, this object
 * gives you an infinite sequence of input streams whose concatenation is the concatenation
 * of all the files that have ever appeared in this directory.
 */
public class DirectoryScanner implements Serializable {

	private static final long serialVersionUID = -8743538912863486122L;

	private Logger log = LoggerFactory.getLogger(DirectoryScanner.class);

    private final File inputDirectory;
    private final Pattern fileNamePattern;

    private Set<File> oldFiles = Sets.newHashSet();
    private Queue<File> pendingFiles = Queues.newConcurrentLinkedQueue();
    private InputStream liveInput = null;
    private File liveFile;

    public DirectoryScanner(File inputDirectory, Pattern fileNamePattern) {
        Preconditions.checkArgument(inputDirectory.exists(), String.format("Directory %s should already exist", inputDirectory));
        this.inputDirectory = inputDirectory;
        this.fileNamePattern = fileNamePattern;
    }

    private File scanForFiles() {
        if (pendingFiles.size() == 0) {
            Set<File> files = Sets.newTreeSet(Lists.newArrayList(inputDirectory.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File file, String s) {
                    return fileNamePattern.matcher(s).matches();
                }
            })));
            oldFiles.retainAll(files);
            files.removeAll(oldFiles);
            oldFiles.addAll(files);

            pendingFiles.addAll(files);
        }

        return pendingFiles.poll();
    }

    public FileInputStream nextInput() {
        FileInputStream r;

        File nextInLine = scanForFiles();
        if (nextInLine == null) {
            log.trace("No new files");
            r = null;
        } else {
            try {
                log.trace("Opening {}", nextInLine);
                r = new FileInputStream(nextInLine);
            } catch (FileNotFoundException e) {
                // bizarre, but conceivable in a directory with crazy updates happening
                log.warn("File was found in scan, but disappeared before open {}", nextInLine);
                r = null;
            }
        }

        if (nextInLine != null) {
            if (liveInput != null) {
                if (liveFile != null) {
                    log.trace("Closing {}", liveFile);
                }
                Closeables.closeQuietly(liveInput);
            }
            liveFile = nextInLine;
            liveInput = r;
        }

        return r;
    }

    public File getLiveFile() {
        return liveFile;
    }

    public Set<File> getOldFiles() {
        return oldFiles;
    }

    public File getInputDirectory() {
        return inputDirectory;
    }

    public Pattern getFileNamePattern() {
        return fileNamePattern;
    }

    public void setOldFiles(Set<File> oldFiles) {
        this.oldFiles = oldFiles;
    }

    public FileInputStream forceInput(File file, long offset) {
        FileInputStream r = null;
        try {
            liveFile = file;
            r = new FileInputStream(liveFile);
            r.getChannel().position(offset);
            liveInput = r;
        } catch (IOException e) {
            log.warn("Couldn't open replay file", e);
        }
        return r;
    }
}
