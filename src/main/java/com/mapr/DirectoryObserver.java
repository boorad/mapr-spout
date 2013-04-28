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

package com.mapr;

import backtype.storm.spout.SpoutOutputCollector;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mapr.storm.DirectoryScanner;
import com.mapr.storm.PendingMessage;
import com.mapr.storm.SpoutState;
import com.mapr.storm.streamparser.StreamParser;
import com.mapr.storm.streamparser.StreamParserFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.regex.Pattern;

/**
 * This is a spout that reads records from files. New records can be appended to
 * these files while this spout is running. This will cause these new records to
 * be emitted the next time around the event loop. It is also possible that we
 * will hit the end of a file of records and notice that a new file has been
 * started. In that case, we will move to that new file and start emitting the
 * records we read from that file.
 * <p/>
 * There are two modes in which a TailSpout can operate. In unreliable mode,
 * transactions are read, but ack's and fail's are ignored so that if
 * transactions are not processed, they are lost. The TailSpout will checkpoint
 * it's own progress periodically and on deactivation so that when the TailSpout
 * is restarted, it will start from close to where it left off. The restart will
 * be exact with a clean shutdown and can be made arbitrarily precise with an
 * unclean shutdown.
 * <p/>
 * In the reliable mode, an in-memory table of unacknowledged transactions is
 * kept. Any that fail are retransmitted and those that succeed are removed from
 * the table. When a checkpoint is done, the file name and offset of the
 * earliest unacknowledged transaction in that file will be retained. On
 * restart, each of the files with unacknowledged transactions will be replayed
 * from that point. Usually only one file will be replaced (the current one) or
 * possibly the current and previous file. This will generally replay a few
 * extra transactions, but if there is a cool-down period on shutdown, this will
 * should very few extra transactions transmitted on startup.
 * <p/>
 * One possible extension would be to allow the system to run in reliable mode,
 * but not save the offsets for unacked transactions. This would be good during
 * orderly shutdowns, but very bad in the event of an unorderly shutdown.
 */
public class DirectoryObserver implements Serializable, Closeable {
    private final Logger log = LoggerFactory.getLogger(DirectoryObserver.class);

    private DirectoryScanner scanner;
    private FileInputStream currentInput = null;

    // these are set in the constructors
    private StreamParserFactory factory;
    private File statusFile;

    // all others are created on the fly
    private Map<Long, PendingMessage> ackBuffer = Maps.newTreeMap();
    private long messageId = 0;

    // how often should we save our state?
    private long tuplesPerCheckpoint = 100;

    // time between forced checkpoints in seconds
    private double checkPointIntervalSeconds = 1.0;
    private Queue<PendingMessage> pendingReplays = Lists.newLinkedList();

    private StreamParser parser = null;
    private double nextCheckPointTime = 0;

    public DirectoryObserver(StreamParserFactory factory, File statusFile)
            throws IOException {
        this.factory = factory;
        scanner = SpoutState.restoreState(pendingReplays, statusFile);
    }

    public DirectoryObserver(StreamParserFactory factory, File statusFile,
                             File inputDirectory, final Pattern inputFileNamePattern) {
        this.factory = factory;
        this.statusFile = statusFile;
        scanner = new DirectoryScanner(inputDirectory, inputFileNamePattern);
    }

    public void setTuplesPerCheckpoint(long tuplesPerCheckpoint) {
        this.tuplesPerCheckpoint = tuplesPerCheckpoint;
    }

    public void setCheckPointIntervalSeconds(double checkPointIntervalSeconds) {
        this.checkPointIntervalSeconds = checkPointIntervalSeconds;
    }

    public void close() {
        SpoutState.recordCurrentState(ackBuffer, scanner, parser, statusFile);
    }

    public Message nextMessage() {
        if (currentInput == null) {
            currentInput = openNextInput();
        }

        try {
            // TODO need to persist current reading state somewhere
            while (currentInput != null) {
                // read a record
                long position = parser.currentOffset();
                List<Object> r = parser.nextRecord();

                // assert currentInput != null
                if (r == null) {
                    // reached end of current file
                    // (currentInput != null && r == null) so we enter loop at least
                    // once
                    while (currentInput != null && r == null) {
                        currentInput = openNextInput();

                        // assert r == null
                        if (currentInput != null) {
                            position = parser.currentOffset();
                            r = parser.nextRecord();
                        }
                        // r != null => currentInput != null
                    }
                    // post: r != null iff currentInput != null
                }
                // post: (r != null iff currentInput != null) || (r != null)
                // post: (r == null => currentInput == null)

                if (r != null) {
                    messageId++;
                    if (messageId % tuplesPerCheckpoint == 0
                            || System.nanoTime() / 1e9 > nextCheckPointTime) {
                        SpoutState.recordCurrentState(ackBuffer, scanner, parser,
                                statusFile);
                        nextCheckPointTime = System.nanoTime() / 1e9
                                + checkPointIntervalSeconds;
                    }
                    return new Message(messageId, r);
                }
            }
        } catch (IOException e) {
            log.error("DirectoryObserver threw I/O exception", e);
            throw new RuntimeException(e);
        }
        // exit only when all files have been processed completely
        return null;
    }

    public static class Message {
        private final long messageId;
        private final List<Object> tuple;

        public Message(long messageId, List<Object> tuple) {
            this.messageId = messageId;
            this.tuple = tuple;
        }

        public long getMessageId() {
            return messageId;
        }

        public List<Object> getTuple() {
            return tuple;
        }
    }

    private FileInputStream openNextInput() {
        PendingMessage next = pendingReplays.poll();
        while (next != null) {
            if (next.getFile().exists()) {
                return scanner.forceInput(next.getFile(), next.getOffset());
            } else {
                log.error("Replay file {} has disappeared", next.getFile());
            }
            next = pendingReplays.poll();
        }

        // look for a new file
        FileInputStream r = scanner.nextInput();
        parser = factory.createParser(r);
        return r;
    }
}
