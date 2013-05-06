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
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mapr.storm.DirectoryScanner;
import com.mapr.storm.PendingMessage;
import com.mapr.storm.SpoutState;
import com.mapr.storm.streamparser.StreamParser;
import com.mapr.storm.streamparser.StreamParserFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
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
public class TailSpout extends BaseRichSpout {

    private static final long serialVersionUID = -3911379881516049619L;

    private final Logger log = LoggerFactory.getLogger(TailSpout.class);

    private DirectoryScanner scanner;
    private FileInputStream currentInput = null;

    private AtomicBoolean replayFailedMessages = new AtomicBoolean(true);

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
    private SpoutOutputCollector collector;
    private double nextCheckPointTime = 0;

    public TailSpout(StreamParserFactory factory, File statusFile) throws IOException {
        this.factory = factory;
        scanner = SpoutState.restoreState(pendingReplays, statusFile);
    }

    public TailSpout(StreamParserFactory factory, File statusFile, File inputDirectory, final Pattern inputFileNamePattern) throws IOException {
        this.factory = factory;
        this.statusFile = statusFile;
        if (statusFile.exists() && statusFile.length() > 0) {
            scanner = SpoutState.restoreState(pendingReplays, statusFile);
        } else {
            scanner = new DirectoryScanner(inputDirectory, inputFileNamePattern);
        }
    }

    public boolean setReliableMode(boolean replayFailedMessages) {
        return this.replayFailedMessages.getAndSet(replayFailedMessages);
    }

    public void setTuplesPerCheckpoint(long tuplesPerCheckpoint) {
        this.tuplesPerCheckpoint = tuplesPerCheckpoint;
    }

    public void setCheckPointIntervalSeconds(double checkPointIntervalSeconds) {
        this.checkPointIntervalSeconds = checkPointIntervalSeconds;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(factory.getOutputFields()));
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map map, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void close() {
        SpoutState.recordCurrentState(ackBuffer, scanner, parser, statusFile);
    }

    @Override
    public void deactivate() {
        SpoutState.recordCurrentState(ackBuffer, scanner, parser, statusFile);
    }

    @Override
    public void nextTuple() {
        if (currentInput == null) {
            currentInput = openNextInput();
        }

        try {
            if (currentInput != null) {
                // read a record
                long position = parser.currentOffset();
                List<Object> r = parser.nextRecord();

                // assert currentInput != null
                if (r == null) {
                    // reached end of current file.  Try just once to
                    // open next input (if it exists) and read again.
                    // if we still get nothing, we stay with the current
                    // file
                    currentInput = openNextInput();

                    // assert r == null
                    if (currentInput != null) {
                        position = parser.currentOffset();
                        r = parser.nextRecord();
                    }
                }

                if (r != null) {
                    if (replayFailedMessages.get()) {
                        collector.emit(r, messageId);
                        ackBuffer.put(messageId, new PendingMessage(scanner.getLiveFile(), position, r));
                        messageId++;
                    } else {
                        collector.emit(r);
                    }

                    // persist current reading state if it is time
                    if (messageId % tuplesPerCheckpoint == 0 || System.nanoTime() / 1e9 > nextCheckPointTime) {
                        SpoutState.recordCurrentState(ackBuffer, scanner, parser, statusFile);
                        nextCheckPointTime = System.nanoTime() / 1e9 + checkPointIntervalSeconds;
                    }
                }
            }
            // exit after emitting a tuple or when end of current input is found
        } catch (IOException e) {
            throw new RuntimeException(e);
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
        if (r != null) {
            parser = factory.createParser(r);
            return r;
        } else {
            return currentInput;
        }
    }

    @Override
    public void ack(Object messageId) {
        if (messageId instanceof Long) {
            if (ackBuffer.remove(messageId) == null) {
                log.error("Unknown messageId {}", messageId);
            }
        } else {
            log.error("Incorrect message id {}", messageId);
        }
    }

    @Override
    public void fail(Object messageId) {
        if (messageId instanceof Long) {
            final PendingMessage message = ackBuffer.get(messageId);
            if (message != null) {
                collector.emit(message.getTuple(), messageId);
            } else {
                log.error("Unknown message id {}", messageId);
            }
        } else {
            log.error("Incorrect message id {}", messageId);
        }
    }
}
