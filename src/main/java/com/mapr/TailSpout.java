package com.mapr;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.google.common.collect.Maps;
import com.mapr.com.mapr.storm.DirectoryScanner;
import com.mapr.com.mapr.storm.StreamParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * This is a spout that reads records from files.  New records can be appended to these files
 * while this spout is running.  This will cause these new records to be emitted the next time
 * around the event loop.  It is also possible that we will hit the end of a file of records and
 * notice that a new file has been started.  In that case, we will move to that new file and
 * start emitting the records we read from that file.
 */
public class TailSpout<T extends Tuple> implements IRichSpout {
    private final Logger log = LoggerFactory.getLogger(TailSpout.class);

    private StreamParser<InputStream> parser;
    private SpoutOutputCollector collector;
    private DirectoryScanner scanner;
    private InputStream currentInput = null;

    private Map<Long, List<Object>> ackBuffer = Maps.newTreeMap();
    private long messageId = 0;

    private boolean replayFailedMessages = false;

    public TailSpout(StreamParser<InputStream> parser, File statusFile, File inputDirectory, final Pattern inputFileNamePattern) {
        this.parser = parser;
        scanner = new DirectoryScanner(inputDirectory, inputFileNamePattern);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        parser.declareOutputFields(outputFieldsDeclarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        throw new UnsupportedOperationException("Default operation");
    }

    @Override
    public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        currentInput = scanner.nextInput();
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Default operation");
    }

    @Override
    public void activate() {
        throw new UnsupportedOperationException("Default operation");
    }

    @Override
    public void deactivate() {
        throw new UnsupportedOperationException("Default operation");
    }

    @Override
    public void nextTuple() {
        if (currentInput == null) {
            // look for a new file
            currentInput = scanner.nextInput();
        }

        while (currentInput != null) {
            // read a record
            List<Object> r = parser.nextRecord(currentInput);

            // assert currentInput != null
            if (r == null) {
                // reached end of current file
                // (currentInput != null && r == null) so we enter loop at least once
                while (currentInput != null && r == null) {
                    currentInput = scanner.nextInput();

                    // assert r == null
                    if (currentInput != null) {
                        r = parser.nextRecord(currentInput);
                    }
                    // r != null => currentInput != null
                }
                // post: r != null iff currentInput != null
            }
            // post: (r != null iff currentInput != null) || (r != null)
            // post: (r == null => currentInput == null)

            if (r != null) {
                if (replayFailedMessages) {
                    collector.emit(r, messageId);
                    ackBuffer.put(messageId, r);
                    messageId++;
                } else {
                    collector.emit(r);
                }
            }
        }
        // exit only when all files have been processed completely
    }

    @Override
    public void ack(Object messageId) {
        if (messageId instanceof Long) {
            if (ackBuffer.remove(messageId) == null) {
                log.error("Invalid messageId {}", messageId);
            }
        } else {
            log.error("Incorrect message id {}", messageId);
        }
    }

    @Override
    public void fail(Object messageId) {
        if (messageId instanceof Long) {
            final List<Object> message = ackBuffer.get(messageId);
            if (message != null) {
                collector.emit(message, messageId);
            } else {
                log.error("Incorrect message id {}", messageId);
            }
        } else {
            log.error("Incorrect message id {}", messageId);
        }
    }

    public boolean setReplayFailedMessages(boolean replayFailedMessages) {
        boolean old = this.replayFailedMessages;
        this.replayFailedMessages = replayFailedMessages;
        return old;
    }
}
