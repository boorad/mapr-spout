package com.mapr;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.mapr.com.mapr.storm.DirectoryScanner;
import com.mapr.com.mapr.storm.StreamParser;
import com.mapr.com.mapr.storm.StreamParserFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * This is a spout that reads records from files.  New records can be appended to these files
 * while this spout is running.  This will cause these new records to be emitted the next time
 * around the event loop.  It is also possible that we will hit the end of a file of records and
 * notice that a new file has been started.  In that case, we will move to that new file and
 * start emitting the records we read from that file.
 *
 * There are two modes in which a TailSpout can operate.  In unreliable mode, transactions are
 * read, but ack's and fail's are ignored so that if transactions are not processed, they are
 * lost.  The TailSpout will checkpoint it's own progress periodically and on deactivation so
 * that when the TailSpout is restarted, it will start from close to where it left off.  The
 * restart will be exact with a clean shutdown and can be made arbitrarily precise with an
 * unclean shutdown.
 *
 * In the reliable mode, an in-memory table of unacknowledged transactions is kept.  Any that
 * fail are retransmitted and those that succeed are removed from the table.  When a checkpoint
 * is done, the file name and offset of the earliest unacknowledged transaction in that file
 * will be retained.  On restart, each of the files with unacknowledged transactions will be
 * replayed from that point.  Usually only one file will be replaced (the current one) or
 * possibly the current and previous file.  This will generally replay a few extra transactions,
 * but if there is a cool-down period on shutdown, this will should very few extra transactions
 * transmitted on startup.
 *
 * One possible extension would be to allow the system to run in reliable mode, but not save
 * the offsets for unacked transactions.  This would be good during orderly shutdowns, but very
 * bad in the event of an unorderly shutdown.
 */
public class TailSpout implements IRichSpout {
    private final Logger log = LoggerFactory.getLogger(TailSpout.class);

    // these fields are saved and restored on restart.
    private DirectoryScanner scanner;
    private FileInputStream currentInput = null;

    private boolean replayFailedMessages = true;

    // these are set in the constructors
    private StreamParserFactory factory;
    private File statusFile;

    // all others are created on the fly
    private Map<Long, Message> ackBuffer = Maps.newTreeMap();
    private long messageId = 0;

    // how often should we save our state?
    private long checkPointInterval = 100;
    private Queue<Message> pendingReplays = Lists.newLinkedList();

    private static class Message {
        File logFile;
        long offset;
        List<Object> tuple;

        private Message(File logFile, long offset, List<Object> tuple) {
            this.logFile = logFile;
            this.offset = offset;
            this.tuple = tuple;
        }

        public List<Object> getTuple() {
            return tuple;
        }

        public File getFile() {
            return logFile;
        }

        public long getOffset() {
            return offset;
        }
    }

    private StreamParser parser = null;
    private SpoutOutputCollector collector;

    public TailSpout(StreamParserFactory factory, File statusFile) throws IOException {
        this.factory = factory;
        restoreState(statusFile);
    }

    public TailSpout(StreamParserFactory factory, File statusFile, File inputDirectory, final Pattern inputFileNamePattern) {
        this.factory = factory;
        this.statusFile = statusFile;
        scanner = new DirectoryScanner(inputDirectory, inputFileNamePattern);
    }

    public boolean setReliableMode(boolean replayFailedMessages) {
        boolean old = this.replayFailedMessages;
        this.replayFailedMessages = replayFailedMessages;
        return old;
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
        recordCurrentState(true);
    }

    @Override
    public void nextTuple() {
        if (currentInput == null) {
            currentInput = openNextInput();
        }

        // TODO need to persist current reading state somewhere
        while (currentInput != null) {
            // read a record
            long position = parser.currentOffset();
            List<Object> r = parser.nextRecord();

            // assert currentInput != null
            if (r == null) {
                // reached end of current file
                // (currentInput != null && r == null) so we enter loop at least once
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
                if (replayFailedMessages) {
                    collector.emit(r, messageId);
                    ackBuffer.put(messageId, new Message(scanner.getLiveFile(), position, r));
                    messageId++;
                } else {
                    collector.emit(r);
                }
                recordCurrentState(false);
            }
        }
        // exit only when all files have been processed completely
    }

    private FileInputStream openNextInput() {
        while (pendingReplays.size() > 0) {
            Message next = pendingReplays.poll();
            if (next.getFile().exists()) {
                return scanner.forceInput(next.getFile(), next.getOffset());
            } else {
                log.error("Replay file {} has disappeared", next.getFile());
            }
        }

        // look for a new file
        FileInputStream r = scanner.nextInput();
        parser = factory.createParser(r);
        return r;
    }

    private void recordCurrentState(boolean force) {
        long position = parser.currentOffset();

        if (force || messageId % checkPointInterval == 0) {
            try {
                // find smallest offset for each file
                Map<File, Long> offsets = Maps.newHashMap();
                for (Message m : ackBuffer.values()) {
                    Long x = offsets.get(m.getFile());
                    if (x == null) {
                        x = m.getOffset();
                        offsets.put(m.getFile(), x);
                    }
                    if (m.getOffset() < x) {
                        offsets.put(m.getFile(), x);
                    }
                }

                Long x = offsets.get(scanner.getLiveFile());
                if (x == null) {
                    offsets.put(scanner.getLiveFile(), parser.currentOffset());
                }

                Files.write(new Gson().toJson(new State(position, scanner, offsets)), statusFile, Charsets.UTF_8);
            } catch (IOException e) {
                log.error("Unable to write status to {}", statusFile);
            }
        }
    }

    private void restoreState(File statusFile) throws IOException {
        BufferedReader in = Files.newReader(statusFile, Charsets.UTF_8);
        State s = new Gson().fromJson(in, State.class);
        scanner = new DirectoryScanner(s.inputDirectory, s.filePattern);

        scanner.setOldFiles(s.oldFiles);
        for (File file : s.offsets.keySet()) {
            pendingReplays.add(new Message(file, s.offsets.get(file), null));
        }
    }

    public static class State {
        private long position;
        private final File liveFile;
        private final Set<File> oldFiles;
        private final File inputDirectory;
        private final Pattern filePattern;
        private Map<File, Long> offsets;

        public State(long position, DirectoryScanner scanner, Map<File, Long> offsets) {
            this.position = position;
            this.liveFile = scanner.getLiveFile();
            this.oldFiles = scanner.getOldFiles();
            this.inputDirectory = scanner.getInputDirectory();
            this.filePattern = scanner.getFileNamePattern();
            this.offsets = offsets;
        }
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
            final Message message = ackBuffer.get(messageId);
            if (message != null) {
                collector.emit(message.getTuple(), messageId);
            } else {
                log.error("Incorrect message id {}", messageId);
            }
        } else {
            log.error("Incorrect message id {}", messageId);
        }
    }
}
