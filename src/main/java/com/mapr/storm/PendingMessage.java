package com.mapr.storm;

import java.io.File;
import java.util.List;

/**
* In reliable mode, PendingMessage objects record messages that haven't been ack'ed.
*/
public class PendingMessage {
    private File logFile;
    private long offset;
    private List<Object> tuple;

    public PendingMessage(File logFile, long offset, List<Object> tuple) {
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
