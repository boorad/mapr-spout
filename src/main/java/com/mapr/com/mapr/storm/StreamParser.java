package com.mapr.com.mapr.storm;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.io.InputStream;
import java.util.List;

/**
 * Used to parse a stream when tailing a file.
 */
public abstract class StreamParser<T extends InputStream> {
    public abstract List<Object> nextRecord(T input);

    public abstract void declareOutputFields(OutputFieldsDeclarer declarer);
}
