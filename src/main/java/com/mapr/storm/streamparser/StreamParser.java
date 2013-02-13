package com.mapr.storm.streamparser;

import java.util.List;

/**
 * Used to parse a stream when tailing a file.
 * 
 * A StreamParser must return a null when it cannot read a full record from the
 * file. It should arrange to be able to finish reading a partial record if the
 * file being read eventually contains more data. One way to do this is to use
 * mark() and reset(), another is to retain parser state and continue parsing on
 * the next call. A parser must also guarantee that the offset returned by
 * currentOffset() before getting the next record can be used to position a
 * FileInputStream so that same record will be read again.
 * 
 * It is also assumed that StreamParsers will be given the file in the form of a
 * FileInputStream to parse at construction time via a call to
 * StreamParserFactory.createParser.
 */
public abstract class StreamParser {
	public abstract long currentOffset();

	public abstract List<Object> nextRecord();
}
