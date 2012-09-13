package com.mapr.storm;

import java.io.FileInputStream;
import java.util.List;

/**
 * Constructs a stream parser.
 */
public interface StreamParserFactory {
    public StreamParser createParser(FileInputStream in);
    public List<String> getOutputFields();
}
