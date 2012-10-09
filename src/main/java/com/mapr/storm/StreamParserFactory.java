package com.mapr.storm;

import java.io.FileInputStream;
import java.io.Serializable;
import java.util.List;

/**
 * Constructs a stream parser.
 */
public interface StreamParserFactory extends Serializable {
    public StreamParser createParser(FileInputStream in);
    public List<String> getOutputFields();
}
