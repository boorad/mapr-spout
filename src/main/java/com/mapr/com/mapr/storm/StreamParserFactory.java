package com.mapr.com.mapr.storm;

import java.io.FileInputStream;

/**
 * Constructs a stream parser.
 */
public interface StreamParserFactory {
    public StreamParser createParser(FileInputStream in);
}
