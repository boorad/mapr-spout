package com.mapr.storm;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

import com.mapr.storm.StreamParser;
import com.mapr.storm.StreamParserFactory;

public class TestStreamParserFactory implements StreamParserFactory {

	private static final long serialVersionUID = 1064671716862931399L;

	public StreamParser createParser(FileInputStream in) {
		return new TestStreamParser(in);
	}

	public List<String> getOutputFields() {
		ArrayList<String> ret = new ArrayList<String>();
		ret.add("content");
		return ret;
	}

}
