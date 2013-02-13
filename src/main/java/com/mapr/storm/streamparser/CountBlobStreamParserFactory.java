package com.mapr.storm.streamparser;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;



public class CountBlobStreamParserFactory implements StreamParserFactory {

	private static final long serialVersionUID = 1064671716862931399L;

	public StreamParser createParser(FileInputStream in) {
		return new CountBlobStreamParser(in);
	}

	public List<String> getOutputFields() {
		ArrayList<String> ret = new ArrayList<String>();
		ret.add("content");
		return ret;
	}

}
