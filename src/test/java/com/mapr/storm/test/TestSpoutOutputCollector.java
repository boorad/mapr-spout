/*
 * Copyright MapR Technologies, $year
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mapr.storm.test;

import java.util.List;

import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;

public class TestSpoutOutputCollector extends SpoutOutputCollector {

	String streamId = "";
	List<Object> tuple = null;
	Object messageId = null;
	
	public TestSpoutOutputCollector() {
		this(null);
	}
	
	public TestSpoutOutputCollector(ISpoutOutputCollector delegate) {
		super(delegate);
		tuple = null;
	}

	@Override
	public List<Integer> emit(String streamId, List<Object> tuple,
			Object messageId) {
		this.streamId = streamId;
		this.tuple = tuple;
		this.messageId = messageId;
		return null;
	}

	@Override
	public void emitDirect(int taskId, String streamId, List<Object> tuple,
			Object messageId) {
		emit(streamId, tuple, messageId);

	}

	@Override
	public void reportError(Throwable error) {		
		this.tuple = null;
	}

	public String getStreamId() {
		return streamId;
	}

	public List<Object> getTuple() {
		return tuple;
	}

	public Object getMessageId() {
		return messageId;
	}

}
