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

package com.mapr;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

import com.mapr.storm.streamparser.StreamParserFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;

/**
 * Push messages into a Storm topology
 */
public class MessageSpout extends BaseRichSpout {
    private final Logger log = LoggerFactory.getLogger(TailSpout.class);
    private StreamParserFactory factory;
    private SpoutOutputCollector collector;

    private Deque<DirectoryObserver> queue = new LinkedList<DirectoryObserver>();
    private int messagesFromCurrentHead = 0;


    private int messageLimit = 100;
    private boolean reliableMode = true;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(factory.getOutputFields()));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        if (queue.size() == 0) {
            return;
        }

        int observersChecked = 0;
        while (observersChecked < queue.size()) {
            if (messagesFromCurrentHead > messageLimit) {
                rotateQueue();
            }
            DirectoryObserver head = queue.peekFirst();
            DirectoryObserver.Message r = head.nextMessage();
            messagesFromCurrentHead++;

            if (r != null) {
                if (reliableMode) {
                    collector.emit(r.getTuple(), r.getMessageId());
                } else {
                    collector.emit(r.getTuple());
                }
            } else {
                rotateQueue();
            }
        }
    }

    private void rotateQueue() {
        DirectoryObserver oldHead = queue.pollFirst();
        queue.addLast(oldHead);
        messagesFromCurrentHead = 0;
    }

    @Override
    public void close() {
        while (queue.size() > 0) {
            DirectoryObserver observer = queue.poll();
            observer.close();
        }
    }
}
