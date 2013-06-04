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

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.regex.Pattern;

import backtype.storm.tuple.Fields;

import com.google.protobuf.ByteString;
import com.mapr.franz.catcher.wire.MessageQueue;
import com.mapr.storm.streamparser.StreamParser;
import com.mapr.storm.streamparser.StreamParserFactory;

/**
 * Tails messages from a file with a protobuf based envelope format.
 */
public class ProtoSpout extends TailSpout {
    private static final long serialVersionUID = 4645868026918866502L;

    public ProtoSpout(TupleParser parser, File statusFile, File inputDirectory, Pattern inputFileNamePattern) throws IOException {
        super(new MessageParserFactory(parser), statusFile, inputDirectory, inputFileNamePattern);
    }

    public static abstract class TupleParser {
        public abstract List<Object> parse(ByteString buffer);

        public abstract Fields getOutputFields();
    }

    public static class MessageParserFactory implements StreamParserFactory {
        private static final long serialVersionUID = 3811432737101662002L;
        private TupleParser parser;

        public MessageParserFactory(TupleParser parser) {
            this.parser = parser;
        }

        @Override
        public StreamParser createParser(FileInputStream in) {
            return new MessageParser(in);
        }

        @Override
        public Fields getOutputFields() {
            return parser.getOutputFields();
        }

        protected class MessageParser extends StreamParser {
            private DataInputStream in;
            private final FileChannel channel;

            public MessageParser(FileInputStream in) {
                this.in = new DataInputStream(in);
                this.channel = in.getChannel();
            }

            @Override
            public long currentOffset() throws IOException {
                return channel.position();
            }

            @Override
            public List<Object> nextRecord() throws IOException {
                long start = channel.position();
                try {
                    if (in.available() >= 4) {
                        int n = in.readInt();
                        byte[] buf = new byte[n];
                        in.readFully(buf);
                        MessageQueue.Message m = MessageQueue.Message.parseFrom(buf);
                        return parser.parse(m.getPayload());
                    } else {
                        return null;
                    }
                } catch (EOFException e) {
                    channel.position(start);
                    return null;
                }
            }
        }
    }
}
