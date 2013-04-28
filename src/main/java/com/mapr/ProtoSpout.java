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

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.mapr.franz.catcher.wire.MessageQueue;
import com.mapr.storm.streamparser.StreamParser;
import com.mapr.storm.streamparser.StreamParserFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Tails messages from a file with a protobuf based envelope format.
 */
public class ProtoSpout extends TailSpout {
    public ProtoSpout(TupleParser parser, File statusFile, File inputDirectory, Pattern inputFileNamePattern) throws IOException {
        super(new MessageParserFactory(parser), statusFile, inputDirectory, inputFileNamePattern);
    }

    public static abstract class TupleParser {
        public abstract List<Object> parse(ByteString buffer);

        public abstract List<String> getOutputFields();
    }

    public static class MessageParserFactory implements StreamParserFactory {
        private TupleParser parser;

        public MessageParserFactory(TupleParser parser) {
            this.parser = parser;
        }

        @Override
        public StreamParser createParser(FileInputStream in) {
            return new MessageParser(in);
        }

        @Override
        public List<String> getOutputFields() {
            return parser.getOutputFields();
        }

        private class MessageParser extends StreamParser {
            private InputStream in;
            private final FileChannel channel;
            private int errors = 0;

            public MessageParser(FileInputStream in) {
                this.in = in;
                this.channel = in.getChannel();
            }

            @Override
            public long currentOffset() throws IOException {
                return channel.position();
            }

            @Override
            public List<Object> nextRecord() throws IOException {
                MessageQueue.Message m;
                while (true) {
                    long start = channel.position();
                    try {
                        m = MessageQueue.Message.parseDelimitedFrom(in);
                        errors = 0;
                        break;
                    } catch (InvalidProtocolBufferException e) {
                        errors++;
                        channel.position(start);
                        if (errors < 5) {
                            return null;
                        } else if (errors < 10) {
                            try {
                                Thread.sleep(10);
                            } catch (InterruptedException e1) {
                                // ignore
                            }
                        } else {
                            throw new IOException("Cannot read message from queue");
                        }
                    }
                }

                if (m != null) {
                    return parser.parse(m.getPayload());
                } else {
                    return null;
                }
            }
        }
    }
}
