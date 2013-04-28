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

package com.mapr.franz.server;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.protobuf.ByteString;
import com.mapr.franz.catcher.wire.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Logs topics to files that are of limited size using a protobuf envelope format.
 *
 * Below the base directory of the logger, each topic has its own directory named after the topic.
 * Each of these directories contains files whose names are the offset from the beginning of the
 * topic stream expressed in hex.  As each file reaches maximum size, the logger moves to the next
 * file.  If we find existing files on startup, the previous process who was writing to this topic
 * may still have some pending writes.  To avoid problems with this, we never open an existing
 * file and always leave a bit of a gap in stream offset when opening the next file unless we
 * know that we were the previous writer.
 */
public class ProtoLogger {

    private static final int KILO = 1000;

    // leave 100K after the end of the last file to allow an existing writer to finish their work
    private static final int EXISTING_FILE_PAD = 100 * KILO;

    // Don't let log files get more than this
    private long maxLogFile = 100 * KILO * KILO;

    private final Logger log = LoggerFactory.getLogger(ProtoLogger.class);

    private File homeDir;
    private final Map<String,FileOutputStream> mapping = Maps.newHashMap();

    public ProtoLogger(String basePath) throws FileNotFoundException {
        homeDir = new File(basePath);
        if (homeDir.mkdirs()) {
            log.warn("Created logger output directory {}", basePath);
        }
        Preconditions.checkArgument(homeDir.exists(), "Can't create logger home directory");
        Preconditions.checkState(homeDir.canWrite(), "Can't write to logger home directory");
    }

    public void write(String topic, ByteString payload) throws IOException {
        FileOutputStream out = getCurrentStream(topic);
        MessageQueue.Message.newBuilder()
                .setTime(System.currentTimeMillis())
                .setPayload(payload)
                .build()
                .writeDelimitedTo(out);
        out.flush();
    }

    private FileOutputStream getCurrentStream(String topic) throws IOException {
        Preconditions.checkArgument(topic.matches("[0-9a-zA-Z_\\-]+"), "Invalid topic name %s", topic);

        FileOutputStream currentStream = mapping.get(topic);
        if (currentStream == null) {
            return getNextStream(topic, EXISTING_FILE_PAD);
        } else {
            if (currentStream.getChannel().position() > maxLogFile) {
                currentStream.close();
                return getNextStream(topic, 0);
            } else {
                return currentStream;
            }
        }
    }

    private FileOutputStream getNextStream(String topic, int pad) throws FileNotFoundException {
        File base = new File(homeDir, topic);
        if (base.mkdirs()) {
            log.warn("Created topic directory {}", base);
        }
        Preconditions.checkState(base.exists(), "Can't create topic directory");
        Preconditions.checkState(base.canWrite(), "Can't write to topic directory");

        String[] files = base.list(new FilenameFilter() {
            Pattern logFileName = Pattern.compile("[0-9a-f]+");

            @Override
            public boolean accept(File dir, String name) {
                return logFileName.matcher(name).matches();
            }
        });

        long offset;
        if (files.length == 0) {
            offset = 0;
        } else {
            Arrays.sort(files, Ordering.<String>natural().reverse());
            offset = Long.parseLong(files[0], 16) + new File(base, files[0]).length() + pad;
        }
        FileOutputStream currentStream = new FileOutputStream(new File(base, String.format("%016x", offset)));
        mapping.put(topic, currentStream);
        return currentStream;
    }

    public void setMaxLogFile(long maxLogFile) {
        this.maxLogFile = maxLogFile;
    }
}
