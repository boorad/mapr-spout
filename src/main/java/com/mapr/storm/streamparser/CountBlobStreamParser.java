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

package com.mapr.storm.streamparser;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Values;


/**
 *
 *  This StreamParser is expecting a format of [size_of_record][content]
 *  that is repeated for each record.
 *
 *  - size_of_record   is a 4-byte encoded integer
 *  - content          is a (size_of_record)-long byte array
 *
 */
public class CountBlobStreamParser extends StreamParser {

    private final Logger log = LoggerFactory.getLogger(CountBlobStreamParser.class);

    private BufferedInputStream buf;
    private byte[] recordSizeBuffer = new byte[4];
    private byte[] recordBuffer;
    private int recordSize = 0;
    private long currentOffset = 0;
    private long currentBeginOffset = 0;
    private int readSize;
    private static final int RECORD_SIZE_BYTES = 4; // length of int in bytes

    public CountBlobStreamParser(FileInputStream in) {
        buf = new BufferedInputStream(in);
        recordSizeBuffer = new byte[4];
        currentOffset = 0;
        currentBeginOffset = 0;
    }

    @Override
    public long currentOffset() {
        return currentOffset;
    }

    @Override
    public List<Object> nextRecord() {
        List<Object> ret = null;

        try {
            while( buf.available() > 0 ) {
                if( currentOffset == currentBeginOffset ) {
                    // we're at beginning, so start a new record
                    startNewRecord();
                }

                // now try to read record content
                readContent();

                // enough to return record?
                if( (currentOffset - currentBeginOffset) == recordSize ) {
                    // we have exactly right amount of bytes, so return it
                    currentBeginOffset = currentOffset;
                    return new Values(recordBuffer);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return ret;
    }

    public long getCurrentBeginOffset() {
        return currentBeginOffset;
    }


    /*
     * private methods
     */

    private void startNewRecord() throws IOException {
        if( buf.read(recordSizeBuffer) == RECORD_SIZE_BYTES ) {
            recordSize = getInt(recordSizeBuffer);
            recordBuffer = new byte[recordSize];
            currentOffset += RECORD_SIZE_BYTES;
            currentBeginOffset = currentOffset;
        }
    }

    private void readContent() throws IOException {
        readSize = recordSize -
                (int)(currentOffset-currentBeginOffset);
        int read = buf.read(recordBuffer, 0, readSize);
        currentOffset += read;
    }

    // get the int value of a byte array
    private int getInt(byte[] buf) {
        ByteBuffer bb = ByteBuffer.wrap(buf);
        return bb.getInt();
    }

}
