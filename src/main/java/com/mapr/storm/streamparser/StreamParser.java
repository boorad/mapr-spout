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

import java.io.IOException;
import java.util.List;

/**
 * Used to parse a stream when tailing a file.
 *
 * A StreamParser must return a null when it cannot read a full record from the
 * file. It should arrange to be able to finish reading a partial record if the
 * file being read eventually contains more data. One way to do this is to use
 * mark() and reset(), another is to retain parser state and continue parsing on
 * the next call. A parser must also guarantee that the offset returned by
 * currentOffset() before getting the next record can be used to position a
 * FileInputStream so that same record will be read again.
 *
 * It is also assumed that StreamParsers will be given the file in the form of a
 * FileInputStream to parse at construction time via a call to
 * StreamParserFactory.createParser.
 */
public abstract class StreamParser {
    public abstract long currentOffset() throws IOException;

    public abstract List<Object> nextRecord() throws IOException;
}
