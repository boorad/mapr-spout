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
