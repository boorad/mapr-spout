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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

import com.google.common.io.Files;
import com.google.protobuf.ByteString;

public class GhettoTopicLogger {

    private String basePath = "";
    private HashMap<String, File> mapping;

    public GhettoTopicLogger(String basePath) {
        this.basePath = basePath;
        mapping = new HashMap<String, File>();
    }

    public void write(String topic, ByteString payload) {
        File f = getFile(topic);
        FileOutputStream fos;
        try {
            fos = new FileOutputStream(f, true); // append == true
            byte[] len = ByteBuffer.allocate(4).putInt(payload.size()).array();
            fos.write(len);
            fos.write(payload.toByteArray());
            fos.close();
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    private File getFile(String topic) {
        File f;
        if( (f = mapping.get(topic)) == null ) {
            f = new File(Files.simplifyPath(basePath + "/" + topic));
            try {
                Files.createParentDirs(f);
            } catch (IOException e) {
                e.printStackTrace();
            }
            mapping.put(topic, f);
        }
        return f;
    }

}
