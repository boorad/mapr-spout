package com.mapr.franz.server;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;

import com.google.common.io.Files;
import com.google.protobuf.ByteString;

public class GhettoTopicLogger {

    private String basePath = "";
    private HashMap<String, File> mapping;

    public GhettoTopicLogger() {
        this("");
    }

    public GhettoTopicLogger(String basePath) {
        this.basePath = basePath;
        mapping = new HashMap<String, File>();
    }

    public void write(String topic, ByteString payload) {
        File f = getFile(topic);
        FileOutputStream fos;
        try {
            fos = new FileOutputStream(f, true); // append == true
            int len = payload.size();
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
