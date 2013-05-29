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

package com.mapr.storm;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;
import com.mapr.storm.streamparser.StreamParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;

/**
* Store and restore the spout state.
*/
public class SpoutState {
    private static final Logger log = LoggerFactory.getLogger(SpoutState.class);

    private Set<File> oldFiles;
    private String inputDirectory;
    private String filePattern;
    private Map<File, Long> offsets;

    public SpoutState() {
    }

    public Pattern getFilePattern() {
        return Pattern.compile(filePattern);
    }

    public String getInputDirectory() {
        return inputDirectory;
    }

    public Map<File, Long> getOffsets() {
        return offsets;
    }

    public Set<File> getOldFiles() {
        return oldFiles;
    }

    public static void recordCurrentState(Map<Long, PendingMessage> ackBuffer, DirectoryScanner scanner, StreamParser parser, File statusFile) {
        try {
            // find smallest offset for each file
            Map<File, Long> offsets = Maps.newHashMap();
            for (PendingMessage m : ackBuffer.values()) {
                Long x = offsets.get(m.getFile());
                if (x == null) {
                    x = m.getOffset();
                    offsets.put(m.getFile(), x);
                }
                if (m.getOffset() < x) {
                    offsets.put(m.getFile(), x);
                }
            }

            Long x = offsets.get(scanner.getLiveFile());
            if (x == null) {
                offsets.put(scanner.getLiveFile(), parser.currentOffset());
            }

            final String jsonState = new Gson().toJson(new SpoutState(scanner, offsets));
            final File newState = new File(statusFile.getParentFile(), String.format("%s-%06x", statusFile.getName(), new Random().nextInt()));
            Files.write(jsonState, newState, Charsets.UTF_8);
            Files.move(newState, statusFile);
        } catch (IOException e) {
            log.error(String.format("Unable to write status to %s", statusFile), e);
        }
    }

    public static DirectoryScanner restoreState(Queue<PendingMessage> pendingReplays, File statusFile) throws IOException {
        Preconditions.checkState(statusFile.exists(), "Status file not found %s", statusFile);
        SpoutState s = SpoutState.fromString(Files.toString(statusFile, Charsets.UTF_8));
        DirectoryScanner scanner = new DirectoryScanner(new File(s.inputDirectory), Pattern.compile(s.filePattern));

        // we always reset all of the old replays.  Even in reliable = false cases,
        // there will be one of these entries for the live file.
        scanner.setOldFiles(s.oldFiles);
        for (File file : s.offsets.keySet()) {
            pendingReplays.add(new PendingMessage(file, s.offsets.get(file), null));
        }
        return scanner;
    }

    public static SpoutState fromString(String serialized) {
        GsonBuilder gson = new GsonBuilder();
        gson.registerTypeAdapter(SpoutState.class, new SpoutStateAdapter());
        return gson.create().fromJson(serialized, SpoutState.class);
    }

    private SpoutState(DirectoryScanner scanner, Map<File, Long> offsets) {
        this.oldFiles = scanner.getOldFiles();
        this.inputDirectory = scanner.getInputDirectory().toString();
        this.filePattern = scanner.getFileNamePattern().toString();
        this.offsets = offsets;
    }

    private static class SpoutStateAdapter implements JsonDeserializer<SpoutState> {

        @Override
        public SpoutState deserialize(JsonElement json, Type type, JsonDeserializationContext context) throws JsonParseException {
            SpoutState r = new SpoutState();
            final JsonObject object = json.getAsJsonObject();
            r.oldFiles = context.deserialize(object.get("oldFiles"), new TypeToken<Set<File>>() {}.getType());
            r.offsets = Maps.newHashMap();
            for (Map.Entry<String, JsonElement> entry: object.get("offsets").getAsJsonObject().entrySet()) {
                r.offsets.put(new File(entry.getKey()), entry.getValue().getAsLong());
            }
            r.inputDirectory = object.get("inputDirectory").getAsString();
            r.filePattern = object.get("filePattern").getAsString();
            return r;
        }
    }
}
