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

package com.mapr.franz.hazel;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.protobuf.ServiceException;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.mapr.franz.catcher.Client;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.SocketException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.nio.file.Files.*;

public class HazelCatcherTest {
    @Test
    public void testSingleServer() throws IOException, ServiceException, InterruptedException {
        String basePath = Files.createTempDir().getPath();

        final HazelCatcher.Options opts = new HazelCatcher.Options()
                .base(basePath)
                .host("localhost")
                .port(7070);

        ExecutorService bg = Executors.newSingleThreadExecutor();
        bg.submit(new Callable<Object>() {
            @Override
            public Object call() {
                try {
                    HazelCatcher.run(opts);
                    return null;
                } catch (SocketException | FileNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
        });


        Thread.sleep(7000);
        Client c = new Client(Lists.newArrayList(new PeerInfo("localhost", 7070)));

        c.sendMessage("foo-1", "hello there");
        c.close();

        Thread.sleep(7000);
        bg.shutdownNow();
        Thread.sleep(1000);

        System.out.printf("\n\n");
        walkFileTree(new File(basePath).toPath(),
                new FileVisitor<Path>() {
                    int indent = 0;

                    @Override
                    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                        System.out.printf("%s%s/\n", Strings.padStart("", indent, ' '), dir.getFileName());
                        indent += 2;
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        if (attrs.isRegularFile()) {
                            System.out.printf("%s%s(%d)\n", Strings.padStart("", indent, ' '), file.getFileName(), file.toFile().length());
                            file.toFile().delete();
                        } else {
                            // ignore
                        }
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                        throw new UnsupportedOperationException("Default operation");
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                        indent -= 2;
                        dir.toFile().delete();
                        return FileVisitResult.CONTINUE;
                    }
                });
        System.out.printf("\n\n");
    }
}
