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

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

import static org.junit.Assert.assertTrue;

/**
 * Handy stuff for tests.
 */
public class Utils {
    /**
     * Recursive delete that handles symlinks and such correctly.  Note that the standard
     * recursive implementation isn't safe because it can't the difference between symlinks
     * and directories.
     *
     * @param f File or directory to recursively delete
     * @return The starting file
     * @throws IOException If a deletion fails.
     */
    public static Path deleteRecursively(File f) throws IOException {
        return java.nio.file.Files.walkFileTree(f.toPath(), new FileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (attrs.isRegularFile()) {
                    assertTrue(file.toFile().delete());
                    return FileVisitResult.CONTINUE;
                } else if (attrs.isDirectory()) {
                    return FileVisitResult.CONTINUE;
                } else {
                    return FileVisitResult.SKIP_SUBTREE;
                }

            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                throw new UnsupportedOperationException("Default operation");
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                assertTrue(dir.toFile().delete());
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
