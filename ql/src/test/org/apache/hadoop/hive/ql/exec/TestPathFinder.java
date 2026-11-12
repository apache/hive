/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.exec;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestPathFinder {

  @TempDir private static Path binDir;

  @BeforeAll
  static void setupDirectory() throws IOException {
    // Add some files mimicking common Linux processes
    Files.createFile(binDir.resolve("cat"));
    Files.createFile(binDir.resolve("wc"));
    Files.createFile(binDir.resolve("rm"));
  }

  @Test
  void testGetAbsolutePathFromEnvWithoutFilePaths() {
    ScriptOperator.PathFinder finder = new ScriptOperator.PathFinder(binDir.toString());
    File found = finder.getAbsolutePath("cat");
    File actual = binDir.resolve("cat").toFile();
    assertEquals(actual, found);
  }

  @Test
  void testGetAbsolutePathFromEnvWithFilePaths() {
    ScriptOperator.PathFinder finder = new ScriptOperator.PathFinder(pathNames(binDir.resolve("rm"), binDir));
    File found = finder.getAbsolutePath("cat");
    File actual = binDir.resolve("cat").toFile();
    assertEquals(actual, found);
  }

  @Test
  void testGetAbsolutePathFromEmptyEnv() {
    ScriptOperator.PathFinder finder = new ScriptOperator.PathFinder("");
    File found = finder.getAbsolutePath("cat");
    assertNull(found);
  }

  @Test
  void testGetAbsolutePathFromEnvWithEmptyEntries() {
    ScriptOperator.PathFinder finder = new ScriptOperator.PathFinder("::");
    File found = finder.getAbsolutePath("cat");
    assertNull(found);
  }

  private static String pathNames(Path... paths) {
    Objects.requireNonNull(paths);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < paths.length; i++) {
      if (i > 0) {
        sb.append(File.pathSeparator);
      }
      sb.append(paths[i].toString());
    }
    return sb.toString();
  }
}
