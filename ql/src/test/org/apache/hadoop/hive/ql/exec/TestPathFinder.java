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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestPathFinder {

  @TempDir
  private static Path binDir1;
  @TempDir
  private static Path binDir2;

  @BeforeAll
  static void setupDirectory() throws IOException {
    // Add some files mimicking common Linux processes
    Files.createFile(binDir1.resolve("cat"));
    Files.createFile(binDir1.resolve("wc"));
    Files.createFile(binDir1.resolve("rm"));
    Files.createFile(binDir2.resolve("ls"));
    Files.createFile(binDir2.resolve("cat"));
    // sort appears under both dirs but in one case it's a directory so
    // it should never be returned
    Files.createFile(binDir2.resolve("sort"));
    Files.createDirectory(binDir1.resolve("sort"));
  }

  @Test
  void testGetAbsolutePathWithDirPath() {
    ScriptOperator.PathFinder finder = new ScriptOperator.PathFinder(binDir1.toString());
    File found = finder.getAbsolutePath("cat");
    File actual = binDir1.resolve("cat").toFile();
    assertEquals(actual, found);
  }

  @Test
  void testGetAbsolutePathWithMultipleDirs1() {
    ScriptOperator.PathFinder finder = new ScriptOperator.PathFinder(binDir1 + File.pathSeparator + binDir2);
    File found = finder.getAbsolutePath("cat");
    File actual = binDir1.resolve("cat").toFile();
    assertEquals(actual, found);
  }

  @Test
  void testGetAbsolutePathWithMultipleDirs2() {
    ScriptOperator.PathFinder finder = new ScriptOperator.PathFinder(binDir2 + File.pathSeparator + binDir1);
    File found = finder.getAbsolutePath("cat");
    File actual = binDir2.resolve("cat").toFile();
    assertEquals(actual, found);
  }

  @Test
  void testGetAbsolutePathWithMultipleDirs3() {
    ScriptOperator.PathFinder finder = new ScriptOperator.PathFinder(binDir1 + File.pathSeparator + binDir2);
    File found = finder.getAbsolutePath("sort");
    File actual = binDir2.resolve("sort").toFile();
    assertEquals(actual, found);
  }

  @Test
  void testGetAbsolutePathWithMultipleDirs4() {
    ScriptOperator.PathFinder finder = new ScriptOperator.PathFinder(binDir1 + File.pathSeparator + binDir2);
    assertNull(finder.getAbsolutePath("missing"));
  }

  /**
   * Tests file paths pointing to executables (e.g., rm) are not accidentally used when resolving unrelated commands.
   * This is the main regression test replicating the problem in HIVE-30258.
   */
  @Test
  void testGetAbsolutePathWithDirAndFilePaths() {
    ScriptOperator.PathFinder finder =
        new ScriptOperator.PathFinder(binDir1.resolve("rm") + File.pathSeparator + binDir1);
    File found = finder.getAbsolutePath("cat");
    File actual = binDir1.resolve("cat").toFile();
    assertEquals(actual, found);
  }

  /**
   * Tests empty directories are skipped and never used for path resolution.
   */
  @Test
  void testEntriesWithPathSeparators() {
    ScriptOperator.PathFinder finder = new ScriptOperator.PathFinder(File.pathSeparator.repeat(3));
    assertEquals(0, finder.entries().count());
  }

  /**
   * Tests empty directories are skipped and whitespaces are ignored.
   */
  @Test
  void testEntriesWithPathSeparatorsAndWhitespace() {
    String env = String.format(
        "\t%s %s%s \t%s \t %s \t ",
        File.pathSeparator,
        File.pathSeparator,
        File.pathSeparator,
        File.pathSeparator,
        File.pathSeparator);
    ScriptOperator.PathFinder finder = new ScriptOperator.PathFinder(env);
    assertEquals(0, finder.entries().count());
  }

  /**
   * Tests empty directories are skipped and whitespace around actual directories is trimmed.
   */
  @Test
  void testEntriesWithDirsAndWhitespace() {
    ScriptOperator.PathFinder finder = new ScriptOperator.PathFinder(
        " " + File.pathSeparator + " " + binDir1 + " " + File.pathSeparator + "\t " + binDir2 + "\t ");
    Set<String> expected = new HashSet<>();
    expected.add(binDir1.toString());
    expected.add(binDir2.toString());
    assertEquals(expected, finder.entries().collect(Collectors.toSet()));
  }

  @Test
  void testPrependToNullPath() {
    ScriptOperator.PathFinder finder = new ScriptOperator.PathFinder(null);
    finder.prependPathComponent(binDir1.toString());
    assertEquals(Collections.singletonList(binDir1.toString()), finder.entries().collect(Collectors.toList()));
    assertEquals(binDir1.resolve("cat").toFile(), finder.getAbsolutePath("cat"));
  }

  @Test
  void testGetAbsolutePathWithOnlyFilePath() {
    ScriptOperator.PathFinder finder = new ScriptOperator.PathFinder(binDir1.resolve("rm").toString());
    assertNull(finder.getAbsolutePath("cat"));
  }

  @Test
  void testPrependPathComponentTakesPrecedence() {
    ScriptOperator.PathFinder finder = new ScriptOperator.PathFinder(binDir2.toString());
    finder.prependPathComponent(binDir1.toString());
    assertEquals(binDir1.resolve("cat").toFile(), finder.getAbsolutePath("cat"));
  }

  @Test
  void testEmptyPathNames() {
    ScriptOperator.PathFinder finder = new ScriptOperator.PathFinder("");
    assertNull(finder.getAbsolutePath("cat"));
    assertEquals(0, finder.entries().count());
  }

  @Test
  void testNullPathNames() {
    ScriptOperator.PathFinder finder = new ScriptOperator.PathFinder(null);
    assertNull(finder.getAbsolutePath("cat"));
    assertEquals(0, finder.entries().count());
  }
}
