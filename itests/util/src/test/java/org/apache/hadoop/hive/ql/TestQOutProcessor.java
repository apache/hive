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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql;

import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.QTestMiniClusters.FsType;
import org.apache.hadoop.hive.ql.qoption.QTestReplaceHandler;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * This class contains unit tests for QTestUtil
 */
public class TestQOutProcessor {
  QOutProcessor qOutProcessor = new QOutProcessor(FsType.LOCAL, new QTestReplaceHandler());

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  /**
   * A raw vertex-killed log line must be replaced with MASKED_VERTEX_KILLED_PATTERN.
   */
  @Test
  public void testVertexKilledLineIsReplaced() {
    String raw = "Vertex killed, vertexName=Map 2, "
        + "diagnostics=[Task failed, taskAttemptId=attempt_1 "
        + "[Map 2] killed/failed due to:OTHER_VERTEX_FAILURE]";
    Assert.assertEquals(QOutProcessor.MASKED_VERTEX_KILLED_PATTERN, processLine(raw));
  }

  /**
   * A line containing multiple embedded MASKED_VERTEX_KILLED_PATTERN tokens
   * (produced after the first regex pass) must be collapsed to a single token.
   */
  @Test
  public void testMultipleEmbeddedVertexKilledTokensCollapsedOnSameLine() {
    String twoTokens = QOutProcessor.MASKED_VERTEX_KILLED_PATTERN
        + QOutProcessor.MASKED_VERTEX_KILLED_PATTERN;
    Assert.assertEquals(QOutProcessor.MASKED_VERTEX_KILLED_PATTERN, processLine(twoTokens));

    String threeTokens = QOutProcessor.MASKED_VERTEX_KILLED_PATTERN
        + QOutProcessor.MASKED_VERTEX_KILLED_PATTERN
        + QOutProcessor.MASKED_VERTEX_KILLED_PATTERN;
    Assert.assertEquals(QOutProcessor.MASKED_VERTEX_KILLED_PATTERN, processLine(threeTokens));
  }

  /**
   * A single MASKED_VERTEX_KILLED_PATTERN token must be left unchanged by processLine.
   */
  @Test
  public void testSingleEmbeddedVertexKilledTokenUnchanged() {
    Assert.assertEquals(
        QOutProcessor.MASKED_VERTEX_KILLED_PATTERN,
        processLine(QOutProcessor.MASKED_VERTEX_KILLED_PATTERN));
  }

  /**
   * killedVertices:<number> must be masked regardless of the numeric value.
   */
  @Test
  public void testKilledVerticesCountIsMasked() {
    Assert.assertEquals("killedVertices:#Masked#", processLine("killedVertices:3"));
    Assert.assertEquals("killedVertices:#Masked#", processLine("killedVertices:0"));
    Assert.assertEquals("killedVertices:#Masked#", processLine("killedVertices:100"));
  }

  /**
   * killedVertices masking should work when embedded in a longer line (e.g. FAILED: summary).
   */
  @Test
  public void testKilledVerticesCountIsMaskedInLongerLine() {
    String input  = "FAILED: Execution Error, return code 2 from org.apache.hadoop.hive.ql.exec.tez."
        + "TezTask. killedVertices:2 Vertex re-run not supported in current execution mode.";
    String output = processLine(input);
    Assert.assertTrue("killedVertices:#Masked# must appear in output",
        output.contains("killedVertices:#Masked#"));
    Assert.assertFalse("raw killedVertices:2 must not appear in output",
        output.contains("killedVertices:2"));
  }

  /**
   * Multiple consecutive standalone MASKED_VERTEX_KILLED_PATTERN lines must be
   * collapsed to a single line by maskPatterns().
   */
  @Test
  public void testConsecutiveVertexKilledLinesDeduplicatedInFile() throws Exception {
    File f = tmpFile(
        "line before",
        QOutProcessor.MASKED_VERTEX_KILLED_PATTERN,
        QOutProcessor.MASKED_VERTEX_KILLED_PATTERN,
        QOutProcessor.MASKED_VERTEX_KILLED_PATTERN,
        "line after");

    qOutProcessor.maskPatterns(f.getAbsolutePath());

    List<String> lines = readLines(f);
    Assert.assertEquals(
        Arrays.asList("line before", QOutProcessor.MASKED_VERTEX_KILLED_PATTERN, "line after"),
        lines);
  }

  /**
   * Two separate (non-consecutive) vertex-killed blocks must each produce one line.
   */
  @Test
  public void testNonConsecutiveVertexKilledLinesKeptSeparately() throws Exception {
    File f = tmpFile(
        QOutProcessor.MASKED_VERTEX_KILLED_PATTERN,
        QOutProcessor.MASKED_VERTEX_KILLED_PATTERN,
        "some other line",
        QOutProcessor.MASKED_VERTEX_KILLED_PATTERN,
        QOutProcessor.MASKED_VERTEX_KILLED_PATTERN);

    qOutProcessor.maskPatterns(f.getAbsolutePath());

    List<String> lines = readLines(f);
    Assert.assertEquals(
        Arrays.asList(
            QOutProcessor.MASKED_VERTEX_KILLED_PATTERN,
            "some other line",
            QOutProcessor.MASKED_VERTEX_KILLED_PATTERN),
        lines);
  }

  /**
   * Vertex-killed deduplication must reset when a normal (masked) line interrupts
   * the run of vertex-killed lines.
   */
  @Test
  public void testVertexKilledRunResetByMaskedLine() throws Exception {
    // "Deleted something" starts with "Deleted" → gets replaced by MASK_PATTERN
    File f = tmpFile(
        QOutProcessor.MASKED_VERTEX_KILLED_PATTERN,
        "Deleted /tmp/something",        // will be masked → MASK_PATTERN
        QOutProcessor.MASKED_VERTEX_KILLED_PATTERN);

    qOutProcessor.maskPatterns(f.getAbsolutePath());

    List<String> lines = readLines(f);
    // MASK_PATTERN lines fold duplicates; but here there is only one occurrence
    Assert.assertEquals(
        Arrays.asList(
            QOutProcessor.MASKED_VERTEX_KILLED_PATTERN,
            QOutProcessor.MASK_PATTERN,
            QOutProcessor.MASKED_VERTEX_KILLED_PATTERN),
        lines);
  }

  @Test
  public void testSelectiveHdfsPatternMaskOnlyHdfsPath() {
    Assert.assertEquals("nothing to be masked", processLine("nothing to be masked"));
    Assert.assertEquals("hdfs://", processLine("hdfs://"));
    Assert.assertEquals(String.format("hdfs://%s", QOutProcessor.HDFS_MASK),
        processLine("hdfs:///"));
    Assert.assertEquals(String.format("hdfs://%s", QOutProcessor.HDFS_MASK),
        processLine("hdfs://a"));
    Assert.assertEquals(String.format("hdfs://%s other text", QOutProcessor.HDFS_MASK),
        processLine("hdfs://tmp.dfs.com:50029/tmp other text"));
    Assert.assertEquals(String.format("hdfs://%s", QOutProcessor.HDFS_MASK), processLine(
        "hdfs://localhost:51594/build/ql/test/data/warehouse/default/encrypted_table_dp/p=2014-09-23"));

    Assert.assertEquals(String.format("hdfs://%s", QOutProcessor.HDFS_MASK),
        processLine("hdfs://localhost:11111/tmp/ct_noperm_loc_foo1"));

    Assert.assertEquals(
        String.format("hdfs://%s hdfs://%s", QOutProcessor.HDFS_MASK, QOutProcessor.HDFS_MASK),
        processLine("hdfs://one hdfs://two"));

    Assert.assertEquals(
        String.format(
            "some text before [name=hdfs://%s]] some text between hdfs://%s some text after",
            QOutProcessor.HDFS_MASK, QOutProcessor.HDFS_MASK),
        processLine(
            "some text before [name=hdfs://localhost:11111/tmp/ct_noperm_loc_foo1]] some text between hdfs://localhost:22222/tmp/ct_noperm_loc_foo2 some text after"));

    Assert.assertEquals(
        String.format("-rw-r--r--   3 %s %s       %s %s hdfs://%s", QOutProcessor.HDFS_USER_MASK,
            QOutProcessor.HDFS_GROUP_MASK, QOutProcessor.HDFS_SIZE_MASK , QOutProcessor.HDFS_DATE_MASK, QOutProcessor.HDFS_MASK),
        processLine(
            "-rw-r--r--   3 hiveptest supergroup       2557 2018-01-11 17:09 hdfs://hello_hdfs_path"));

    Assert.assertEquals(
        String.format("-rw-r--r--   3 %s %s       %s %s hdfs://%s", QOutProcessor.HDFS_USER_MASK,
            QOutProcessor.HDFS_GROUP_MASK, QOutProcessor.HDFS_SIZE_MASK, QOutProcessor.HDFS_DATE_MASK, QOutProcessor.HDFS_MASK),
        processLine(
            "-rw-r--r--   3 hiveptest supergroup       2557 2018-01-11 17:09 hdfs://hello_hdfs_path"));
    // Test Hdfs Size not to be masked when its 0.
    Assert.assertEquals(
            String.format("-rw-r--r--   3 %s %s       0 %s hdfs://%s", QOutProcessor.HDFS_USER_MASK,
                    QOutProcessor.HDFS_GROUP_MASK, QOutProcessor.HDFS_DATE_MASK, QOutProcessor.HDFS_MASK),
            processLine(
                    "-rw-r--r--   3 hiveptest supergroup       0 2018-01-11 17:09 hdfs://hello_hdfs_path"));
  }

  private String processLine(String line) {
    return qOutProcessor.processLine(line).get();
  }

  private File tmpFile(String... lines) throws Exception {
    File f = tmpFolder.newFile();
    try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(f.toPath(), StandardCharsets.UTF_8))) {
      for (String l : lines) {
        pw.println(l);
      }
    }
    return f;
  }

  private List<String> readLines(File f) throws Exception {
    List<String> all = Files.readAllLines(f.toPath(), StandardCharsets.UTF_8);
    while (!all.isEmpty() && all.get(all.size() - 1).isEmpty()) {
      all.remove(all.size() - 1);
    }
    return all;
  }

}