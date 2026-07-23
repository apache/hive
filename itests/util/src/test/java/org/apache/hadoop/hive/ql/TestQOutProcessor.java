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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.QTestMiniClusters.FsType;
import org.apache.hadoop.hive.ql.qoption.QTestReplaceHandler;
import org.apache.hadoop.hive.common.io.QTestFetchConverter;
import org.apache.hadoop.hive.common.io.SortPrintStream;
import org.apache.hive.beeline.ConvertedOutputFile;
import org.apache.hive.beeline.ConvertedOutputFile.Converter;
import org.apache.hive.beeline.OutputFile;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link QOutProcessor} in-memory masking APIs and qtest capture streams.
 */
public class TestQOutProcessor {
  QOutProcessor qOutProcessor = new QOutProcessor(FsType.LOCAL, new QTestReplaceHandler());

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  /**
   * Query fetch rows (-- SORT_QUERY_RESULTS) are one line per println; identity transform passes through.
   */
  @Test
  public void testSingleLineQueryRowPassThrough() throws Exception {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    QTestFetchConverter converter = new QTestFetchConverter(buf, true, "UTF-8", line -> line);
    converter.println("0\tabc");
    Assert.assertEquals("0\tabc\n", buf.toString(StandardCharsets.UTF_8));
  }

  /**
   * Transform returning null suppresses the line (BeeLine "Reading log file:" and mask folding).
   */
  @Test
  public void testNullTransformSuppressesLine() throws Exception {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    QTestFetchConverter converter = new QTestFetchConverter(buf, true, "UTF-8",
        line -> line.startsWith("Reading log file:") ? null : line);
    converter.println("Reading log file: /tmp/op.log");
    converter.println("PREHOOK: query: SELECT 1");
    Assert.assertEquals("PREHOOK: query: SELECT 1\n", buf.toString(StandardCharsets.UTF_8));
  }

  /**
   * Consecutive full-mask lines fold to one via {@link QOutProcessor#maskLines(List)}.
   */
  @Test
  public void testConsecutiveMaskPatternFoldsInMemory() {
    List<String> input = Arrays.asList(
        "line before",
        QOutProcessor.MASK_PATTERN,
        QOutProcessor.MASK_PATTERN,
        QOutProcessor.MASK_PATTERN,
        "line after");
    Assert.assertEquals(
        Arrays.asList("line before", QOutProcessor.MASK_PATTERN, "line after"),
        qOutProcessor.maskLines(input));
  }

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
   * collapsed to a single line by maskLines().
   */
  @Test
  public void testConsecutiveVertexKilledLinesDeduplicatedInMemory() {
    List<String> input = Arrays.asList(
        "line before",
        QOutProcessor.MASKED_VERTEX_KILLED_PATTERN,
        QOutProcessor.MASKED_VERTEX_KILLED_PATTERN,
        QOutProcessor.MASKED_VERTEX_KILLED_PATTERN,
        "line after");
    Assert.assertEquals(
        Arrays.asList("line before", QOutProcessor.MASKED_VERTEX_KILLED_PATTERN, "line after"),
        qOutProcessor.maskLines(input));
  }

  /**
   * Two separate (non-consecutive) vertex-killed blocks must each produce one line.
   */
  @Test
  public void testNonConsecutiveVertexKilledLinesKeptSeparatelyInMemory() {
    List<String> input = Arrays.asList(
        QOutProcessor.MASKED_VERTEX_KILLED_PATTERN,
        QOutProcessor.MASKED_VERTEX_KILLED_PATTERN,
        "some other line",
        QOutProcessor.MASKED_VERTEX_KILLED_PATTERN,
        QOutProcessor.MASKED_VERTEX_KILLED_PATTERN);
    Assert.assertEquals(
        Arrays.asList(
            QOutProcessor.MASKED_VERTEX_KILLED_PATTERN,
            "some other line",
            QOutProcessor.MASKED_VERTEX_KILLED_PATTERN),
        qOutProcessor.maskLines(input));
  }

  /**
   * Vertex-killed deduplication must reset when a normal (masked) line interrupts
   * the run of vertex-killed lines.
   */
  @Test
  public void testVertexKilledRunResetByMaskedLineInMemory() {
    List<String> input = Arrays.asList(
        QOutProcessor.MASKED_VERTEX_KILLED_PATTERN,
        "Deleted /tmp/something",
        QOutProcessor.MASKED_VERTEX_KILLED_PATTERN);
    Assert.assertEquals(
        Arrays.asList(
            QOutProcessor.MASKED_VERTEX_KILLED_PATTERN,
            QOutProcessor.MASK_PATTERN,
            QOutProcessor.MASKED_VERTEX_KILLED_PATTERN),
        qOutProcessor.maskLines(input));
  }

  @Test
  public void testMaskContentFoldsConsecutiveVertexKilledLines() throws Exception {
    String input = String.join("\n",
        QOutProcessor.MASKED_VERTEX_KILLED_PATTERN,
        QOutProcessor.MASKED_VERTEX_KILLED_PATTERN,
        "line after") + "\n";
    Assert.assertEquals(
        QOutProcessor.MASKED_VERTEX_KILLED_PATTERN + "\nline after\n",
        qOutProcessor.maskContent(input));
  }

  /**
   * PREHOOK/POSTHOOK emit multiline query text in one println; stream masking must match
   * {@link QOutProcessor#maskContent(String)} (including consecutive-line folding).
   */
  @Test
  public void testMultilinePrehookStreamMaskMatchesMaskContent() throws Exception {
    String multiline =
        "PREHOOK: query: CREATE TABLE alter_table_location2 (id int, name string, dept string)\n"
            + "  PARTITIONED BY (year int)\n"
            + "  STORED AS ORC\n"
            + "  LOCATION 'pfile://${system:test.tmp.dir}/alter_table_location2'\n"
            + "  TBLPROPERTIES (\"transactional\"=\"true\")";
    String expected = qOutProcessor.maskContent(multiline + "\n");

    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    QOutProcessor.MaskingFoldState state = new QOutProcessor.MaskingFoldState();
    QTestFetchConverter converter = new QTestFetchConverter(buf, true, "UTF-8",
        line -> qOutProcessor.maskAndFoldLine(line, state));
    converter.println(multiline);

    Assert.assertEquals(expected, buf.toString(StandardCharsets.UTF_8));
  }

  @Test
  public void testBeeLineRecordMaskingAppliedWhenProcessorProvided() throws Exception {
    File out = tmpFolder.newFile("test.out");
    OutputFile inner = new OutputFile(out.getAbsolutePath());
    QOutProcessor.MaskingFoldState state = new QOutProcessor.MaskingFoldState();

    ConvertedOutputFile record =
        new ConvertedOutputFile(inner, Converter.NONE, qOutProcessor, state);
    record.println("hdfs://localhost:51594/tmp/other text");
    record.close();

    String content = Files.readString(out.toPath(), StandardCharsets.UTF_8);
    Assert.assertTrue(content.contains(QOutProcessor.HDFS_MASK));
    Assert.assertFalse(content.contains("localhost:51594"));
  }

  /**
   * BeeLine fetches HS2 operation logs in test mode; the "Reading log file:" header must be dropped
   * (same as {@link org.apache.hive.beeline.QFile#getStaticFilterSet()}).
   */
  @Test
  public void testBeeLineRecordReadingLogFileLineIsSuppressed() throws Exception {
    File out = tmpFolder.newFile("test.out");
    OutputFile inner = new OutputFile(out.getAbsolutePath());
    QOutProcessor.MaskingFoldState state = new QOutProcessor.MaskingFoldState();
    ConvertedOutputFile record =
        new ConvertedOutputFile(inner, Converter.NONE, qOutProcessor, state);

    record.println("Reading log file: /tmp/cyanzheng/operation_logs/query.test");
    record.println("PREHOOK: query: SELECT 1");
    record.close();

    Assert.assertEquals("PREHOOK: query: SELECT 1\n",
        Files.readString(out.toPath(), StandardCharsets.UTF_8));
  }

  @Test
  public void testBeeLineRecordNoMaskingWithoutProcessor() throws Exception {
    File out = tmpFolder.newFile("test.out");
    OutputFile inner = new OutputFile(out.getAbsolutePath());

    ConvertedOutputFile record = new ConvertedOutputFile(inner, Converter.NONE);
    record.println("hdfs://localhost:51594/tmp/other text");
    record.close();

    Assert.assertEquals("hdfs://localhost:51594/tmp/other text\n",
        Files.readString(out.toPath(), StandardCharsets.UTF_8));
  }

  /**
   * BeeLine PREHOOK/POSTHOOK emit multiline query text in one println; !record output must match
   * {@link QOutProcessor#maskContent(String)} via {@link ConvertedOutputFile}.
   */
  @Test
  public void testMultilinePrehookBeeLineRecordMaskMatchesMaskContent() throws Exception {
    String multiline =
        "PREHOOK: query: CREATE TABLE alter_table_location2 (id int, name string, dept string)\n"
            + "  PARTITIONED BY (year int)\n"
            + "  STORED AS ORC\n"
            + "  LOCATION 'pfile://${system:test.tmp.dir}/alter_table_location2'\n"
            + "  TBLPROPERTIES (\"transactional\"=\"true\")";
    String expected = qOutProcessor.maskContent(multiline + "\n");

    File out = tmpFolder.newFile("test.out");
    OutputFile inner = new OutputFile(out.getAbsolutePath());
    QOutProcessor.MaskingFoldState state = new QOutProcessor.MaskingFoldState();
    ConvertedOutputFile record =
        new ConvertedOutputFile(inner, Converter.NONE, qOutProcessor, state);

    record.println(multiline);
    record.close();

    Assert.assertEquals(expected, Files.readString(out.toPath(), StandardCharsets.UTF_8));
  }

  /**
   * With -- SORT_QUERY_RESULTS, sort unmasked rows first then apply masking on output so identical
   * mask placeholders are not clustered by the sorter (same stream order as ConvertedOutputFile).
   */
  @Test
  public void testSortQueryResultsMasksAfterSort() throws Exception {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    QOutProcessor.MaskingFoldState state = new QOutProcessor.MaskingFoldState();
    QTestFetchConverter mask = new QTestFetchConverter(buf, false, "UTF-8",
        line -> qOutProcessor.maskAndFoldLine(line, state));
    SortPrintStream sort = new SortPrintStream(mask, "UTF-8");

    sort.fetchStarted();
    sort.foundQuery(true);
    sort.println("b\tcol\thdfs://host-b");
    sort.println("a\tcol\thdfs://host-a");
    sort.fetchFinished();

    String output = buf.toString(StandardCharsets.UTF_8);
    Assert.assertFalse(output.contains("host-a"));
    Assert.assertFalse(output.contains("host-b"));
    Assert.assertTrue(output.contains(QOutProcessor.HDFS_MASK));
    Assert.assertEquals(
        "a\tcol\thdfs://" + QOutProcessor.HDFS_MASK + "\n"
            + "b\tcol\thdfs://" + QOutProcessor.HDFS_MASK + "\n",
        output);
  }


  @Test
  public void testCliDriverMaskBeforeSort() throws Exception {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    SortPrintStream sort = new SortPrintStream(buf, "UTF-8");
    QOutProcessor.MaskingFoldState state = new QOutProcessor.MaskingFoldState();
    QTestFetchConverter mask = new QTestFetchConverter(sort, false, "UTF-8",
        line -> qOutProcessor.maskAndFoldLine(line, state));

    mask.fetchStarted();
    mask.foundQuery(true);
    mask.println("b\tcol\thdfs://host-b");
    mask.println("a\tcol\thdfs://host-a");
    mask.fetchFinished();

    String output = buf.toString(StandardCharsets.UTF_8);
    Assert.assertFalse(output.contains("host-a"));
    Assert.assertFalse(output.contains("host-b"));
    Assert.assertEquals(
        "a\tcol\thdfs://" + QOutProcessor.HDFS_MASK + "\n"
            + "b\tcol\thdfs://" + QOutProcessor.HDFS_MASK + "\n",
        output);
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

}
