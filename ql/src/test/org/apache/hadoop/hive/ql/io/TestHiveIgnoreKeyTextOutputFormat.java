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
package org.apache.hadoop.hive.ql.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for HiveIgnoreKeyTextOutputFormat header/footer emission.
 */
public class TestHiveIgnoreKeyTextOutputFormat {

  private Configuration conf;
  private JobConf job;
  private FileSystem fileSystem;
  private Path testDir;
  private Reporter reporter;

  @Before
  public void setUp() throws IOException {
    conf = new Configuration();
    job = new JobConf(conf);
    fileSystem = FileSystem.getLocal(conf);
    testDir = new Path(System.getProperty("test.tmp.dir", System.getProperty(
        "user.dir", new File(".").getAbsolutePath()))
        + "/TestHiveIgnoreKeyTextOutputFormat");
    reporter = Reporter.NULL;
    fileSystem.delete(testDir, true);
    fileSystem.mkdirs(testDir);
  }

  @After
  public void tearDown() throws IOException {
    fileSystem.delete(testDir, true);
  }

  @Test
  public void testBuildColumnNameHeaderLine() {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, "a,b");
    props.setProperty(serdeConstants.FIELD_DELIM, ",");
    assertEquals("a,b", HiveIgnoreKeyTextOutputFormat.buildColumnNameHeaderLine(props));
  }

  @Test
  public void testBuildColumnNameHeaderLineQuotesSpecialCharacters() {
    Properties props = new Properties();
    props.setProperty(serdeConstants.COLUMN_NAME_DELIMITER, "\0");
    props.setProperty(serdeConstants.LIST_COLUMNS, "a\0col,with,comma");
    props.setProperty(serdeConstants.FIELD_DELIM, ",");
    props.setProperty(serdeConstants.QUOTE_CHAR, "\"");
    props.setProperty(serdeConstants.ESCAPE_CHAR, "\\");
    assertEquals("a,\"col,with,comma\"", HiveIgnoreKeyTextOutputFormat.buildColumnNameHeaderLine(props));
  }

  @Test
  public void testWriteHeaderAndFooterRoundTrip() throws Exception {
    Path outFile = new Path(testDir, "out.csv");
    Properties tableProperties = tablePropertiesWithSkipCounts(1, 1);
    tableProperties.setProperty(serdeConstants.LIST_COLUMNS, "a,b");
    tableProperties.setProperty(serdeConstants.FIELD_DELIM, ",");

    HiveIgnoreKeyTextOutputFormat<LongWritable, Text> outputFormat =
        new HiveIgnoreKeyTextOutputFormat<>();
    FileSinkOperator.RecordWriter writer = outputFormat.getHiveRecordWriter(
        job, outFile, Text.class, false, tableProperties, null);
    writer.write(new Text("x,y"));
    writer.write(new Text("a,b"));
    writer.write(new Text("c,d"));
    writer.close(false);

    byte[] fileBytes = readFileBytes(outFile);
    assertArrayEquals("a,b\nx,y\na,b\nc,d\n\n".getBytes(StandardCharsets.UTF_8), fileBytes);

    SkippingTextInputFormat inputFormat = new SkippingTextInputFormat();
    inputFormat.configure(job, 1, 1);
    FileInputFormat.setInputPaths(job, outFile);
    InputSplit[] splits = inputFormat.getSplits(job, 1);
    List<String> received = readAllRows(inputFormat, splits);
    assertEquals(3, received.size());
    assertEquals("x,y", received.get(0));
    assertEquals("a,b", received.get(1));
    assertEquals("c,d", received.get(2));
  }

  @Test
  public void testMultipleHeaderLinesRoundTrip() throws Exception {
    Path outFile = new Path(testDir, "multi_header.csv");
    Properties tableProperties = tablePropertiesWithSkipCounts(3, 0);
    tableProperties.setProperty(serdeConstants.LIST_COLUMNS, "a,b");
    tableProperties.setProperty(serdeConstants.FIELD_DELIM, ",");

    HiveIgnoreKeyTextOutputFormat<LongWritable, Text> outputFormat =
        new HiveIgnoreKeyTextOutputFormat<>();
    FileSinkOperator.RecordWriter writer = outputFormat.getHiveRecordWriter(
        job, outFile, Text.class, false, tableProperties, null);
    writer.write(new Text("x,y"));
    writer.write(new Text("a,b"));
    writer.write(new Text("c,d"));
    writer.close(false);

    SkippingTextInputFormat inputFormat = new SkippingTextInputFormat();
    inputFormat.configure(job, 3, 0);
    FileInputFormat.setInputPaths(job, outFile);
    InputSplit[] splits = inputFormat.getSplits(job, 1);
    List<String> received = readAllRows(inputFormat, splits);
    assertEquals(3, received.size());
    assertEquals("x,y", received.get(0));
    assertEquals("a,b", received.get(1));
    assertEquals("c,d", received.get(2));
  }

  @Test
  public void testFooterOnlyRoundTrip() throws Exception {
    Path outFile = new Path(testDir, "footer_only.csv");
    Properties tableProperties = tablePropertiesWithSkipCounts(0, 2);
    tableProperties.setProperty(serdeConstants.LIST_COLUMNS, "a,b");
    tableProperties.setProperty(serdeConstants.FIELD_DELIM, ",");

    HiveIgnoreKeyTextOutputFormat<LongWritable, Text> outputFormat =
        new HiveIgnoreKeyTextOutputFormat<>();
    FileSinkOperator.RecordWriter writer = outputFormat.getHiveRecordWriter(
        job, outFile, Text.class, false, tableProperties, null);
    writer.write(new Text("x,y"));
    writer.write(new Text("a,b"));
    writer.write(new Text("c,d"));
    writer.close(false);

    SkippingTextInputFormat inputFormat = new SkippingTextInputFormat();
    inputFormat.configure(job, 0, 2);
    FileInputFormat.setInputPaths(job, outFile);
    InputSplit[] splits = inputFormat.getSplits(job, 1);
    List<String> received = readAllRows(inputFormat, splits);
    assertEquals(3, received.size());
    assertEquals("x,y", received.get(0));
    assertEquals("a,b", received.get(1));
    assertEquals("c,d", received.get(2));
  }

  @Test
  public void testNoHeaderFooterWhenCountsAreZero() throws Exception {
    Path outFile = new Path(testDir, "plain.csv");
    Properties tableProperties = tablePropertiesWithSkipCounts(0, 0);
    tableProperties.setProperty(serdeConstants.LIST_COLUMNS, "a,b");
    tableProperties.setProperty(serdeConstants.FIELD_DELIM, ",");

    HiveIgnoreKeyTextOutputFormat<LongWritable, Text> outputFormat =
        new HiveIgnoreKeyTextOutputFormat<>();
    FileSinkOperator.RecordWriter writer = outputFormat.getHiveRecordWriter(
        job, outFile, Text.class, false, tableProperties, null);
    writer.write(new Text("x,y"));
    writer.close(false);

    byte[] fileBytes = readFileBytes(outFile);
    assertArrayEquals("x,y\n".getBytes(StandardCharsets.UTF_8), fileBytes);
  }

  private static Properties tablePropertiesWithSkipCounts(int headerCount, int footerCount) {
    Properties props = new Properties();
    props.setProperty(serdeConstants.HEADER_COUNT, Integer.toString(headerCount));
    props.setProperty(serdeConstants.FOOTER_COUNT, Integer.toString(footerCount));
    props.setProperty(serdeConstants.LINE_DELIM, "\n");
    return props;
  }

  private byte[] readFileBytes(Path file) throws IOException {
    try (java.io.InputStream in = fileSystem.open(file)) {
      java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
      byte[] chunk = new byte[4096];
      int read;
      while ((read = in.read(chunk)) >= 0) {
        buffer.write(chunk, 0, read);
      }
      return buffer.toByteArray();
    }
  }

  private List<String> readAllRows(SkippingTextInputFormat inputFormat, InputSplit[] splits)
      throws Exception {
    List<String> received = new ArrayList<>();
    for (InputSplit split : splits) {
      RecordReader<LongWritable, Text> reader =
          inputFormat.getRecordReader(split, job, reporter);
      LongWritable key = reader.createKey();
      Text value = reader.createValue();
      while (reader.next(key, value)) {
        received.add(value.toString());
      }
      reader.close();
    }
    return received;
  }
}
