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

package org.apache.hadoop.hive.ql.io;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that multi-byte {@code line.delim} round-trips correctly
 * after the fix to {@link HiveIgnoreKeyTextOutputFormat} (write) and
 * the {@code textinputformat.record.delimiter} propagation in
 * {@link HiveInputFormat} (read), including field values that contain
 * bare {@code \n} bytes.
 */
public class TestMultiCharLineDelimTextFormat {

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  private static final String LINE_DELIM = "\n";
  private static final String FIELD_DELIM = "@_@";

  @Test
  public void testWriteThenReadMultiByteLineDelim() throws Exception {
    // --- Write ---
    Properties tblProps = new Properties();
    tblProps.setProperty(serdeConstants.LINE_DELIM, LINE_DELIM);
    tblProps.setProperty(serdeConstants.FIELD_DELIM, FIELD_DELIM);

    JobConf job = new JobConf();
    File outDir = tmpFolder.newFolder("write_test");
    Path outPath = new Path(outDir.getAbsolutePath(), "000000_0");

    HiveIgnoreKeyTextOutputFormat<?, ?> outputFormat =
        new HiveIgnoreKeyTextOutputFormat<>();
    RecordWriter writer = outputFormat.getHiveRecordWriter(
        job, outPath, Text.class, false, tblProps, Reporter.NULL);

    // Rows with \n inside field values.  Field delimiter is already
    // applied here; in real usage the SerDe does that.
    String[] rows = {
        "1001" + FIELD_DELIM + "gaoqiang" + FIELD_DELIM + "28",
        "1002" + FIELD_DELIM + "an\nxin" + FIELD_DELIM + "27",
        "1003" + FIELD_DELIM + "mengyu" + FIELD_DELIM + "25"
    };
    for (String row : rows) {
      writer.write(new Text(row));
    }
    writer.close(false);

    // Verify raw file content: each record ends with \002\n
    byte[] fileBytes = Files.readAllBytes(outDir.toPath().resolve("000000_0"));
    String fileContent = new String(fileBytes, StandardCharsets.UTF_8);
    for (String row : rows) {
      Assert.assertTrue("File should contain row + \\002\\n",
          fileContent.contains(row + LINE_DELIM));
    }

    // --- Read ---
    // Simulate what HiveInputFormat.getRecordReader does after the fix:
    // read line.delim from table properties, then set
    // textinputformat.record.delimiter on the JobConf.
    JobConf readJob = new JobConf();
    readJob.set("textinputformat.record.delimiter", LINE_DELIM);

    FileInputFormat.setInputPaths(readJob, outPath);

    TextInputFormat inputFormat = new TextInputFormat();
    inputFormat.configure(readJob);

    InputSplit[] splits = inputFormat.getSplits(readJob, 1);
    Assert.assertEquals(1, splits.length);

    RecordReader<LongWritable, Text> reader =
        inputFormat.getRecordReader(splits[0], readJob, Reporter.NULL);

    List<String> records = new ArrayList<>();
    LongWritable key = reader.createKey();
    Text value = reader.createValue();
    while (reader.next(key, value)) {
      records.add(value.toString());
    }
    reader.close();

    Assert.assertEquals("Should read exactly 3 records", 3, records.size());
    Assert.assertEquals(rows[0], records.get(0));
    Assert.assertEquals(rows[1], records.get(1));
    Assert.assertEquals(rows[2], records.get(2));
  }

  /**
   * Verify that when line.delim is not set, the default \n splitting
   * behaviour is preserved on both write and read paths.
   */
  @Test
  public void testDefaultLineDelim() throws Exception {
    File outDir = tmpFolder.newFolder("default_delim_test");
    Path outPath = new Path(outDir.getAbsolutePath(), "000000_0");

    Properties tblProps = new Properties();
    // No line.delim — defaults to \n
    JobConf job = new JobConf();
    HiveIgnoreKeyTextOutputFormat<?, ?> outputFormat =
        new HiveIgnoreKeyTextOutputFormat<>();
    RecordWriter writer = outputFormat.getHiveRecordWriter(
        job, outPath, Text.class, false, tblProps, Reporter.NULL);
    writer.write(new Text("hello"));
    writer.write(new Text("world"));
    writer.close(false);

    JobConf readJob = new JobConf();
    FileInputFormat.setInputPaths(readJob, outPath);
    TextInputFormat inputFormat = new TextInputFormat();
    inputFormat.configure(readJob);
    InputSplit[] splits = inputFormat.getSplits(readJob, 1);
    RecordReader<LongWritable, Text> reader =
        inputFormat.getRecordReader(splits[0], readJob, Reporter.NULL);

    List<String> records = new ArrayList<>();
    LongWritable key = reader.createKey();
    Text value = reader.createValue();
    while (reader.next(key, value)) {
      records.add(value.toString());
    }
    reader.close();

    Assert.assertEquals(2, records.size());
    Assert.assertEquals("hello", records.get(0));
    Assert.assertEquals("world", records.get(1));
  }
}
