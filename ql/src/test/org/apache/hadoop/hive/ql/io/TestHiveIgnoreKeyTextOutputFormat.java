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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 * Unittest for HiveIgnoreKeyTextOutputFormat header/footer writing.
 */
public class TestHiveIgnoreKeyTextOutputFormat {

  private Configuration conf;
  private JobConf job;
  private FileSystem fileSystem;
  private Path testDir;

  @Before
  public void setUp() throws IOException {
    conf = new Configuration();
    job = new JobConf(conf);
    fileSystem = FileSystem.getLocal(conf);
    testDir = new Path(System.getProperty("test.tmp.dir", System.getProperty(
        "user.dir", new File(".").getAbsolutePath()))
        + "/TestHiveIgnoreKeyTextOutputFormat");
    fileSystem.delete(testDir, true);
    fileSystem.mkdirs(testDir);
  }

  @After
  public void tearDown() throws IOException {
    fileSystem.delete(testDir, true);
  }

  /**
   * Test that HiveIgnoreKeyTextOutputFormat writes a header and footer so that
   * SkippingTextInputFormat does not lose data rows.
   */
  @Test
  public void testHeaderAndFooterAreWritten() throws Exception {
    Path outFile = new Path(testDir, "test.csv");

    Properties tableProperties = new Properties();
    tableProperties.setProperty(serdeConstants.LIST_COLUMNS, "a,b");
    tableProperties.setProperty(serdeConstants.FIELD_DELIM, ",");
    tableProperties.setProperty(serdeConstants.LINE_DELIM, "\n");
    tableProperties.setProperty(serdeConstants.HEADER_COUNT, "1");
    tableProperties.setProperty(serdeConstants.FOOTER_COUNT, "2");

    HiveIgnoreKeyTextOutputFormat<WritableComparable, Writable> outputFormat =
        new HiveIgnoreKeyTextOutputFormat<>();
    FileSinkOperator.RecordWriter writer = outputFormat.getHiveRecordWriter(
        job, outFile, Text.class, false, tableProperties, new Progressable() {
          @Override
          public void progress() {
          }
        });

    writer.write(new Text("x,y"));
    writer.write(new Text("a,b"));
    writer.write(new Text("c,d"));
    writer.close(false);

    // Read the file back with header and footer skipping.
    SkippingTextInputFormat inputFormat = new SkippingTextInputFormat();
    inputFormat.configure(job, 1, 2);
    FileInputFormat.setInputPaths(job, outFile);
    InputSplit[] splits = inputFormat.getSplits(job, 2);

    List<String> received = new ArrayList<>();
    for (InputSplit split : splits) {
      RecordReader<LongWritable, Text> reader =
          inputFormat.getRecordReader(split, job, Reporter.NULL);
      LongWritable key = reader.createKey();
      Text value = reader.createValue();
      while (reader.next(key, value)) {
        received.add(value.toString());
      }
      reader.close();
    }

    assertEquals(Arrays.asList("x,y", "a,b", "c,d"), received);
  }
}
