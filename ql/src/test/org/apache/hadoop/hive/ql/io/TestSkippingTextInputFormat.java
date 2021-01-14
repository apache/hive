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
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Unittest for SkippingTextInputFormat with Skip Header/Footer.
 */
public class TestSkippingTextInputFormat {

  private Configuration conf;
  private JobConf job;
  private FileSystem fileSystem;
  private Path testDir;
  Reporter reporter;

  private Path dataDir;

  private CompressionCodecFactory compressionCodecs = null;
  private CompressionCodec codec;

  @Before
  public void setUp() throws IOException {
    conf = new Configuration();
    job = new JobConf(conf);

    TableDesc tblDesc = Utilities.defaultTd;
    PartitionDesc partDesc = new PartitionDesc(tblDesc, null);
    LinkedHashMap<Path, PartitionDesc> pt = new LinkedHashMap<>();
    pt.put(new Path("/tmp/testfolder"), partDesc);
    MapredWork mrwork = new MapredWork();
    mrwork.getMapWork().setPathToPartitionInfo(pt);
    Utilities.setMapRedWork(job, mrwork,new Path("/tmp/" + System.getProperty("user.name"), "hive"));

    fileSystem = FileSystem.getLocal(conf);
    testDir = new Path(System.getProperty("test.tmp.dir", System.getProperty(
        "user.dir", new File(".").getAbsolutePath()))
        + "/TestSkippingTextInputFormat");
    reporter = Reporter.NULL;
    fileSystem.delete(testDir, true);

    dataDir =  new Path(testDir, "datadir");
    fileSystem.mkdirs(dataDir);
  }

  @After
  public void tearDown() throws IOException {
    fileSystem.delete(testDir, true);
  }

  /**
   * Test CSV input file with header/footer skip.
   */
  @Test
  public void testSkipFileSplits() throws Exception {
    FileSystem fs = dataDir.getFileSystem(job);
    FileInputFormat.setInputPaths(job, dataDir);

    // First Dir with 1 File
    Path dir1_file1 = new Path(dataDir, "skipfile1.csv");
    writeTextFile(dir1_file1,
        "dir1_header\n" +
            "dir1_file1_line1\n" +
            "dir1_file1_line2\n" +
            "dir1_footer"
    );

    SkippingTextInputFormat inputFormat = new SkippingTextInputFormat();
    // One header and one footer line to be deducted
    inputFormat.configure(job, 1, 1);

    FileInputFormat.setInputPaths(job, dir1_file1);
    InputSplit[] splits = inputFormat.getSplits(job, 2);

    assertTrue(splits.length == 2);

    // Read all values.
    List<String> received = new ArrayList<String>();
    for (int i=0; i < splits.length; i++) {
      RecordReader<LongWritable, Text> reader =
          inputFormat.getRecordReader(splits[i], job, reporter);

      HiveInputFormat.HiveInputSplit hiveInputSplit =
          new HiveInputFormat.HiveInputSplit(splits[i], inputFormat.getClass().getName());
      assertTrue(hiveInputSplit.getInputSplit().getClass() == FileSplit.class);

      LongWritable key = reader.createKey();
      Text value = reader.createValue();
      while (reader.next(key, value)) {
        received.add(value.toString());
      }
      reader.close();
    }
    // make sure we skipped the header and the footer across splits
    assertTrue(received.size() == 2);
    assertTrue(!received.get(0).contains("header"));
    assertTrue(!received.get(received.size()-1).contains("footer"));
  }

  /**
   * Test compressed CSV input file with header/footer skip.
   */
  @Test
  public void testSkipCompressedFileSplits() throws Exception {
    FileSystem fs = dataDir.getFileSystem(job);
    FileInputFormat.setInputPaths(job, dataDir);

    // First Dir with 1 Compressed CSV File
    Path dir1_file1 = new Path(dataDir, "skipfile1.csv.bz2");
    writeTextFile(dir1_file1,
        "dir1_header\n" +
            "dir1_file1_line1\n" +
            "dir1_file1_line2\n" +
            "dir1_footer"
    );

    SkippingTextInputFormat inputFormat = new SkippingTextInputFormat();
    // One header and one footer line to be deducted
    inputFormat.configure(job, 1, 1);

    compressionCodecs = new CompressionCodecFactory(conf);
    codec = compressionCodecs.getCodec(dir1_file1);
    System.out.println("Codec: "+ codec);

    FileInputFormat.setInputPaths(job, dir1_file1);
    InputSplit[] splits = inputFormat.getSplits(job, 1);

    // Should not generate splits for compressed file!
    assertTrue(splits.length == 1);

    // Read all values.
    List<String> received = new ArrayList<String>();
    for (int i=0; i < splits.length; i++) {
      RecordReader<LongWritable, Text> reader =
          inputFormat.getRecordReader(splits[i], job, reporter);

      HiveInputFormat.HiveInputSplit hiveInputSplit =
          new HiveInputFormat.HiveInputSplit(splits[i], inputFormat.getClass().getName());
      System.out.println(hiveInputSplit.getInputSplit().getClass());
      assertTrue(FileSplit.class == hiveInputSplit.getInputSplit().getClass());
      System.out.println("Split: [" +i + "] "+ hiveInputSplit.getStart() + " => " + hiveInputSplit.getLength());

      LongWritable key = reader.createKey();
      Text value = reader.createValue();
      while (reader.next(key, value)) {
        System.out.println("Splits:" + i + " Val: "+ value);
        received.add(value.toString());
      }
      reader.close();
    }
  }

  /**
   * Writes the given string to the given file.
   */
  private void writeTextFile(Path file, String content) throws IOException {
    OutputStreamWriter writer = new OutputStreamWriter(fileSystem.create(file));
    writer.write(content);
    writer.close();
  }
}
