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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
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
   * Reproduces ClassCastException (PushbackInputStream cannot be cast to Seekable)
   * during split generation when a header line ends in a lone CR ('\r') followed
   * by a non-'\n' byte (classic-Mac line endings). readLine() swaps the stream's
   * inner input for a non-Seekable PushbackInputStream; the following getPos() casts.
   * With the byte-counting reader this now succeeds instead of throwing.
   */
  @Test
  public void testSkipFileSplitsLoneCR() throws Exception {
    FileInputFormat.setInputPaths(job, dataDir);
    Path loneCrFile = new Path(dataDir, "data1_cr_only.csv");
    // Exact bytes from HIVE-29785:
    //   printf "id;name;place;\n1;smruti;ctc;\n2;biswal;bbsr;\n3;'';NULL;\n" | tr '\n' '\r'
    writeTextFile(loneCrFile,
        "id;name;place;\r" +
        "1;smruti;ctc;\r" +
        "2;biswal;bbsr;\r" +
        "3;'';NULL;\r");

    SkippingTextInputFormat inputFormat = new SkippingTextInputFormat();
    inputFormat.configure(job, 1, 0); // skip.header.line.count = 1, no footer
    FileInputFormat.setInputPaths(job, loneCrFile);

    // On unmodified master this throws ClassCastException during split generation.
    InputSplit[] splits = inputFormat.getSplits(job, 1);
    assertTrue(splits.length >= 1);

    // Read every row back: the header must be skipped and no data row truncated,
    // i.e. SELECT COUNT(*) would return the 3 data rows.
    List<String> received = new ArrayList<String>();
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
    assertEquals(3, received.size());
    assertEquals("1;smruti;ctc;", received.get(0));
    assertEquals("2;biswal;bbsr;", received.get(1));
    assertEquals("3;'';NULL;", received.get(2));
  }

  /**
   * The lone-CR fix must not move split boundaries for the well-behaved cases.
   * The same logical content is written with LF, lone-CR and CRLF terminators;
   * LF and lone-CR share a one-byte terminator so their (start, length) must be
   * identical, while CRLF's two-byte terminator shifts the header boundary by one.
   */
  @Test
  public void testSkipHeaderSplitOffsetsAcrossLineEndings() throws Exception {
    Path lf = new Path(dataDir, "lf.csv");
    writeTextFile(lf,
        "id;name;place;\n1;smruti;ctc;\n2;biswal;bbsr;\n3;'';NULL;\n");
    Path cr = new Path(dataDir, "cr.csv");
    writeTextFile(cr,
        "id;name;place;\r1;smruti;ctc;\r2;biswal;bbsr;\r3;'';NULL;\r");
    Path crlf = new Path(dataDir, "crlf.csv");
    writeTextFile(crlf,
        "id;name;place;\r\n1;smruti;ctc;\r\n2;biswal;bbsr;\r\n3;'';NULL;\r\n");

    FileSplit lfSplit = singleHeaderSplit(lf);
    FileSplit crSplit = singleHeaderSplit(cr);
    FileSplit crlfSplit = singleHeaderSplit(crlf);

    // LF and lone-CR: identical byte layout (1-byte terminator) => identical split.
    assertEquals(14, lfSplit.getStart());
    assertEquals(41, lfSplit.getLength());
    assertEquals(lfSplit.getStart(), crSplit.getStart());
    assertEquals(lfSplit.getLength(), crSplit.getLength());

    // CRLF: 2-byte terminator shifts the header boundary by one; file is 4 bytes longer.
    assertEquals(15, crlfSplit.getStart());
    assertEquals(44, crlfSplit.getLength());
  }

  /**
   * Exercises the footer detection path (getCachedEndIndex) with lone-CR line
   * endings, which throws the same ClassCastException on unmodified master.
   * The header and footer rows must be skipped and the two data rows read back.
   */
  @Test
  public void testSkipFileSplitsLoneCRHeaderFooter() throws Exception {
    FileInputFormat.setInputPaths(job, dataDir);
    Path file = new Path(dataDir, "cr_header_footer.csv");
    writeTextFile(file,
        "dir1_header\r" +
        "dir1_file1_line1\r" +
        "dir1_file1_line2\r" +
        "dir1_footer");

    SkippingTextInputFormat inputFormat = new SkippingTextInputFormat();
    inputFormat.configure(job, 1, 1); // skip one header and one footer line
    FileInputFormat.setInputPaths(job, file);
    InputSplit[] splits = inputFormat.getSplits(job, 2);

    List<String> received = new ArrayList<String>();
    for (int i = 0; i < splits.length; i++) {
      RecordReader<LongWritable, Text> reader =
          inputFormat.getRecordReader(splits[i], job, reporter);
      LongWritable key = reader.createKey();
      Text value = reader.createValue();
      while (reader.next(key, value)) {
        received.add(value.toString());
      }
      reader.close();
    }
    assertEquals(2, received.size());
    assertTrue(!received.get(0).contains("header"));
    assertTrue(!received.get(received.size() - 1).contains("footer"));
  }

  /**
   * Header skipping with a custom textinputformat.record.delimiter must locate the
   * delimiter by BYTE offset. The last header line is decoded one char per byte
   * (ISO-8859-1) so indexOf() lines up with the byte-counting currPos; decoding the
   * header as UTF-8 (Text.toString()) would shrink the index by one for every
   * multi-byte character preceding the delimiter and shift the split start.
   */
  @Test
  public void testSkipHeaderMultiByteWithRecordDelimiter() throws Exception {
    // 'é' is two UTF-8 bytes (0xC3 0xA9), so the '~' delimiter sits at byte offset 7
    // ("h", "é" [2 bytes], "a", "d", "e", "r", "~") but at char index 6 under UTF-8.
    Path file = new Path(dataDir, "multibyte_delim.csv");
    writeUtf8File(file, "héader~data_row1~data_row2");

    job.set("textinputformat.record.delimiter", "~");
    SkippingTextInputFormat inputFormat = new SkippingTextInputFormat();
    inputFormat.configure(job, 1, 0); // skip one header "line", no footer
    FileInputFormat.setInputPaths(job, file);
    InputSplit[] splits = inputFormat.getSplits(job, 1);

    assertEquals(1, splits.length);
    assertTrue(splits[0] instanceof FileSplit);
    // Byte offset of the delimiter; char-based UTF-8 decoding would wrongly yield 6.
    assertEquals(7, ((FileSplit) splits[0]).getStart());
  }

  /**
   * Generates the single (header-adjusted) split for the given file with
   * skip.header.line.count=1 and no footer.
   */
  private FileSplit singleHeaderSplit(Path file) throws Exception {
    SkippingTextInputFormat inputFormat = new SkippingTextInputFormat();
    inputFormat.configure(job, 1, 0);
    FileInputFormat.setInputPaths(job, file);
    InputSplit[] splits = inputFormat.getSplits(job, 1);
    assertEquals(1, splits.length);
    assertTrue(splits[0] instanceof FileSplit);
    return (FileSplit) splits[0];
  }

  /**
   * Writes the given string to the given file.
   */
  private void writeTextFile(Path file, String content) throws IOException {
    OutputStreamWriter writer = new OutputStreamWriter(fileSystem.create(file));
    writer.write(content);
    writer.close();
  }

  /**
   * Writes the given string to the given file as UTF-8, so multi-byte characters
   * land as a deterministic byte sequence regardless of the platform default charset.
   */
  private void writeUtf8File(Path file, String content) throws IOException {
    try (OutputStreamWriter writer =
        new OutputStreamWriter(fileSystem.create(file), StandardCharsets.UTF_8)) {
      writer.write(content);
    }
  }
}
