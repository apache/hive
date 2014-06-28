/**
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

package org.apache.hive.hcatalog.rcfile;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TestRCFile.
 *
 */
public class TestRCFileMapReduceInputFormat extends TestCase {

  private static final Logger LOG = LoggerFactory.getLogger(TestRCFileMapReduceInputFormat.class);

  private static Configuration conf = new Configuration();

  private static ColumnarSerDe serDe;

  private static Path file;

  private static FileSystem fs;

  private static Properties tbl;

  static {
    try {
      fs = FileSystem.getLocal(conf);
      Path dir = new Path(System.getProperty("test.tmp.dir", ".") + "/mapred");
      file = new Path(dir, "test_rcfile");
      fs.delete(dir, true);
      // the SerDe part is from TestLazySimpleSerDe
      serDe = new ColumnarSerDe();
      // Create the SerDe
      tbl = createProperties();
      SerDeUtils.initializeSerDe(serDe, conf, tbl, null);
    } catch (Exception e) {
    }
  }

  private static BytesRefArrayWritable patialS = new BytesRefArrayWritable();

  private static byte[][] bytesArray = null;

  private static BytesRefArrayWritable s = null;

  static {
    try {
      bytesArray = new byte[][]{"123".getBytes("UTF-8"),
        "456".getBytes("UTF-8"), "789".getBytes("UTF-8"),
        "1000".getBytes("UTF-8"), "5.3".getBytes("UTF-8"),
        "hive and hadoop".getBytes("UTF-8"), new byte[0],
        "NULL".getBytes("UTF-8")};
      s = new BytesRefArrayWritable(bytesArray.length);
      s.set(0, new BytesRefWritable("123".getBytes("UTF-8")));
      s.set(1, new BytesRefWritable("456".getBytes("UTF-8")));
      s.set(2, new BytesRefWritable("789".getBytes("UTF-8")));
      s.set(3, new BytesRefWritable("1000".getBytes("UTF-8")));
      s.set(4, new BytesRefWritable("5.3".getBytes("UTF-8")));
      s.set(5, new BytesRefWritable("hive and hadoop".getBytes("UTF-8")));
      s.set(6, new BytesRefWritable("NULL".getBytes("UTF-8")));
      s.set(7, new BytesRefWritable("NULL".getBytes("UTF-8")));

      // partial test init
      patialS.set(0, new BytesRefWritable("NULL".getBytes("UTF-8")));
      patialS.set(1, new BytesRefWritable("NULL".getBytes("UTF-8")));
      patialS.set(2, new BytesRefWritable("789".getBytes("UTF-8")));
      patialS.set(3, new BytesRefWritable("1000".getBytes("UTF-8")));
      patialS.set(4, new BytesRefWritable("NULL".getBytes("UTF-8")));
      patialS.set(5, new BytesRefWritable("NULL".getBytes("UTF-8")));
      patialS.set(6, new BytesRefWritable("NULL".getBytes("UTF-8")));
      patialS.set(7, new BytesRefWritable("NULL".getBytes("UTF-8")));

    } catch (UnsupportedEncodingException e) {
    }
  }


  /** For debugging and testing. */
  public static void main(String[] args) throws Exception {
    int count = 10000;
    boolean create = true;

    String usage = "Usage: RCFile " + "[-count N]" + " file";
    if (args.length == 0) {
      LOG.error(usage);
      System.exit(-1);
    }

    try {
      for (int i = 0; i < args.length; ++i) { // parse command line
        if (args[i] == null) {
          continue;
        } else if (args[i].equals("-count")) {
          count = Integer.parseInt(args[++i]);
        } else {
          // file is required parameter
          file = new Path(args[i]);
        }
      }

      if (file == null) {
        LOG.error(usage);
        System.exit(-1);
      }

      LOG.info("count = {}", count);
      LOG.info("create = {}", create);
      LOG.info("file = {}", file);

      // test.performanceTest();
      LOG.info("Finished.");
    } finally {
      fs.close();
    }
  }

  private static Properties createProperties() {
    Properties tbl = new Properties();

    // Set the configuration parameters
    tbl.setProperty(serdeConstants.SERIALIZATION_FORMAT, "9");
    tbl.setProperty("columns",
      "abyte,ashort,aint,along,adouble,astring,anullint,anullstring");
    tbl.setProperty("columns.types",
      "tinyint:smallint:int:bigint:double:string:int:string");
    tbl.setProperty(serdeConstants.SERIALIZATION_NULL_FORMAT, "NULL");
    return tbl;
  }


  public void testSynAndSplit() throws IOException, InterruptedException {
    splitBeforeSync();
    splitRightBeforeSync();
    splitInMiddleOfSync();
    splitRightAfterSync();
    splitAfterSync();
  }

  private void splitBeforeSync() throws IOException, InterruptedException {
    writeThenReadByRecordReader(600, 1000, 2, 17684, null);
  }

  private void splitRightBeforeSync() throws IOException, InterruptedException {
    writeThenReadByRecordReader(500, 1000, 2, 17750, null);
  }

  private void splitInMiddleOfSync() throws IOException, InterruptedException {
    writeThenReadByRecordReader(500, 1000, 2, 17760, null);

  }

  private void splitRightAfterSync() throws IOException, InterruptedException {
    writeThenReadByRecordReader(500, 1000, 2, 17770, null);
  }

  private void splitAfterSync() throws IOException, InterruptedException {
    writeThenReadByRecordReader(500, 1000, 2, 19950, null);
  }

  private void writeThenReadByRecordReader(int intervalRecordCount,
                       int writeCount, int splitNumber, long maxSplitSize, CompressionCodec codec)
    throws IOException, InterruptedException {
    Path testDir = new Path(System.getProperty("test.tmp.dir", ".")
      + "/mapred/testsmallfirstsplit");
    Path testFile = new Path(testDir, "test_rcfile");
    fs.delete(testFile, true);
    Configuration cloneConf = new Configuration(conf);
    RCFileOutputFormat.setColumnNumber(cloneConf, bytesArray.length);
    cloneConf.setInt(HiveConf.ConfVars.HIVE_RCFILE_RECORD_INTERVAL.varname, intervalRecordCount);

    RCFile.Writer writer = new RCFile.Writer(fs, cloneConf, testFile, null, codec);

    BytesRefArrayWritable bytes = new BytesRefArrayWritable(bytesArray.length);
    for (int i = 0; i < bytesArray.length; i++) {
      BytesRefWritable cu = null;
      cu = new BytesRefWritable(bytesArray[i], 0, bytesArray[i].length);
      bytes.set(i, cu);
    }
    for (int i = 0; i < writeCount; i++) {
      writer.append(bytes);
    }
    writer.close();

    RCFileMapReduceInputFormat<LongWritable, BytesRefArrayWritable> inputFormat = new RCFileMapReduceInputFormat<LongWritable, BytesRefArrayWritable>();
    Configuration jonconf = new Configuration(cloneConf);
    jonconf.set("mapred.input.dir", testDir.toString());
    JobContext context = new Job(jonconf);
    context.getConfiguration().setLong(
        ShimLoader.getHadoopShims().getHadoopConfNames().get("MAPREDMAXSPLITSIZE"), maxSplitSize);
    List<InputSplit> splits = inputFormat.getSplits(context);
    assertEquals("splits length should be " + splitNumber, splits.size(), splitNumber);
    int readCount = 0;
    for (int i = 0; i < splits.size(); i++) {
      TaskAttemptContext tac = ShimLoader.getHadoopShims().getHCatShim().createTaskAttemptContext(jonconf,
          new TaskAttemptID());
      RecordReader<LongWritable, BytesRefArrayWritable> rr = inputFormat.createRecordReader(splits.get(i), tac);
      rr.initialize(splits.get(i), tac);
      while (rr.nextKeyValue()) {
        readCount++;
      }
    }
    assertEquals("readCount should be equal to writeCount", readCount, writeCount);
  }

}


