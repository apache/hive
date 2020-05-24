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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.tez;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.serializer.JavaSerialization;
import org.apache.hadoop.io.serializer.JavaSerializationComparator;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestTezTotalOrderPartitioner {

  public static final String PARTITIONER_PATH = "mapreduce.totalorderpartitioner.path";
  private static final String TEZ_RUNTIME_FRAMEWORK_PREFIX = "tez.runtime.framework.";

  public static final String TEZ_RUNTIME_NUM_EXPECTED_PARTITIONS =
      TEZ_RUNTIME_FRAMEWORK_PREFIX + "num.expected.partitions";

  private static final int LENGTH_BYTES = 4;

  private static final HiveKey[] splitStrings = new HiveKey[] {
      // -inf // 0
      new HiveKey("aabbb".getBytes()), // 1
      new HiveKey("babbb".getBytes()), // 2
      new HiveKey("daddd".getBytes()), // 3
      new HiveKey("dddee".getBytes()), // 4
      new HiveKey("ddhee".getBytes()), // 5
      new HiveKey("dingo".getBytes()), // 6
      new HiveKey("hijjj".getBytes()), // 7
      new HiveKey("n".getBytes()), // 8
      new HiveKey("yak".getBytes()), // 9
  };

  static class Check<T> {
    T data;
    int part;

    Check(T data, int part) {
      this.data = data;
      this.part = part;
    }
  }

  private static final ArrayList<Check<HiveKey>> testStrings = new ArrayList<Check<HiveKey>>();
  static {
    testStrings.add(new Check<HiveKey>(new HiveKey("aaaaa".getBytes()), 0));
    testStrings.add(new Check<HiveKey>(new HiveKey("aaabb".getBytes()), 0));
    testStrings.add(new Check<HiveKey>(new HiveKey("aabbb".getBytes()), 1));
    testStrings.add(new Check<HiveKey>(new HiveKey("aaaaa".getBytes()), 0));
    testStrings.add(new Check<HiveKey>(new HiveKey("babbb".getBytes()), 2));
    testStrings.add(new Check<HiveKey>(new HiveKey("baabb".getBytes()), 1));
    testStrings.add(new Check<HiveKey>(new HiveKey("yai".getBytes()), 8));
    testStrings.add(new Check<HiveKey>(new HiveKey("yak".getBytes()), 9));
    testStrings.add(new Check<HiveKey>(new HiveKey("z".getBytes()), 9));
    testStrings.add(new Check<HiveKey>(new HiveKey("ddngo".getBytes()), 5));
    testStrings.add(new Check<HiveKey>(new HiveKey("hi".getBytes()), 6));
  };

  private static <T> Path writePartitionFile(String testname, Configuration conf, T[] splits)
      throws IOException {
    final FileSystem fs = FileSystem.getLocal(conf);
    final Path testdir = new Path(System.getProperty("test.build.data", "/tmp"))
        .makeQualified(fs.getUri(), fs.getWorkingDirectory());
    Path p = new Path(testdir, testname + "/_partition.lst");
    conf.set(PARTITIONER_PATH, p.toString());
    conf.setInt(TEZ_RUNTIME_NUM_EXPECTED_PARTITIONS, splits.length + 1);
    SequenceFile.Writer w = null;
    try {
      w = SequenceFile.createWriter(conf, SequenceFile.Writer.file(p),
          SequenceFile.Writer.keyClass(HiveKey.class),
          SequenceFile.Writer.valueClass(NullWritable.class),
          SequenceFile.Writer.compression(CompressionType.NONE));
      for (int i = 0; i < splits.length; ++i) {
        w.append(splits[i], NullWritable.get());
      }
    } finally {
      if (null != w)
        w.close();
    }
    return p;
  }

  @Test
  public void testTotalOrderMemCmp() throws Exception {
    TezTotalOrderPartitioner partitioner = new TezTotalOrderPartitioner();
    Configuration conf = new Configuration();
    Path p = TestTezTotalOrderPartitioner.<HiveKey> writePartitionFile("totalordermemcmp", conf,
        splitStrings);
    try {
      partitioner.configure(new JobConf(conf));
      NullWritable nw = NullWritable.get();
      for (Check<HiveKey> chk : testStrings) {
        assertEquals(chk.data.toString(), chk.part,
            partitioner.getPartition(chk.data, nw, splitStrings.length + 1));
      }
    } finally {
      p.getFileSystem(conf).delete(p, true);
    }
  }

  @Test
  public void testTotalOrderBinarySearch() throws Exception {
    TezTotalOrderPartitioner partitioner = new TezTotalOrderPartitioner();
    Configuration conf = new Configuration();
    Path p = TestTezTotalOrderPartitioner.<HiveKey> writePartitionFile("totalorderbinarysearch",
        conf, splitStrings);
    conf.setBoolean(TotalOrderPartitioner.NATURAL_ORDER, false);

    try {
      partitioner.configure(new JobConf(conf));
      NullWritable nw = NullWritable.get();
      for (Check<HiveKey> chk : testStrings) {
        assertEquals(chk.data.toString(), chk.part,
            partitioner.getPartition(chk.data, nw, splitStrings.length + 1));
      }
    } finally {
      p.getFileSystem(conf).delete(p, true);
    }
  }

  /** A Comparator optimized for HiveKey. */
  public static class ReverseHiveKeyComparator implements RawComparator<HiveKey> {

    /**
     * Compare the buffers in serialized form.
     */
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      int c = -1 * WritableComparator.compareBytes(b1, s1 + LENGTH_BYTES, l1 - LENGTH_BYTES, b2,
          s2 + LENGTH_BYTES, l2 - LENGTH_BYTES);
      return c;
    }

    @Override
    public int compare(HiveKey o1, HiveKey o2) {
      return -o1.compareTo(o2);
    }
  }

  @Test
  public void testTotalOrderCustomComparator() throws Exception {
    TezTotalOrderPartitioner partitioner = new TezTotalOrderPartitioner();
    Configuration conf = new Configuration();
    HiveKey[] revSplitStrings = Arrays.copyOf(splitStrings, splitStrings.length);
    Arrays.sort(revSplitStrings, new ReverseHiveKeyComparator());
    Path p = TestTezTotalOrderPartitioner.<HiveKey> writePartitionFile("totalordercustomcomparator",
        conf, revSplitStrings);
    conf.setBoolean(TotalOrderPartitioner.NATURAL_ORDER, false);
    conf.setClass(MRJobConfig.KEY_COMPARATOR, ReverseHiveKeyComparator.class, RawComparator.class);
    ArrayList<Check<HiveKey>> revCheck = new ArrayList<Check<HiveKey>>();
    revCheck.add(new Check<HiveKey>(new HiveKey("aaaaa".getBytes()), 9));
    revCheck.add(new Check<HiveKey>(new HiveKey("aaabb".getBytes()), 9));
    revCheck.add(new Check<HiveKey>(new HiveKey("aabbb".getBytes()), 9));
    revCheck.add(new Check<HiveKey>(new HiveKey("aaaaa".getBytes()), 9));
    revCheck.add(new Check<HiveKey>(new HiveKey("babbb".getBytes()), 8));
    revCheck.add(new Check<HiveKey>(new HiveKey("baabb".getBytes()), 8));
    revCheck.add(new Check<HiveKey>(new HiveKey("yai".getBytes()), 1));
    revCheck.add(new Check<HiveKey>(new HiveKey("yak".getBytes()), 1));
    revCheck.add(new Check<HiveKey>(new HiveKey("z".getBytes()), 0));
    revCheck.add(new Check<HiveKey>(new HiveKey("ddngo".getBytes()), 4));
    revCheck.add(new Check<HiveKey>(new HiveKey("hi".getBytes()), 3));
    try {
      partitioner.configure(new JobConf(conf));
      NullWritable nw = NullWritable.get();
      for (Check<HiveKey> chk : revCheck) {
        assertEquals(chk.data.toString(), chk.part,
            partitioner.getPartition(chk.data, nw, splitStrings.length + 1));
      }
    } finally {
      p.getFileSystem(conf).delete(p, true);
    }
  }
}