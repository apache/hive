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
package org.apache.hadoop.hive.ql.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.txn.ValidCompactorTxnList;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.ql.io.orc.TestInputOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.TestInputOutputFormat.MockFile;
import org.apache.hadoop.hive.ql.io.orc.TestInputOutputFormat.MockFileSystem;
import org.apache.hadoop.hive.ql.io.orc.TestInputOutputFormat.MockPath;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestAcidUtils {

  @Test
  public void testCreateFilename() throws Exception {
    Path p = new Path("/tmp");
    Configuration conf = new Configuration();
    AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf)
        .setOldStyle(true).bucket(1);
    assertEquals("/tmp/00001_0",
        AcidUtils.createFilename(p, options).toString());
    options.bucket(123);
    assertEquals("/tmp/00123_0",
        AcidUtils.createFilename(p, options).toString());
    options.bucket(23)
        .minimumTransactionId(100)
        .maximumTransactionId(200)
        .writingBase(true)
        .setOldStyle(false);
    assertEquals("/tmp/base_0000200/bucket_00023",
        AcidUtils.createFilename(p, options).toString());
    options.writingBase(false);
    assertEquals("/tmp/delta_0000100_0000200/bucket_00023",
        AcidUtils.createFilename(p, options).toString());
  }

  @Test
  public void testParsing() throws Exception {
    assertEquals(123, AcidUtils.parseBase(new Path("/tmp/base_000123")));
    Path dir = new Path("/tmp/tbl");
    Configuration conf = new Configuration();
    AcidOutputFormat.Options opts =
        AcidUtils.parseBaseBucketFilename(new Path(dir, "base_567/bucket_123"),
            conf);
    assertEquals(false, opts.getOldStyle());
    assertEquals(true, opts.isWritingBase());
    assertEquals(567, opts.getMaximumTransactionId());
    assertEquals(0, opts.getMinimumTransactionId());
    assertEquals(123, opts.getBucket());
    opts = AcidUtils.parseBaseBucketFilename(new Path(dir, "000123_0"), conf);
    assertEquals(true, opts.getOldStyle());
    assertEquals(true, opts.isWritingBase());
    assertEquals(123, opts.getBucket());
    assertEquals(0, opts.getMinimumTransactionId());
    assertEquals(0, opts.getMaximumTransactionId());
  }

  @Test
  public void testOriginal() throws Exception {
    Configuration conf = new Configuration();
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/tbl/part1/000000_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/000001_1", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/000002_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/random", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/_done", 0, new byte[0]),
        new MockFile("mock:/tbl/part1/subdir/000000_0", 0, new byte[0]));
    AcidUtils.Directory dir =
        AcidUtils.getAcidState(new MockPath(fs, "/tbl/part1"), conf,
            new ValidReadTxnList("100:"));
    assertEquals(null, dir.getBaseDirectory());
    assertEquals(0, dir.getCurrentDirectories().size());
    assertEquals(0, dir.getObsolete().size());
    List<FileStatus> result = dir.getOriginalFiles();
    assertEquals(5, result.size());
    assertEquals("mock:/tbl/part1/000000_0", result.get(0).getPath().toString());
    assertEquals("mock:/tbl/part1/000001_1", result.get(1).getPath().toString());
    assertEquals("mock:/tbl/part1/000002_0", result.get(2).getPath().toString());
    assertEquals("mock:/tbl/part1/random", result.get(3).getPath().toString());
    assertEquals("mock:/tbl/part1/subdir/000000_0", result.get(4).getPath().toString());
  }

  @Test
  public void testOriginalDeltas() throws Exception {
    Configuration conf = new Configuration();
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/tbl/part1/000000_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/000001_1", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/000002_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/random", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/_done", 0, new byte[0]),
        new MockFile("mock:/tbl/part1/subdir/000000_0", 0, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_025_025/bucket_0", 0, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_029_029/bucket_0", 0, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_025_030/bucket_0", 0, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_050_100/bucket_0", 0, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_101_101/bucket_0", 0, new byte[0]));
    AcidUtils.Directory dir =
        AcidUtils.getAcidState(new TestInputOutputFormat.MockPath(fs,
            "mock:/tbl/part1"), conf, new ValidReadTxnList("100:"));
    assertEquals(null, dir.getBaseDirectory());
    List<FileStatus> obsolete = dir.getObsolete();
    assertEquals(2, obsolete.size());
    assertEquals("mock:/tbl/part1/delta_025_025",
        obsolete.get(0).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_029_029",
        obsolete.get(1).getPath().toString());
    List<FileStatus> result = dir.getOriginalFiles();
    assertEquals(5, result.size());
    assertEquals("mock:/tbl/part1/000000_0", result.get(0).getPath().toString());
    assertEquals("mock:/tbl/part1/000001_1", result.get(1).getPath().toString());
    assertEquals("mock:/tbl/part1/000002_0", result.get(2).getPath().toString());
    assertEquals("mock:/tbl/part1/random", result.get(3).getPath().toString());
    assertEquals("mock:/tbl/part1/subdir/000000_0", result.get(4).getPath().toString());
    List<AcidUtils.ParsedDelta> deltas = dir.getCurrentDirectories();
    assertEquals(2, deltas.size());
    AcidUtils.ParsedDelta delt = deltas.get(0);
    assertEquals("mock:/tbl/part1/delta_025_030", delt.getPath().toString());
    assertEquals(25, delt.getMinTransaction());
    assertEquals(30, delt.getMaxTransaction());
    delt = deltas.get(1);
    assertEquals("mock:/tbl/part1/delta_050_100", delt.getPath().toString());
    assertEquals(50, delt.getMinTransaction());
    assertEquals(100, delt.getMaxTransaction());
  }

  @Test
  public void testBaseDeltas() throws Exception {
    Configuration conf = new Configuration();
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/tbl/part1/base_5/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/base_10/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/base_49/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_025_025/bucket_0", 0, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_029_029/bucket_0", 0, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_025_030/bucket_0", 0, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_050_105/bucket_0", 0, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_90_120/bucket_0", 0, new byte[0]));
    AcidUtils.Directory dir =
        AcidUtils.getAcidState(new TestInputOutputFormat.MockPath(fs,
            "mock:/tbl/part1"), conf, new ValidReadTxnList("100:"));
    assertEquals("mock:/tbl/part1/base_49", dir.getBaseDirectory().toString());
    List<FileStatus> obsolete = dir.getObsolete();
    assertEquals(5, obsolete.size());
    assertEquals("mock:/tbl/part1/base_10", obsolete.get(0).getPath().toString());
    assertEquals("mock:/tbl/part1/base_5", obsolete.get(1).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_025_030", obsolete.get(2).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_025_025", obsolete.get(3).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_029_029", obsolete.get(4).getPath().toString());
    assertEquals(0, dir.getOriginalFiles().size());
    List<AcidUtils.ParsedDelta> deltas = dir.getCurrentDirectories();
    assertEquals(1, deltas.size());
    AcidUtils.ParsedDelta delt = deltas.get(0);
    assertEquals("mock:/tbl/part1/delta_050_105", delt.getPath().toString());
    assertEquals(50, delt.getMinTransaction());
    assertEquals(105, delt.getMaxTransaction());
  }

  @Test
  public void testBestBase() throws Exception {
    Configuration conf = new Configuration();
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/tbl/part1/base_5/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/base_10/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/base_25/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/base_100/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/base_200/bucket_0", 500, new byte[0]));
    Path part = new MockPath(fs, "/tbl/part1");
    AcidUtils.Directory dir =
        AcidUtils.getAcidState(part, conf, new ValidReadTxnList("150:"));
    assertEquals("mock:/tbl/part1/base_200", dir.getBaseDirectory().toString());
    List<FileStatus> obsoletes = dir.getObsolete();
    assertEquals(4, obsoletes.size());
    assertEquals("mock:/tbl/part1/base_10", obsoletes.get(0).getPath().toString());
    assertEquals("mock:/tbl/part1/base_100", obsoletes.get(1).getPath().toString());
    assertEquals("mock:/tbl/part1/base_25", obsoletes.get(2).getPath().toString());
    assertEquals("mock:/tbl/part1/base_5", obsoletes.get(3).getPath().toString());
    assertEquals(0, dir.getOriginalFiles().size());
    assertEquals(0, dir.getCurrentDirectories().size());
    // we should always get the latest base
    dir = AcidUtils.getAcidState(part, conf, new ValidReadTxnList("10:"));
    assertEquals("mock:/tbl/part1/base_200", dir.getBaseDirectory().toString());
  }

  @Test
  public void testObsoleteOriginals() throws Exception {
    Configuration conf = new Configuration();
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/tbl/part1/base_10/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/base_5/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/000000_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/000001_1", 500, new byte[0]));
    Path part = new MockPath(fs, "/tbl/part1");
    AcidUtils.Directory dir =
        AcidUtils.getAcidState(part, conf, new ValidReadTxnList("150:"));
    // The two original buckets won't be in the obsolete list because we don't look at those
    // until we have determined there is no base.
    List<FileStatus> obsolete = dir.getObsolete();
    assertEquals(1, obsolete.size());
    assertEquals("mock:/tbl/part1/base_5", obsolete.get(0).getPath().toString());
    assertEquals("mock:/tbl/part1/base_10", dir.getBaseDirectory().toString());
  }

  @Test
  public void testOverlapingDelta() throws Exception {
    Configuration conf = new Configuration();
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/tbl/part1/delta_0000063_63/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_000062_62/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_00061_61/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_40_60/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_0060_60/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_052_55/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_40_60/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/base_50/bucket_0", 500, new byte[0]));
    Path part = new MockPath(fs, "mock:/tbl/part1");
    AcidUtils.Directory dir =
        AcidUtils.getAcidState(part, conf, new ValidReadTxnList("100:"));
    assertEquals("mock:/tbl/part1/base_50", dir.getBaseDirectory().toString());
    List<FileStatus> obsolete = dir.getObsolete();
    assertEquals(2, obsolete.size());
    assertEquals("mock:/tbl/part1/delta_052_55", obsolete.get(0).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_0060_60", obsolete.get(1).getPath().toString());
    List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
    assertEquals(4, delts.size());
    assertEquals("mock:/tbl/part1/delta_40_60", delts.get(0).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_00061_61", delts.get(1).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_000062_62", delts.get(2).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_0000063_63", delts.get(3).getPath().toString());
  }

  @Test
  public void deltasWithOpenTxnInRead() throws Exception {
    Configuration conf = new Configuration();
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/tbl/part1/delta_1_1/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_2_5/bucket_0", 500, new byte[0]));
    Path part = new MockPath(fs, "mock:/tbl/part1");
    AcidUtils.Directory dir = AcidUtils.getAcidState(part, conf, new ValidReadTxnList("100:4"));
    List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
    assertEquals(2, delts.size());
    assertEquals("mock:/tbl/part1/delta_1_1", delts.get(0).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_2_5", delts.get(1).getPath().toString());
  }

  @Test
  public void deltasWithOpenTxnsNotInCompact() throws Exception {
    Configuration conf = new Configuration();
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/tbl/part1/delta_1_1/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_2_5/bucket_0", 500, new byte[0]));
    Path part = new MockPath(fs, "mock:/tbl/part1");
    AcidUtils.Directory dir =
        AcidUtils.getAcidState(part, conf, new ValidCompactorTxnList("100:4"));
    List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
    assertEquals(1, delts.size());
    assertEquals("mock:/tbl/part1/delta_1_1", delts.get(0).getPath().toString());
  }

  @Test
  public void deltasWithOpenTxnsNotInCompact2() throws Exception {
    Configuration conf = new Configuration();
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/tbl/part1/delta_1_1/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_2_5/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_2_5/bucket_0" + AcidUtils.DELTA_SIDE_FILE_SUFFIX, 500,
            new byte[0]),
        new MockFile("mock:/tbl/part1/delta_6_10/bucket_0", 500, new byte[0]));
    Path part = new MockPath(fs, "mock:/tbl/part1");
    AcidUtils.Directory dir =
        AcidUtils.getAcidState(part, conf, new ValidCompactorTxnList("100:4"));
    List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
    assertEquals(1, delts.size());
    assertEquals("mock:/tbl/part1/delta_1_1", delts.get(0).getPath().toString());
  }


}
