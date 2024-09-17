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

import static org.apache.hadoop.hive.common.AcidConstants.SOFT_DELETE_TABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidCompactorWriteIdList;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidUtils.AcidOperationalProperties;
import org.apache.hadoop.hive.ql.io.orc.TestInputOutputFormat.MockFile;
import org.apache.hadoop.hive.ql.io.orc.TestInputOutputFormat.MockFileSystem;
import org.apache.hadoop.hive.ql.io.orc.TestInputOutputFormat.MockPath;
import org.apache.hadoop.hive.ql.io.orc.TestOrcRawRecordMerger;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.shims.HadoopShims.HdfsFileStatusWithId;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestAcidUtils {

  @Test
  public void testCreateFilename() throws Exception {
    Path p = new Path("/tmp");
    Configuration conf = new Configuration();
    AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf)
        .setOldStyle(true).bucket(1);
    assertEquals("/tmp/000001_0",
        AcidUtils.createFilename(p, options).toString());
    options.bucket(123);
    assertEquals("/tmp/000123_0",
      AcidUtils.createFilename(p, options).toString());
    options.bucket(23)
        .minimumWriteId(100)
        .maximumWriteId(200)
        .writingBase(true)
        .setOldStyle(false);
    assertEquals("/tmp/base_0000200/bucket_00023",
      AcidUtils.createFilename(p, options).toString());
    options.writingBase(false);
    assertEquals("/tmp/delta_0000100_0000200_0000/bucket_00023",
      AcidUtils.createFilename(p, options).toString());
    options.writingDeleteDelta(true);
    assertEquals("/tmp/delete_delta_0000100_0000200_0000/bucket_00023",
        AcidUtils.createFilename(p, options).toString());
    options.writingDeleteDelta(false);
    options.statementId(-1);
    assertEquals("/tmp/delta_0000100_0000200/bucket_00023",
      AcidUtils.createFilename(p, options).toString());
    options.writingDeleteDelta(true);
    assertEquals("/tmp/delete_delta_0000100_0000200/bucket_00023",
        AcidUtils.createFilename(p, options).toString());
    options.writingDeleteDelta(false);
    options.statementId(7);
    assertEquals("/tmp/delta_0000100_0000200_0007/bucket_00023",
      AcidUtils.createFilename(p, options).toString());
    options.writingDeleteDelta(true);
    assertEquals("/tmp/delete_delta_0000100_0000200_0007/bucket_00023",
        AcidUtils.createFilename(p, options).toString());
  }
  @Test
  public void testCreateFilenameLargeIds() throws Exception {
    Path p = new Path("/tmp");
    Configuration conf = new Configuration();
    AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf)
      .setOldStyle(true).bucket(123456789);
    assertEquals("/tmp/123456789_0",
      AcidUtils.createFilename(p, options).toString());
    options.bucket(23)
      .minimumWriteId(1234567880)
      .maximumWriteId(1234567890)
      .writingBase(true)
      .setOldStyle(false);
    assertEquals("/tmp/base_1234567890/bucket_00023",
      AcidUtils.createFilename(p, options).toString());
    options.writingBase(false);
    assertEquals("/tmp/delta_1234567880_1234567890_0000/bucket_00023",
      AcidUtils.createFilename(p, options).toString());
  }

  @Test
  public void testParsing() {
    Configuration conf = new Configuration();
    MockFileSystem fs = new MockFileSystem(conf,
        //new MockFile("mock:/tmp/base_000123/bucket_00001", 500, new byte[0]),
        new MockFile("mock:/tmp/delta_000005_000006/bucket_00001", 500, new byte[0]),
        new MockFile("mock:/tmp/delete_delta_000005_000006/bucket_00001", 500,
            new byte[0]));
    assertEquals(123,
        AcidUtils.ParsedBaseLight.parseBase(new Path("/tmp/base_000123")).getWriteId());
    assertEquals(0,
        AcidUtils.ParsedBaseLight.parseBase(new Path("/tmp/base_000123")).getVisibilityTxnId());
    Path dir = new Path("mock:/tmp/");
    AcidOutputFormat.Options opts =
        AcidUtils.parseBaseOrDeltaBucketFilename(new Path(dir, "base_567/bucket_123"),
            conf);
    assertEquals(false, opts.getOldStyle());
    assertEquals(true, opts.isWritingBase());
    assertEquals(567, opts.getMaximumWriteId());
    assertEquals(0, opts.getMinimumWriteId());
    assertEquals(123, opts.getBucketId());
    opts = AcidUtils.parseBaseOrDeltaBucketFilename(
        new MockPath(fs, dir + "/delta_000005_000006/bucket_00001"), conf);
    assertEquals(false, opts.getOldStyle());
    assertEquals(false, opts.isWritingBase());
    assertEquals(6, opts.getMaximumWriteId());
    assertEquals(5, opts.getMinimumWriteId());
    assertEquals(1, opts.getBucketId());
    opts = AcidUtils.parseBaseOrDeltaBucketFilename(
        new MockPath(fs, dir + "/delete_delta_000005_000006/bucket_00001"), conf);
    assertEquals(false, opts.getOldStyle());
    assertEquals(false, opts.isWritingBase());
    assertEquals(6, opts.getMaximumWriteId());
    assertEquals(5, opts.getMinimumWriteId());
    assertEquals(1, opts.getBucketId());
    opts = AcidUtils.parseBaseOrDeltaBucketFilename(new Path(dir, "000123_0"), conf);
    assertEquals(true, opts.getOldStyle());
    assertEquals(true, opts.isWritingBase());
    assertEquals(123, opts.getBucketId());
    assertEquals(0, opts.getMinimumWriteId());
    assertEquals(0, opts.getMaximumWriteId());
    // Test handling files generated by LOAD DATA command
    opts = AcidUtils.parseBaseOrDeltaBucketFilename(
        new MockPath(fs, dir + "/delta_0000002_0000002_0000/000012_0"), conf);
    assertEquals(true, opts.getOldStyle());
    assertEquals(false, opts.isWritingBase());
    assertEquals(12, opts.getBucketId());
    assertEquals(2, opts.getMinimumWriteId());
    assertEquals(2, opts.getMaximumWriteId());
  }

  @Test
  public void testOriginal() throws Exception {
    Configuration conf = new Configuration();
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/tbl/part1/000000_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/000000_0" + Utilities.COPY_KEYWORD + "1",
          500, new byte[0]),
        new MockFile("mock:/tbl/part1/000000_0" + Utilities.COPY_KEYWORD + "2",
          500, new byte[0]),
        new MockFile("mock:/tbl/part1/000001_1", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/000002_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/random", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/_done", 0, new byte[0]),
        new MockFile("mock:/tbl/part1/subdir/000000_0", 0, new byte[0]));
    AcidDirectory dir = AcidUtils.getAcidState(fs, new MockPath(fs, "/tbl/part1"), conf,
        new ValidReaderWriteIdList("tbl:100:" + Long.MAX_VALUE + ":"), null, false);
    assertEquals(null, dir.getBaseDirectory());
    assertEquals(0, dir.getCurrentDirectories().size());
    assertEquals(0, dir.getObsolete().size());
    List<HdfsFileStatusWithId> result = dir.getOriginalFiles();
    assertEquals(7, result.size());
    assertEquals("mock:/tbl/part1/000000_0", result.get(0).getFileStatus().getPath().toString());
    assertEquals("mock:/tbl/part1/000000_0" + Utilities.COPY_KEYWORD + "1",
      result.get(1).getFileStatus().getPath().toString());
    assertEquals("mock:/tbl/part1/000000_0" + Utilities.COPY_KEYWORD + "2",
      result.get(2).getFileStatus().getPath().toString());
    assertEquals("mock:/tbl/part1/000001_1", result.get(3).getFileStatus().getPath().toString());
    assertEquals("mock:/tbl/part1/000002_0", result.get(4).getFileStatus().getPath().toString());
    assertEquals("mock:/tbl/part1/random", result.get(5).getFileStatus().getPath().toString());
    assertEquals("mock:/tbl/part1/subdir/000000_0",
        result.get(6).getFileStatus().getPath().toString());
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
    conf.set(ValidTxnList.VALID_TXNS_KEY,
        new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
    AcidDirectory dir = AcidUtils.getAcidState(fs, new MockPath(fs, "mock:/tbl/part1"), conf,
        new ValidReaderWriteIdList("tbl:100:" + Long.MAX_VALUE + ":"), null, false);
    assertEquals(null, dir.getBaseDirectory());
    List<Path> obsolete = dir.getObsolete();
    assertEquals(2, obsolete.size());
    assertEquals("mock:/tbl/part1/delta_025_025",
        obsolete.get(0).toString());
    assertEquals("mock:/tbl/part1/delta_029_029",
        obsolete.get(1).toString());
    List<HdfsFileStatusWithId> result = dir.getOriginalFiles();
    assertEquals(5, result.size());
    assertEquals("mock:/tbl/part1/000000_0", result.get(0).getFileStatus().getPath().toString());
    assertEquals("mock:/tbl/part1/000001_1", result.get(1).getFileStatus().getPath().toString());
    assertEquals("mock:/tbl/part1/000002_0", result.get(2).getFileStatus().getPath().toString());
    assertEquals("mock:/tbl/part1/random", result.get(3).getFileStatus().getPath().toString());
    assertEquals("mock:/tbl/part1/subdir/000000_0",
        result.get(4).getFileStatus().getPath().toString());
    List<AcidUtils.ParsedDelta> deltas = dir.getCurrentDirectories();
    assertEquals(2, deltas.size());
    AcidUtils.ParsedDelta delt = deltas.get(0);
    assertEquals("mock:/tbl/part1/delta_025_030", delt.getPath().toString());
    assertEquals(25, delt.getMinWriteId());
    assertEquals(30, delt.getMaxWriteId());
    delt = deltas.get(1);
    assertEquals("mock:/tbl/part1/delta_050_100", delt.getPath().toString());
    assertEquals(50, delt.getMinWriteId());
    assertEquals(100, delt.getMaxWriteId());
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
    conf.set(ValidTxnList.VALID_TXNS_KEY,
        new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
    AcidDirectory dir = AcidUtils.getAcidState(fs, new MockPath(fs, "mock:/tbl/part1"), conf,
        new ValidReaderWriteIdList("tbl:100:" + Long.MAX_VALUE + ":"), null, false);
    assertEquals("mock:/tbl/part1/base_49", dir.getBaseDirectory().toString());
    List<Path> obsoletes = dir.getObsolete();
    assertEquals(5, obsoletes.size());
    Set<String> obsoletePathNames = new HashSet<String>();
    for (Path obsolete : obsoletes) {
      obsoletePathNames.add(obsolete.toString());
    }
    assertTrue(obsoletePathNames.contains("mock:/tbl/part1/base_5"));
    assertTrue(obsoletePathNames.contains("mock:/tbl/part1/base_10"));
    assertTrue(obsoletePathNames.contains("mock:/tbl/part1/delta_025_030"));
    assertTrue(obsoletePathNames.contains("mock:/tbl/part1/delta_025_025"));
    assertTrue(obsoletePathNames.contains("mock:/tbl/part1/delta_029_029"));
    assertEquals(0, dir.getOriginalFiles().size());
    List<AcidUtils.ParsedDelta> deltas = dir.getCurrentDirectories();
    assertEquals(1, deltas.size());
    AcidUtils.ParsedDelta delt = deltas.get(0);
    assertEquals("mock:/tbl/part1/delta_050_105", delt.getPath().toString());
    assertEquals(50, delt.getMinWriteId());
    assertEquals(105, delt.getMaxWriteId());
  }

  @Test
  public void testRecursiveDirListingIsReusedWhenSnapshotTrue() throws IOException {
    Configuration conf = new Configuration();
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/tbl/part1/base_0/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/base_0/_orc_acid_version", 10, new byte[0]));
    conf.set(ValidTxnList.VALID_TXNS_KEY,
        new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
    AcidDirectory dir = AcidUtils.getAcidState(fs, new MockPath(fs, "mock:/tbl/part1"), conf,
        new ValidReaderWriteIdList(), null, false);
    assertEquals("mock:/tbl/part1/base_0", dir.getBaseDirectory().toString());
    assertEquals(0, dir.getObsolete().size());
    assertEquals(0, dir.getOriginalFiles().size());
    assertEquals(0, dir.getCurrentDirectories().size());
    assertEquals(0, fs.getNumOpenFileCalls());
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
    conf.set(ValidTxnList.VALID_TXNS_KEY,
        new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
    AcidDirectory dir = AcidUtils.getAcidState(fs, part, conf, new ValidReaderWriteIdList("tbl:150:"
        + Long.MAX_VALUE + ":"), null, false);
    // Obsolete list should include the two original bucket files, and the old base dir
    List<Path> obsoletes = dir.getObsolete();
    assertEquals(3, obsoletes.size());
    assertEquals("mock:/tbl/part1/base_5", obsoletes.get(0).toString());
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
        new MockFile("mock:/tbl/part1/base_50/bucket_0", 500, new byte[0]));
    Path part = new MockPath(fs, "mock:/tbl/part1");
    conf.set(ValidTxnList.VALID_TXNS_KEY,
        new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
    AcidDirectory dir = AcidUtils.getAcidState(fs, part, conf, new ValidReaderWriteIdList("tbl:100:"
        + Long.MAX_VALUE + ":"), null, false);
    assertEquals("mock:/tbl/part1/base_50", dir.getBaseDirectory().toString());
    List<Path> obsolete = dir.getObsolete();
    assertEquals(2, obsolete.size());
    assertEquals("mock:/tbl/part1/delta_052_55", obsolete.get(0).toString());
    assertEquals("mock:/tbl/part1/delta_0060_60", obsolete.get(1).toString());
    List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
    assertEquals(4, delts.size());
    assertEquals("mock:/tbl/part1/delta_40_60", delts.get(0).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_00061_61", delts.get(1).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_000062_62", delts.get(2).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_0000063_63", delts.get(3).getPath().toString());
  }

  /**
   * Hive 1.3.0 delta dir naming scheme which supports multi-statement txns
   * @throws Exception
   */
  @Test
  public void testOverlapingDelta2() throws Exception {
    Configuration conf = new Configuration();
    MockFileSystem fs = new MockFileSystem(conf,
      new MockFile("mock:/tbl/part1/delta_0000063_63_0/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_000062_62_0/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_000062_62_3/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_00061_61_0/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_40_60/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_0060_60_1/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_0060_60_4/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_0060_60_7/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_052_55/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_058_58/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/base_50/bucket_0", 500, new byte[0]));
    Path part = new MockPath(fs, "mock:/tbl/part1");
    conf.set(ValidTxnList.VALID_TXNS_KEY,
        new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
    AcidDirectory dir = AcidUtils.getAcidState(fs, part, conf, new ValidReaderWriteIdList("tbl:100:"
        + Long.MAX_VALUE + ":"), null, false);
    assertEquals("mock:/tbl/part1/base_50", dir.getBaseDirectory().toString());
    List<Path> obsolete = dir.getObsolete();
    assertEquals(5, obsolete.size());
    assertEquals("mock:/tbl/part1/delta_052_55", obsolete.get(0).toString());
    assertEquals("mock:/tbl/part1/delta_058_58", obsolete.get(1).toString());
    assertEquals("mock:/tbl/part1/delta_0060_60_1", obsolete.get(2).toString());
    assertEquals("mock:/tbl/part1/delta_0060_60_4", obsolete.get(3).toString());
    assertEquals("mock:/tbl/part1/delta_0060_60_7", obsolete.get(4).toString());
    List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
    assertEquals(5, delts.size());
    assertEquals("mock:/tbl/part1/delta_40_60", delts.get(0).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_00061_61_0", delts.get(1).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_000062_62_0", delts.get(2).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_000062_62_3", delts.get(3).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_0000063_63_0", delts.get(4).getPath().toString());
  }

  @Test
  public void deltasWithOpenTxnInRead() throws Exception {
    Configuration conf = new Configuration();
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/tbl/part1/delta_1_1/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_2_5/bucket_0", 500, new byte[0]));
    Path part = new MockPath(fs, "mock:/tbl/part1");
    //hypothetically, txn 50 is open and writing write ID 4
    conf.set(ValidTxnList.VALID_TXNS_KEY,
        new ValidReadTxnList(new long[] {50}, new BitSet(), 1000, 55).writeToString());
    AcidDirectory dir = AcidUtils.getAcidState(fs, part, conf, new ValidReaderWriteIdList("tbl:100:4:4"), null,
        false);
    List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
    assertEquals(2, delts.size());
    assertEquals("mock:/tbl/part1/delta_1_1", delts.get(0).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_2_5", delts.get(1).getPath().toString());
  }

  /**
   * @since 1.3.0
   * @throws Exception
   */
  @Test
  public void deltasWithOpenTxnInRead2() throws Exception {
    Configuration conf = new Configuration();
    MockFileSystem fs = new MockFileSystem(conf,
      new MockFile("mock:/tbl/part1/delta_1_1/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_2_5/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_4_4_1/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_4_4_3/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_101_101_1/bucket_0", 500, new byte[0]));
    Path part = new MockPath(fs, "mock:/tbl/part1");
    //hypothetically, txn 50 is open and writing write ID 4
    conf.set(ValidTxnList.VALID_TXNS_KEY,
        new ValidReadTxnList(new long[] {50}, new BitSet(), 1000, 55).writeToString());
    AcidDirectory dir = AcidUtils.getAcidState(fs, part, conf, new ValidReaderWriteIdList("tbl:100:4:4"), null,
        false);
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
    conf.set(ValidTxnList.VALID_TXNS_KEY,
        new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
    AcidDirectory dir = AcidUtils.getAcidState(fs, part, conf, new ValidCompactorWriteIdList("tbl:4:"
        + Long.MAX_VALUE), null, false);
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
    conf.set(ValidTxnList.VALID_TXNS_KEY,
        new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
    AcidDirectory dir = AcidUtils.getAcidState(fs, part, conf, new ValidCompactorWriteIdList("tbl:3:"
        + Long.MAX_VALUE), null, false);
    List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
    assertEquals(1, delts.size());
    assertEquals("mock:/tbl/part1/delta_1_1", delts.get(0).getPath().toString());
  }

  @Test
  public void testBaseWithDeleteDeltas() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(HiveConf.ConfVars.HIVE_TXN_OPERATIONAL_PROPERTIES.varname,
        AcidOperationalProperties.getDefault().toInt());
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/tbl/part1/base_5/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/base_10/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/base_49/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_025_025/bucket_0", 0, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_029_029/bucket_0", 0, new byte[0]),
        new MockFile("mock:/tbl/part1/delete_delta_029_029/bucket_0", 0, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_025_030/bucket_0", 0, new byte[0]),
        new MockFile("mock:/tbl/part1/delete_delta_025_030/bucket_0", 0, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_050_105/bucket_0", 0, new byte[0]),
        new MockFile("mock:/tbl/part1/delete_delta_050_105/bucket_0", 0, new byte[0]),
        new MockFile("mock:/tbl/part1/delete_delta_110_110/bucket_0", 0, new byte[0]));
    conf.set(ValidTxnList.VALID_TXNS_KEY,
        new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
    AcidDirectory dir = AcidUtils.getAcidState(fs, new MockPath(fs, "mock:/tbl/part1"), conf,
        new ValidReaderWriteIdList("tbl:100:" + Long.MAX_VALUE + ":"), null, false);
    assertEquals("mock:/tbl/part1/base_49", dir.getBaseDirectory().toString());
    List<Path> obsoletes = dir.getObsolete();
    assertEquals(7, obsoletes.size());
    Set<String> obsoletePathNames = new HashSet<String>();
    for (Path obsolete : obsoletes) {
      obsoletePathNames.add(obsolete.toString());
    }
    assertTrue(obsoletePathNames.contains("mock:/tbl/part1/base_5"));
    assertTrue(obsoletePathNames.contains("mock:/tbl/part1/base_10"));
    assertTrue(obsoletePathNames.contains("mock:/tbl/part1/delete_delta_025_030"));
    assertTrue(obsoletePathNames.contains("mock:/tbl/part1/delta_025_030"));
    assertTrue(obsoletePathNames.contains("mock:/tbl/part1/delta_025_025"));
    assertTrue(obsoletePathNames.contains("mock:/tbl/part1/delete_delta_029_029"));
    assertTrue(obsoletePathNames.contains("mock:/tbl/part1/delta_029_029"));
    assertEquals(0, dir.getOriginalFiles().size());
    List<AcidUtils.ParsedDelta> deltas = dir.getCurrentDirectories();
    assertEquals(2, deltas.size());
    assertEquals("mock:/tbl/part1/delete_delta_050_105", deltas.get(0).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_050_105", deltas.get(1).getPath().toString());
    // The delete_delta_110_110 should not be read because it is greater than the high watermark.
  }

  @Test
  public void testOverlapingDeltaAndDeleteDelta() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(HiveConf.ConfVars.HIVE_TXN_OPERATIONAL_PROPERTIES.varname,
        AcidOperationalProperties.getDefault().toInt());
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/tbl/part1/delta_0000063_63/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_000062_62/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_00061_61/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delete_delta_00064_64/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_40_60/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delete_delta_40_60/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_0060_60/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_052_55/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delete_delta_052_55/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/base_50/bucket_0", 500, new byte[0]));
    Path part = new MockPath(fs, "mock:/tbl/part1");
    conf.set(ValidTxnList.VALID_TXNS_KEY,
        new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
    AcidDirectory dir = AcidUtils.getAcidState(fs, part, conf, new ValidReaderWriteIdList("tbl:100:"
        + Long.MAX_VALUE + ":"), null, false);
    assertEquals("mock:/tbl/part1/base_50", dir.getBaseDirectory().toString());
    List<Path> obsolete = dir.getObsolete();
    assertEquals(3, obsolete.size());
    assertEquals("mock:/tbl/part1/delete_delta_052_55", obsolete.get(0).toString());
    assertEquals("mock:/tbl/part1/delta_052_55", obsolete.get(1).toString());
    assertEquals("mock:/tbl/part1/delta_0060_60", obsolete.get(2).toString());
    List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
    assertEquals(6, delts.size());
    assertEquals("mock:/tbl/part1/delete_delta_40_60", delts.get(0).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_40_60", delts.get(1).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_00061_61", delts.get(2).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_000062_62", delts.get(3).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_0000063_63", delts.get(4).getPath().toString());
    assertEquals("mock:/tbl/part1/delete_delta_00064_64", delts.get(5).getPath().toString());
  }

  @Test
  public void testMinorCompactedDeltaMakesInBetweenDelteDeltaObsolete() throws Exception {
    // This test checks that if we have a minor compacted delta for the txn range [40,60]
    // then it will make any delete delta in that range as obsolete.
    Configuration conf = new Configuration();
    conf.setInt(HiveConf.ConfVars.HIVE_TXN_OPERATIONAL_PROPERTIES.varname,
        AcidUtils.AcidOperationalProperties.getDefault().toInt());
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/tbl/part1/delta_40_60/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delete_delta_50_50/bucket_0", 500, new byte[0]));
    Path part = new MockPath(fs, "mock:/tbl/part1");
    conf.set(ValidTxnList.VALID_TXNS_KEY,
        new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
    AcidDirectory dir = AcidUtils.getAcidState(fs, part, conf, new ValidReaderWriteIdList("tbl:100:"
        + Long.MAX_VALUE + ":"), null, false);
    List<Path> obsolete = dir.getObsolete();
    assertEquals(1, obsolete.size());
    assertEquals("mock:/tbl/part1/delete_delta_50_50", obsolete.get(0).toString());
    List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
    assertEquals(1, delts.size());
    assertEquals("mock:/tbl/part1/delta_40_60", delts.get(0).getPath().toString());
  }

  @Test
  public void deltasAndDeleteDeltasWithOpenTxnsNotInCompact() throws Exception {
    // This tests checks that appropriate delta and delete_deltas are included when minor
    // compactions specifies a valid open txn range.
    Configuration conf = new Configuration();
    conf.setInt(HiveConf.ConfVars.HIVE_TXN_OPERATIONAL_PROPERTIES.varname,
        AcidUtils.AcidOperationalProperties.getDefault().toInt());
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/tbl/part1/delta_1_1/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delete_delta_2_2/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_2_5/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delete_delta_2_5/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_2_5/bucket_0" + AcidUtils.DELTA_SIDE_FILE_SUFFIX, 500,
            new byte[0]),
        new MockFile("mock:/tbl/part1/delete_delta_7_7/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_6_10/bucket_0", 500, new byte[0]));
    Path part = new MockPath(fs, "mock:/tbl/part1");
    conf.set(ValidTxnList.VALID_TXNS_KEY,
        new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
    AcidDirectory dir = AcidUtils.getAcidState(fs, part, conf, new ValidCompactorWriteIdList("tbl:4:"
        + Long.MAX_VALUE + ":"), null, false);
    List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
    assertEquals(2, delts.size());
    assertEquals("mock:/tbl/part1/delta_1_1", delts.get(0).getPath().toString());
    assertEquals("mock:/tbl/part1/delete_delta_2_2", delts.get(1).getPath().toString());
  }

  @Test
  public void deleteDeltasWithOpenTxnInRead() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(HiveConf.ConfVars.HIVE_TXN_OPERATIONAL_PROPERTIES.varname,
        AcidUtils.AcidOperationalProperties.getDefault().toInt());
    MockFileSystem fs = new MockFileSystem(conf,
      new MockFile("mock:/tbl/part1/delta_1_1/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_2_5/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delete_delta_2_5/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delete_delta_3_3/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_4_4_1/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_4_4_3/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_101_101_1/bucket_0", 500, new byte[0]));
    Path part = new MockPath(fs, "mock:/tbl/part1");
    //hypothetically, txn 50 is open and writing write ID 4
    conf.set(ValidTxnList.VALID_TXNS_KEY,
        new ValidReadTxnList(new long[] {50}, new BitSet(), 1000, 55).writeToString());
    AcidDirectory dir = AcidUtils.getAcidState(fs, part, conf, new ValidReaderWriteIdList("tbl:100:4:4"), null,
        false);
    List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
    assertEquals(3, delts.size());
    assertEquals("mock:/tbl/part1/delta_1_1", delts.get(0).getPath().toString());
    assertEquals("mock:/tbl/part1/delete_delta_2_5", delts.get(1).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_2_5", delts.get(2).getPath().toString());
    // Note that delete_delta_3_3 should not be read, when a minor compacted
    // [delete_]delta_2_5 is present.
  }

  @Test
  public void testDeleteDeltaSubdirPathGeneration() throws Exception {
    String deleteDeltaSubdirPath = AcidUtils.deleteDeltaSubdir(1, 10);
    assertEquals("delete_delta_0000001_0000010", deleteDeltaSubdirPath);
    deleteDeltaSubdirPath = AcidUtils.deleteDeltaSubdir(1, 10, 5);
    assertEquals("delete_delta_0000001_0000010_0005", deleteDeltaSubdirPath);
  }

  @Test
  public void testDeleteEventDeltaDirPathFilter() throws Exception {
    Path positivePath = new Path("delete_delta_000001_000010");
    Path negativePath = new Path("delta_000001_000010");
    assertEquals(true, AcidUtils.deleteEventDeltaDirFilter.accept(positivePath));
    assertEquals(false, AcidUtils.deleteEventDeltaDirFilter.accept(negativePath));
  }

  @Test
  public void testAcidOperationalProperties() throws Exception {
    AcidUtils.AcidOperationalProperties testObj = AcidUtils.AcidOperationalProperties.getDefault();
    assertsForAcidOperationalProperties(testObj, "default");

    testObj = AcidUtils.AcidOperationalProperties.parseInt(1);
    assertsForAcidOperationalProperties(testObj, "split_update");

    testObj = AcidUtils.AcidOperationalProperties.parseString("default");
    assertsForAcidOperationalProperties(testObj, "default");

  }

  private void assertsForAcidOperationalProperties(AcidUtils.AcidOperationalProperties testObj,
      String type) throws Exception {
    switch(type) {
      case "split_update":
      case "default":
        assertEquals(true, testObj.isSplitUpdate());
        assertEquals(false, testObj.isHashBasedMerge());
        assertEquals(1, testObj.toInt());
        assertEquals("|split_update", testObj.toString());
        break;
      default:
        break;
    }
  }

  @Test
  public void testAcidOperationalPropertiesSettersAndGetters() throws Exception {
    AcidUtils.AcidOperationalProperties oprProps = AcidUtils.AcidOperationalProperties.getDefault();
    Configuration testConf = new Configuration();
    // Test setter for configuration object.
    AcidUtils.setAcidOperationalProperties(testConf, true, oprProps);
    assertEquals(1, testConf.getInt(HiveConf.ConfVars.HIVE_TXN_OPERATIONAL_PROPERTIES.varname, -1));
    // Test getter for configuration object.
    assertEquals(oprProps.toString(), AcidUtils.getAcidOperationalProperties(testConf).toString());

    Map<String, String> parameters = new HashMap<String, String>();
    // Test setter for map object.
    AcidUtils.setAcidOperationalProperties(parameters, true, oprProps);
    assertEquals(oprProps.toString(),
        parameters.get(HiveConf.ConfVars.HIVE_TXN_OPERATIONAL_PROPERTIES.varname));
    // Test getter for map object.
    assertEquals(1, AcidUtils.getAcidOperationalProperties(parameters).toInt());
    parameters.put(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES, oprProps.toString());
    // Set the appropriate key in the map and test that we are able to read it back correctly.
    assertEquals(1, AcidUtils.getAcidOperationalProperties(parameters).toInt());
  }

  /**
   * See {@link TestOrcRawRecordMerger#testGetLogicalLength()}
   */
  @Test
  public void testGetLogicalLength() throws Exception {
  }
  
  @Test
  public void testTableIsSoftDeleteCompliant(){
    Table table = new Table("dummy", "test_acid");
    table.setTableType(TableType.MANAGED_TABLE);
    
    HiveConf conf = new HiveConf();
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED, true);
    
    Map<String, String> parameters = new HashMap<>();
    parameters.put(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, "true");
    parameters.put(SOFT_DELETE_TABLE, "true");
    table.setParameters(parameters);
    Assert.assertTrue(AcidUtils.isTableSoftDeleteEnabled(table, conf));

    parameters.remove(SOFT_DELETE_TABLE);
    Assert.assertFalse(AcidUtils.isTableSoftDeleteEnabled(table, conf));
  }

  @Test
  public void testShouldGetHDFSSnapShots() throws Exception {
    MockFileSystem fs = new MockFileSystem(new HiveConf(),
        new MockFile("mock:/tbl/part1/.hive-staging_dir/-ext-10002", 500, new byte[0]),
        new MockFile("mock:/tbl/part2/.hive-staging_dir", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/_tmp_space.db", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_1_1/bucket-0000-0000", 500, new byte[0]));
    Path path = new MockPath(fs, "/tbl");

    Map<Path, AcidUtils.HdfsDirSnapshot> hdfsDirSnapshots = AcidUtils.getHdfsDirSnapshots(fs, path);
    assertEquals(1, hdfsDirSnapshots.size());
    assertEquals("mock:/tbl/part1/delta_1_1", hdfsDirSnapshots.keySet().stream().findFirst().get().toString());
  }

  @Test
  public void testShouldNotThrowFNFEWhenHiveStagingDirectoryIsRemovedWhileFetchingHDFSSnapshots() throws Exception {
    MockFileSystem fs = new MockFileSystem(new HiveConf(),
        new MockFile("mock:/tbl/part1/.hive-staging_dir/-ext-10002", 500, new byte[0]),
        new MockFile("mock:/tbl/part2/.hive-staging_dir", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/_tmp_space.db", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_1_1/bucket-0000-0000", 500, new byte[0]));
    Path path = new MockPath(fs, "/tbl");
    Path stageDir = new MockPath(fs, "mock:/tbl/part1/.hive-staging_dir");
    FileSystem mockFs = spy(fs);
    Mockito.doThrow(new FileNotFoundException("")).when(mockFs).listLocatedStatus(stageDir);
    try {
      Map<Path, AcidUtils.HdfsDirSnapshot> hdfsDirSnapshots = AcidUtils.getHdfsDirSnapshots(mockFs, path);
      assertEquals(1, hdfsDirSnapshots.size());
      assertEquals("mock:/tbl/part1/delta_1_1", hdfsDirSnapshots.keySet().stream().findFirst().get().toString());
    }
    catch (FileNotFoundException fnf) {
      Assert.fail("Should not throw FileNotFoundException when a directory is removed while fetching HDFSSnapshots");
    }
  }
}
