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
import org.apache.hadoop.hive.common.ValidCompactorWriteIdList;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidUtils.AcidOperationalProperties;
import org.apache.hadoop.hive.ql.io.orc.TestInputOutputFormat.MockPath;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.shims.HadoopShims.HdfsFileStatusWithId;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestAcidUtilsWithHmsCacheEnabled {

  private FileSystem fs;
  private String dbPath;

  @Before
  public void setUp() throws IOException, HiveException, TException{
    HiveConf conf = new HiveConf();

    Path whPath = new Path(conf.getVar(HiveConf.ConfVars.METASTOREWAREHOUSE));
    fs = whPath.getFileSystem(conf);
    Database db = new DatabaseBuilder().
            setName("db").
            setLocation(whPath.toString()).build(conf);

    dbPath = db.getLocationUri();
    Hive.get().getMSC().createDatabase(db);

    Table table = new TableBuilder()
            .inDb(db)
            .setTableName("tbl")
            .addCol("id", "int")
            .addPartCol("part1", "string").build(conf);

    Hive.get().getMSC().createTable(table);

    Partition p = new PartitionBuilder()
            .inTable(table)
            .addValue("0").build(conf);

    Hive.get().getMSC().add_partition(p);
  }

  @After
  public void cleanUp() throws IOException, HiveException, TException {
    fs.delete(new Path(dbPath), true);
    fs.close();
    Hive.get().getMSC().dropDatabase("db", true,true, true);
  }

  @Test
  public void testOriginal() throws Exception {
    Configuration conf = new Configuration();
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/000000_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/000000_0" + Utilities.COPY_KEYWORD + "1"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/000000_0" + Utilities.COPY_KEYWORD + "2"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/000001_1"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/000002_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/random"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/_done"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/subdir/000000_0"));

    AcidUtils.Directory dir = AcidUtils.getAcidState(fs, new Path(dbPath + "/db.db/tbl/part1=0"), conf,
            new ValidReaderWriteIdList("db.tbl:100:" + Long.MAX_VALUE + ":"), null, false);
    assertEquals(null, dir.getBaseDirectory());
    assertEquals(0, dir.getCurrentDirectories().size());
    assertEquals(0, dir.getObsolete().size());
    List<HdfsFileStatusWithId> result = dir.getOriginalFiles();
    assertEquals(7, result.size());
    assertEquals(dbPath + "/db.db/tbl/part1=0/000000_0", result.get(0).getFileStatus().getPath().toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/000000_0" + Utilities.COPY_KEYWORD + "1",
            result.get(1).getFileStatus().getPath().toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/000000_0" + Utilities.COPY_KEYWORD + "2",
            result.get(2).getFileStatus().getPath().toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/000001_1", result.get(3).getFileStatus().getPath().toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/000002_0", result.get(4).getFileStatus().getPath().toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/random", result.get(5).getFileStatus().getPath().toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/subdir/000000_0",
            result.get(6).getFileStatus().getPath().toString());
  }

  @Test
  public void testOriginalDeltas() throws Exception {
    Configuration conf = new Configuration();
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/000000_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/000001_1"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/000002_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/random"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/_done"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/subdir/000000_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_025_025/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_029_029/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_025_030/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_050_100/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_101_101/bucket_0"));

    conf.set(ValidTxnList.VALID_TXNS_KEY,
            new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
    AcidUtils.Directory dir = AcidUtils.getAcidState(fs, new Path(dbPath + "/db.db/tbl/part1=0"), conf,
            new ValidReaderWriteIdList("db.tbl:100:" + Long.MAX_VALUE + ":"), null, false);
    assertEquals(null, dir.getBaseDirectory());
    List<Path> obsolete = dir.getObsolete();
    assertEquals(2, obsolete.size());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_025_025",
            obsolete.get(0).toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_029_029",
            obsolete.get(1).toString());
    List<HdfsFileStatusWithId> result = dir.getOriginalFiles();
    assertEquals(5, result.size());
    assertEquals(dbPath + "/db.db/tbl/part1=0/000000_0", result.get(0).getFileStatus().getPath().toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/000001_1", result.get(1).getFileStatus().getPath().toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/000002_0", result.get(2).getFileStatus().getPath().toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/random", result.get(3).getFileStatus().getPath().toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/subdir/000000_0",
            result.get(4).getFileStatus().getPath().toString());
    List<AcidUtils.ParsedDelta> deltas = dir.getCurrentDirectories();
    assertEquals(2, deltas.size());
    AcidUtils.ParsedDelta delt = deltas.get(0);
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_025_030", delt.getPath().toString());
    assertEquals(25, delt.getMinWriteId());
    assertEquals(30, delt.getMaxWriteId());
    delt = deltas.get(1);
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_050_100", delt.getPath().toString());
    assertEquals(50, delt.getMinWriteId());
    assertEquals(100, delt.getMaxWriteId());
  }

  @Test
  public void testBaseDeltas() throws Exception {
    Configuration conf = new Configuration();
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/base_5/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/base_10/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/base_49/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_025_025/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_029_029/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_025_030/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_050_105/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_90_120/bucket_0"));

    conf.set(ValidTxnList.VALID_TXNS_KEY,
            new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
    AcidUtils.Directory dir = AcidUtils.getAcidState(fs, new Path(dbPath + "/db.db/tbl/part1=0"), conf,
            new ValidReaderWriteIdList("db.tbl:100:" + Long.MAX_VALUE + ":"), null, false);
    assertEquals(dbPath + "/db.db/tbl/part1=0/base_49", dir.getBaseDirectory().toString());
    List<Path> obsoletes = dir.getObsolete();
    assertEquals(5, obsoletes.size());
    Set<String> obsoletePathNames = new HashSet<String>();
    for (Path obsolete : obsoletes) {
      obsoletePathNames.add(obsolete.toString());
    }
    assertTrue(obsoletePathNames.contains(dbPath + "/db.db/tbl/part1=0/base_5"));
    assertTrue(obsoletePathNames.contains(dbPath + "/db.db/tbl/part1=0/base_10"));
    assertTrue(obsoletePathNames.contains(dbPath + "/db.db/tbl/part1=0/delta_025_030"));
    assertTrue(obsoletePathNames.contains(dbPath + "/db.db/tbl/part1=0/delta_025_025"));
    assertTrue(obsoletePathNames.contains(dbPath + "/db.db/tbl/part1=0/delta_029_029"));
    assertEquals(0, dir.getOriginalFiles().size());
    List<AcidUtils.ParsedDelta> deltas = dir.getCurrentDirectories();
    assertEquals(1, deltas.size());
    AcidUtils.ParsedDelta delt = deltas.get(0);
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_050_105", delt.getPath().toString());
    assertEquals(50, delt.getMinWriteId());
    assertEquals(105, delt.getMaxWriteId());
  }

  @Test
  public void testObsoleteOriginals() throws Exception {
    Configuration conf = new Configuration();
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/base_10/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/base_5/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/000000_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/000001_1"));
    Path part = new Path(dbPath + "/db.db/tbl/part1=0");
    conf.set(ValidTxnList.VALID_TXNS_KEY,
            new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
    AcidUtils.Directory dir = AcidUtils.getAcidState(fs, part, conf, new ValidReaderWriteIdList("db.tbl:150:"
            + Long.MAX_VALUE + ":"), null, false);
    // Obsolete list should include the two original bucket files, and the old base dir
    List<Path> obsoletes = dir.getObsolete();
    assertEquals(3, obsoletes.size());
    assertEquals(dbPath + "/db.db/tbl/part1=0/base_5", obsoletes.get(0).toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/base_10", dir.getBaseDirectory().toString());
  }

  @Test
  public void testOverlapingDelta() throws Exception {
    Configuration conf = new Configuration();
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_0000063_63/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_0000062_62/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_0000061_61/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_40_60/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_0060_60/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_052_55/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/base_50/bucket_0"));
    Path part = new Path(dbPath + "/db.db/tbl/part1=0");
    conf.set(ValidTxnList.VALID_TXNS_KEY,
            new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
    AcidUtils.Directory dir = AcidUtils.getAcidState(fs, part, conf, new ValidReaderWriteIdList("db.tbl:100:"
            + Long.MAX_VALUE + ":"), null, false);
    assertEquals(dbPath + "/db.db/tbl/part1=0/base_50", dir.getBaseDirectory().toString());
    List<Path> obsolete = dir.getObsolete();
    assertEquals(2, obsolete.size());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_052_55", obsolete.get(0).toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_0060_60", obsolete.get(1).toString());
    List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
    assertEquals(4, delts.size());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_40_60", delts.get(0).getPath().toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_0000061_61", delts.get(1).getPath().toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_0000062_62", delts.get(2).getPath().toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_0000063_63", delts.get(3).getPath().toString());
  }

  /**
   * Hive 1.3.0 delta dir naming scheme which supports multi-statement txns
   *
   * @throws Exception
   */
  @Test
  public void testOverlapingDelta2() throws Exception {
    Configuration conf = new Configuration();
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_0000063_63/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_000062_62_0/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_000062_62_3/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_00061_61_0/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_40_60/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_0060_60_1/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_0060_60_4/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_0060_60_7/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_052_55/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_058_58/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/base_50/bucket_0"));
    Path part = new Path(dbPath + "/db.db/tbl/part1=0");

    conf.set(ValidTxnList.VALID_TXNS_KEY,
            new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
    AcidUtils.Directory dir = AcidUtils.getAcidState(fs, part, conf, new ValidReaderWriteIdList("db.tbl:100:"
            + Long.MAX_VALUE + ":"), null, false);
    assertEquals(dbPath + "/db.db/tbl/part1=0/base_50", dir.getBaseDirectory().toString());
    List<Path> obsolete = dir.getObsolete();
    assertEquals(5, obsolete.size());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_052_55", obsolete.get(0).toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_058_58", obsolete.get(1).toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_0060_60_1", obsolete.get(2).toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_0060_60_4", obsolete.get(3).toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_0060_60_7", obsolete.get(4).toString());
    List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
    assertEquals(5, delts.size());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_40_60", delts.get(0).getPath().toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_00061_61_0", delts.get(1).getPath().toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_000062_62_0", delts.get(2).getPath().toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_000062_62_3", delts.get(3).getPath().toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_0000063_63", delts.get(4).getPath().toString());
  }

  @Test
  public void deltasWithOpenTxnInRead() throws Exception {
    Configuration conf = new Configuration();
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_1_1/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_2_5/bucket_0"));
    Path part = new Path(dbPath + "/db.db/tbl/part1=0");
    //hypothetically, txn 50 is open and writing write ID 4
    conf.set(ValidTxnList.VALID_TXNS_KEY,
            new ValidReadTxnList(new long[]{50}, new BitSet(), 1000, 55).writeToString());
    AcidUtils.Directory dir = AcidUtils.getAcidState(fs, part, conf, new ValidReaderWriteIdList("db.tbl:100:4:4"), null,
            false);
    List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
    assertEquals(2, delts.size());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_1_1", delts.get(0).getPath().toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_2_5", delts.get(1).getPath().toString());
  }

  /**
   * @throws Exception
   * @since 1.3.0
   */
  @Test
  public void deltasWithOpenTxnInRead2() throws Exception {
    Configuration conf = new Configuration();
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_1_1/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_2_5/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_4_4_1/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_4_4_3/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_101_101_1/bucket_0"));

    Path part = new Path(dbPath + "/db.db/tbl/part1=0");
    //hypothetically, txn 50 is open and writing write ID 4
    conf.set(ValidTxnList.VALID_TXNS_KEY,
            new ValidReadTxnList(new long[]{50}, new BitSet(), 1000, 55).writeToString());
    AcidUtils.Directory dir = AcidUtils.getAcidState(fs, part, conf, new ValidReaderWriteIdList("db.tbl:100:4:4"), null,
            false);
    List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
    assertEquals(2, delts.size());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_1_1", delts.get(0).getPath().toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_2_5", delts.get(1).getPath().toString());
  }

  @Test
  public void deltasWithOpenTxnsNotInCompact() throws Exception {
    Configuration conf = new Configuration();
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_1_1/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_2_5/bucket_0"));
    Path part = new Path(dbPath + "/db.db/tbl/part1=0");
    conf.set(ValidTxnList.VALID_TXNS_KEY,
            new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
    AcidUtils.Directory dir = AcidUtils.getAcidState(fs, part, conf, new ValidCompactorWriteIdList("db.tbl:4:"
            + Long.MAX_VALUE), null, false);
    List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
    assertEquals(1, delts.size());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_1_1", delts.get(0).getPath().toString());
  }

  @Test
  public void deltasWithOpenTxnsNotInCompact2() throws Exception {
    Configuration conf = new Configuration();
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_1_1/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_2_5/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_2_5/bucket_0" + AcidUtils.DELTA_SIDE_FILE_SUFFIX));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_6_10/bucket_0"));
    Path part = new Path(dbPath + "/db.db/tbl/part1=0");

    conf.set(ValidTxnList.VALID_TXNS_KEY,
            new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
    AcidUtils.Directory dir = AcidUtils.getAcidState(fs, part, conf, new ValidCompactorWriteIdList("db.tbl:3:"
            + Long.MAX_VALUE), null, false);
    List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
    assertEquals(1, delts.size());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_1_1", delts.get(0).getPath().toString());
  }

  @Test
  public void testBaseWithDeleteDeltas() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(HiveConf.ConfVars.HIVE_TXN_OPERATIONAL_PROPERTIES.varname,
            AcidOperationalProperties.getDefault().toInt());
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/base_5/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/base_10/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/base_49/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_025_025/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_029_029/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delete_delta_029_029/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_025_030/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delete_delta_025_030/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_050_105/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delete_delta_050_105/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delete_delta_110_110/bucket_0"));

    conf.set(ValidTxnList.VALID_TXNS_KEY,
            new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
    AcidUtils.Directory dir = AcidUtils.getAcidState(fs, new MockPath(fs, dbPath + "/db.db/tbl/part1=0"), conf,
            new ValidReaderWriteIdList("db.tbl:100:" + Long.MAX_VALUE + ":"), null, false);
    assertEquals(dbPath + "/db.db/tbl/part1=0/base_49", dir.getBaseDirectory().toString());
    List<Path> obsoletes = dir.getObsolete();
    assertEquals(7, obsoletes.size());
    Set<String> obsoletePathNames = new HashSet<String>();
    for (Path obsolete : obsoletes) {
      obsoletePathNames.add(obsolete.toString());
    }
    assertTrue(obsoletePathNames.contains(dbPath + "/db.db/tbl/part1=0/base_5"));
    assertTrue(obsoletePathNames.contains(dbPath + "/db.db/tbl/part1=0/base_10"));
    assertTrue(obsoletePathNames.contains(dbPath + "/db.db/tbl/part1=0/delete_delta_025_030"));
    assertTrue(obsoletePathNames.contains(dbPath + "/db.db/tbl/part1=0/delta_025_030"));
    assertTrue(obsoletePathNames.contains(dbPath + "/db.db/tbl/part1=0/delta_025_025"));
    assertTrue(obsoletePathNames.contains(dbPath + "/db.db/tbl/part1=0/delete_delta_029_029"));
    assertTrue(obsoletePathNames.contains(dbPath + "/db.db/tbl/part1=0/delta_029_029"));
    assertEquals(0, dir.getOriginalFiles().size());
    List<AcidUtils.ParsedDelta> deltas = dir.getCurrentDirectories();
    assertEquals(2, deltas.size());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delete_delta_050_105", deltas.get(0).getPath().toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_050_105", deltas.get(1).getPath().toString());
    // The delete_delta_110_110 should not be read because it is greater than the high watermark.
  }

  @Test
  public void testOverlapingDeltaAndDeleteDelta() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(HiveConf.ConfVars.HIVE_TXN_OPERATIONAL_PROPERTIES.varname,
            AcidOperationalProperties.getDefault().toInt());
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_0000063_63/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_000062_62/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_00061_61/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delete_delta_00064_64/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_40_60/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delete_delta_40_60/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_0060_60/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_052_55/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delete_delta_052_55/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/base_50/bucket_0"));
    Path part = new Path(dbPath + "/db.db/tbl/part1=0");

    conf.set(ValidTxnList.VALID_TXNS_KEY,
            new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
    AcidUtils.Directory dir = AcidUtils.getAcidState(fs, part, conf, new ValidReaderWriteIdList("db.tbl:100:"
            + Long.MAX_VALUE + ":"), null, false);
    assertEquals(dbPath + "/db.db/tbl/part1=0/base_50", dir.getBaseDirectory().toString());
    List<Path> obsolete = dir.getObsolete();
    assertEquals(3, obsolete.size());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delete_delta_052_55", obsolete.get(0).toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_052_55", obsolete.get(1).toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_0060_60", obsolete.get(2).toString());
    List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
    assertEquals(6, delts.size());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delete_delta_40_60", delts.get(0).getPath().toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_40_60", delts.get(1).getPath().toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_00061_61", delts.get(2).getPath().toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_000062_62", delts.get(3).getPath().toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_0000063_63", delts.get(4).getPath().toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delete_delta_00064_64", delts.get(5).getPath().toString());
  }

  @Test
  public void testMinorCompactedDeltaMakesInBetweenDelteDeltaObsolete() throws Exception {
    // This test checks that if we have a minor compacted delta for the txn range [40,60]
    // then it will make any delete delta in that range as obsolete.
    Configuration conf = new Configuration();
    conf.setInt(HiveConf.ConfVars.HIVE_TXN_OPERATIONAL_PROPERTIES.varname,
            AcidOperationalProperties.getDefault().toInt());
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_40_60/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delete_delta_50_50/bucket_0"));
    Path part = new Path(dbPath + "/db.db/tbl/part1=0");
    conf.set(ValidTxnList.VALID_TXNS_KEY,
            new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
    AcidUtils.Directory dir = AcidUtils.getAcidState(fs, part, conf, new ValidReaderWriteIdList("db.tbl:100:"
            + Long.MAX_VALUE + ":"), null, false);
    List<Path> obsolete = dir.getObsolete();
    assertEquals(1, obsolete.size());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delete_delta_50_50", obsolete.get(0).toString());
    List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
    assertEquals(1, delts.size());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_40_60", delts.get(0).getPath().toString());
  }

  @Test
  public void deltasAndDeleteDeltasWithOpenTxnsNotInCompact() throws Exception {
    // This tests checks that appropriate delta and delete_deltas are included when minor
    // compactions specifies a valid open txn range.
    Configuration conf = new Configuration();
    conf.setInt(HiveConf.ConfVars.HIVE_TXN_OPERATIONAL_PROPERTIES.varname,
            AcidOperationalProperties.getDefault().toInt());
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_1_1/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delete_delta_2_2/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_2_5/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delete_delta_2_5/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_2_5/bucket_0" + AcidUtils.DELTA_SIDE_FILE_SUFFIX));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delete_delta_7_7/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_6_10/bucket_0"));
    Path part = new Path(dbPath + "/db.db/tbl/part1=0");

    conf.set(ValidTxnList.VALID_TXNS_KEY,
            new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
    AcidUtils.Directory dir = AcidUtils.getAcidState(fs, part, conf, new ValidCompactorWriteIdList("db.tbl:4:"
            + Long.MAX_VALUE + ":"), null, false);
    List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
    assertEquals(2, delts.size());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_1_1", delts.get(0).getPath().toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delete_delta_2_2", delts.get(1).getPath().toString());
  }

  @Test
  public void deleteDeltasWithOpenTxnInRead() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(HiveConf.ConfVars.HIVE_TXN_OPERATIONAL_PROPERTIES.varname,
            AcidOperationalProperties.getDefault().toInt());
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_1_1/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_2_5/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delete_delta_2_5/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delete_delta_3_3/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_4_4_1/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_4_4_3/bucket_0"));
    fs.createNewFile(new Path(dbPath + "/db.db/tbl/part1=0/delta_101_101_1/bucket_0"));
    Path part = new Path(dbPath + "/db.db/tbl/part1=0");

    //hypothetically, txn 50 is open and writing write ID 4
    conf.set(ValidTxnList.VALID_TXNS_KEY,
            new ValidReadTxnList(new long[]{50}, new BitSet(), 1000, 55).writeToString());
    AcidUtils.Directory dir = AcidUtils.getAcidState(fs, part, conf, new ValidReaderWriteIdList("db.tbl:100:4:4"), null,
            false);
    List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
    assertEquals(3, delts.size());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_1_1", delts.get(0).getPath().toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delete_delta_2_5", delts.get(1).getPath().toString());
    assertEquals(dbPath + "/db.db/tbl/part1=0/delta_2_5", delts.get(2).getPath().toString());
    // Note that delete_delta_3_3 should not be read, when a minor compacted
    // [delete_]delta_2_5 is present.
  }
}