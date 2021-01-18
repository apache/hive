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
package org.apache.hadoop.hive.ql.metadata;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.ql.TxnCommandsBaseForTests;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils.ParsedDeltaLight;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.List;

/**
 * Tests for the MSCK REPAIR TABLE operation on transactional tables.
 */
public class TestMSCKRepairOnAcid extends TxnCommandsBaseForTests {

  private static final String TEST_DATA_DIR = new File(
      System.getProperty("java.io.tmpdir") + File.separator + TestMSCKRepairOnAcid.class.getCanonicalName()
          + "-" + System.currentTimeMillis()).getPath().replaceAll("\\\\", "/");

  private final String acidTblPartMsck = "acidtblpartmsck";

  @Override
  protected String getTestDataDir() {
    return TEST_DATA_DIR;
  }

  /**
   * A new partition copied under a table containing only deltas.
   * @throws Exception ex
   */
  @Test
  public void testAddPartitionDeltas() throws Exception {

    runStatementOnDriver("drop table if exists " + acidTblPartMsck);

    // Insert few rows
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values(1,1,'p1'),(2,2,'p1'),(3,3,'p1')");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values(1,2,'p1'),(2,3,'p1'),(3,4,'p1')");

    List<String> r = runStatementOnDriver("select a, b from " + Table.ACIDTBLPART + " where p='p1' order by a, b");
    int[][] expected = { { 1, 1 }, { 1, 2 }, { 2, 2 }, { 2, 3 }, { 3, 3 }, { 3, 4 } };
    Assert.assertEquals(stringifyValues(expected), r);
    // Create target table

    runStatementOnDriver("create table " + acidTblPartMsck
        + " (a int, b int) partitioned by (p string) clustered by (a) into 2 buckets"
        + " stored as orc TBLPROPERTIES ('transactional'='true')");

    // copy files on fs
    FileSystem fs = FileSystem.get(hiveConf);
    FileUtil.copy(fs, new Path(getWarehouseDir() + "/" + Table.ACIDTBLPART.toString().toLowerCase() + "/p=p1"), fs,
        new Path(getWarehouseDir(), acidTblPartMsck), false, hiveConf);

    FileStatus[] fileStatuses = fs.listStatus(new Path(getWarehouseDir(), acidTblPartMsck + "/p=p1"));
    Assert.assertEquals(2, fileStatuses.length);
    // call msk repair
    runStatementOnDriver("msck repair table " + acidTblPartMsck);

    r = runStatementOnDriver("select a, b from " + acidTblPartMsck + " where p='p1' order by a, b");
    Assert.assertEquals(stringifyValues(expected), r);

    runStatementOnDriver("drop table if exists " + acidTblPartMsck);
  }

  /**
   * More partition copied under a table containing only deltas.
   * @throws Exception ex
   */
  @Test
  public void testAddMultiPartitionDeltas() throws Exception {

    runStatementOnDriver("drop table if exists " + acidTblPartMsck);

    // Insert few rows
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values(1,1,'p1'),(2,2,'p1'),(3,3,'p1')");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values(4,4,'p1')");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values(1,2,'p2'),(2,3,'p2'),(3,4,'p2')");

    List<String> r = runStatementOnDriver("select a, b from " + Table.ACIDTBLPART + " order by a, b");
    int[][] expected = { { 1, 1 }, { 1, 2 }, { 2, 2 }, { 2, 3 }, { 3, 3 }, { 3, 4 }, { 4, 4 } };
    Assert.assertEquals(stringifyValues(expected), r);
    // Create target table
    runStatementOnDriver("create table " + acidTblPartMsck
        + " (a int, b int) partitioned by (p string) clustered by (a) into 2 buckets"
        + " stored as orc TBLPROPERTIES ('transactional'='true')");

    // copy files on fs
    FileSystem fs = FileSystem.get(hiveConf);
    FileUtil.copy(fs, new Path(getWarehouseDir() + "/" + Table.ACIDTBLPART.toString().toLowerCase() + "/p=p1"), fs,
        new Path(getWarehouseDir(), acidTblPartMsck), false, hiveConf);
    FileUtil.copy(fs, new Path(getWarehouseDir() + "/" + Table.ACIDTBLPART.toString().toLowerCase() + "/p=p2"), fs,
        new Path(getWarehouseDir(), acidTblPartMsck), false, hiveConf);

    // call msk repair
    runStatementOnDriver("msck repair table " + acidTblPartMsck);

    r = runStatementOnDriver("select a, b from " + acidTblPartMsck + " order by a, b");
    Assert.assertEquals(stringifyValues(expected), r);

    runStatementOnDriver("drop table if exists " + acidTblPartMsck);
  }

  /**
   * A new partition copied under a table containing only deltas, but the table already contains allocated writes,
   * and the restored partition has higher writeId.
   * @throws Exception ex
   */
  @Test
  public void testAddPartitionHighWriteIdException() throws Exception {

    runStatementOnDriver("drop table if exists " + acidTblPartMsck);

    // Insert few rows
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values(1,1,'p1'),(2,2,'p1'),(3,3,'p1')");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values(1,2,'p2'),(2,3,'p2'),(3,4,'p2')");

    List<String> r = runStatementOnDriver("select a, b from " + Table.ACIDTBLPART + " order by a, b");
    int[][] expected = { { 1, 1 }, { 1, 2 }, { 2, 2 }, { 2, 3 }, { 3, 3 }, { 3, 4 } };
    Assert.assertEquals(stringifyValues(expected), r);
    // Create target table
    runStatementOnDriver("create table " + acidTblPartMsck
        + " (a int, b int) partitioned by (p string) clustered by (a) into 2 buckets"
        + " stored as orc TBLPROPERTIES ('transactional'='true')");

    // Insert data in p1 so we allocate writeId in the msck table
    runStatementOnDriver("insert into " + acidTblPartMsck + " partition(p) values(1,1,'p1'),(2,2,'p1'),(3,3,'p1')");

    // copy files on fs
    FileSystem fs = FileSystem.get(hiveConf);
    FileUtil.copy(fs, new Path(getWarehouseDir() + "/" + Table.ACIDTBLPART.toString().toLowerCase() + "/p=p2"), fs,
        new Path(getWarehouseDir(), acidTblPartMsck), false, false, hiveConf);

    // One partition written, one copied
    FileStatus[] fileStatuses = fs.listStatus(new Path(getWarehouseDir(), acidTblPartMsck));
    Assert.assertEquals(2, fileStatuses.length);

    // call msk repair, it should fail, since it will find a delta folder with writeId 2
    // that is higher than the allocated max in the table
    CommandProcessorException e = runStatementOnDriverNegative("msck repair table " + acidTblPartMsck);
    Assert.assertEquals(-1, e.getErrorCode());

    runStatementOnDriver("drop table if exists " + acidTblPartMsck);
  }
  /**
   * A new partition copied under a table containing only deltas, but the table already contains allocated writes,
   * and the restored partition has lower writeId.
   * @throws Exception ex
   */
  @Test
  public void testAddPartitionLowerWriteId() throws Exception {

    runStatementOnDriver("drop table if exists " + acidTblPartMsck);

    // Insert few rows
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values(1,1,'p1'),(2,2,'p1'),(3,3,'p1')");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values(1,2,'p2'),(2,3,'p2'),(3,4,'p2')");

    List<String> r = runStatementOnDriver("select a, b from " + Table.ACIDTBLPART + " order by a, b");
    int[][] expected = { { 1, 1 }, { 1, 2 }, { 2, 2 }, { 2, 3 }, { 3, 3 }, { 3, 4 } };
    Assert.assertEquals(stringifyValues(expected), r);
    // Create target table
    runStatementOnDriver("create table " + acidTblPartMsck
        + " (a int, b int) partitioned by (p string) clustered by (a) into 2 buckets"
        + " stored as orc TBLPROPERTIES ('transactional'='true')");

    // Insert data in p2 so we allocate writeId in the msck table
    runStatementOnDriver("insert into " + acidTblPartMsck + " partition(p) values(1,2,'p2'),(2,3,'p2'),(3,4,'p2')");
    runStatementOnDriver("insert into " + acidTblPartMsck + " partition(p) values(4,5,'p2')");

    // copy files on fs
    FileSystem fs = FileSystem.get(hiveConf);
    FileUtil.copy(fs, new Path(getWarehouseDir() + "/" + Table.ACIDTBLPART.toString().toLowerCase() + "/p=p1"), fs,
        new Path(getWarehouseDir(), acidTblPartMsck), false, false, hiveConf);

    // One partition written, one copied
    FileStatus[] fileStatuses = fs.listStatus(new Path(getWarehouseDir(), acidTblPartMsck));
    Assert.assertEquals(2, fileStatuses.length);

    // call msk repair
    runStatementOnDriver("msck repair table " + acidTblPartMsck);

    // The allocated writeId should be 2 and the copied partition has 1 as maximum, so we are on the safe side
    r = runStatementOnDriver("select a, b from " + acidTblPartMsck + " order by a, b");
    int[][] expected2 = { { 1, 1 }, { 1, 2 }, { 2, 2 }, { 2, 3 }, { 3, 3 }, { 3, 4 }, { 4, 5 } };
    Assert.assertEquals(stringifyValues(expected2), r);

    runStatementOnDriver("drop table if exists " + acidTblPartMsck);
  }
  /**
   * A new partition copied under a table containing compactd delta.
   * @throws Exception ex
   */
  @Test
  public void testAddPartitionMinorCompacted() throws Exception {

    runStatementOnDriver("drop table if exists " + acidTblPartMsck);

    // Insert few rows
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values(1,1,'p1'),(2,2,'p1'),(3,3,'p1')");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values(4,4,'p1')");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values(1,2,'p2'),(2,3,'p2'),(3,4,'p2')");

    //run Compaction
    runStatementOnDriver("alter table " + Table.ACIDTBLPART + " partition (p='p1') compact 'minor'");
    runWorker(hiveConf);

    List<String> r = runStatementOnDriver("select a, b from " + Table.ACIDTBLPART + " order by a, b");
    int[][] expected = { { 1, 1 }, { 1, 2 }, { 2, 2 }, { 2, 3 }, { 3, 3 }, { 3, 4 }, { 4, 4 } };
    Assert.assertEquals(stringifyValues(expected), r);
    // Create target table
    runStatementOnDriver("create table " + acidTblPartMsck
        + " (a int, b int) partitioned by (p string) clustered by (a) into 2 buckets"
        + " stored as orc TBLPROPERTIES ('transactional'='true')");

    // copy files on fs
    FileSystem fs = FileSystem.get(hiveConf);
    FileUtil.copy(fs, new Path(getWarehouseDir() + "/" + Table.ACIDTBLPART.toString().toLowerCase() + "/p=p1"), fs,
        new Path(getWarehouseDir(), acidTblPartMsck), false, hiveConf);
    FileUtil.copy(fs, new Path(getWarehouseDir() + "/" + Table.ACIDTBLPART.toString().toLowerCase() + "/p=p2"), fs,
        new Path(getWarehouseDir(), acidTblPartMsck), false, hiveConf);

    FileStatus[] fileStatuses = fs.listStatus(new Path(getWarehouseDir(), acidTblPartMsck + "/p=p1"));
    // two + one delta
    Assert.assertEquals(3, fileStatuses.length);

    // call msk repair
    runStatementOnDriver("msck repair table " + acidTblPartMsck);

    r = runStatementOnDriver("select a, b from " + acidTblPartMsck + " order by a, b");
    Assert.assertEquals(stringifyValues(expected), r);

    runStatementOnDriver("drop table if exists " + acidTblPartMsck);
  }
  /**
   * A new partition copied under a table containing compacted base.
   * @throws Exception ex
   */
  @Test
  public void testAddPartitionMajorCompacted() throws Exception {

    runStatementOnDriver("drop table if exists " + acidTblPartMsck);

    // Insert few rows
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values(1,1,'p1'),(2,2,'p1'),(3,3,'p1')");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values(4,4,'p1')");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values(1,2,'p2'),(2,3,'p2'),(3,4,'p2')");

    //run Compaction
    runStatementOnDriver("alter table " + Table.ACIDTBLPART + " partition (p='p1') compact 'major'");
    runWorker(hiveConf);

    List<String> r = runStatementOnDriver("select a, b from " + Table.ACIDTBLPART + " order by a, b");
    int[][] expected = { { 1, 1 }, { 1, 2 }, { 2, 2 }, { 2, 3 }, { 3, 3 }, { 3, 4 }, { 4, 4 } };
    Assert.assertEquals(stringifyValues(expected), r);
    // Create target table
    runStatementOnDriver("create table " + acidTblPartMsck
        + " (a int, b int) partitioned by (p string) clustered by (a) into 2 buckets"
        + " stored as orc TBLPROPERTIES ('transactional'='true')");

    // copy files on fs
    FileSystem fs = FileSystem.get(hiveConf);
    FileUtil.copy(fs, new Path(getWarehouseDir() + "/" + Table.ACIDTBLPART.toString().toLowerCase() + "/p=p1"), fs,
        new Path(getWarehouseDir(), acidTblPartMsck), false, hiveConf);
    FileUtil.copy(fs, new Path(getWarehouseDir() + "/" + Table.ACIDTBLPART.toString().toLowerCase() + "/p=p2"), fs,
        new Path(getWarehouseDir(), acidTblPartMsck), false, hiveConf);

    FileStatus[] fileStatuses = fs.listStatus(new Path(getWarehouseDir(), acidTblPartMsck + "/p=p1"));
    // two delta + a base
    Assert.assertEquals(3, fileStatuses.length);

    // call msk repair
    runStatementOnDriver("msck repair table " + acidTblPartMsck);

    r = runStatementOnDriver("select a, b from " + acidTblPartMsck + " order by a, b");
    Assert.assertEquals(stringifyValues(expected), r);

    runStatementOnDriver("drop table if exists " + acidTblPartMsck);
  }

  /**
   * Delete one partition from a table, drop the partition from the HMS, then restore the partition
   * and repair the table.
   * @throws Exception ex
   */
  @Test
  public void testBackUpAndRestorePartition() throws Exception {

    // Insert few rows
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values(1,1,'p1'),(2,2,'p1'),(3,3,'p1')");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values(4,4,'p1')");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values(1,2,'p2'),(2,3,'p2'),(3,4,'p2')");

    List<String> r = runStatementOnDriver("select a, b from " + Table.ACIDTBLPART + " order by a, b");
    int[][] expected = { { 1, 1 }, { 1, 2 }, { 2, 2 }, { 2, 3 }, { 3, 3 }, { 3, 4 }, { 4, 4 } };
    Assert.assertEquals(stringifyValues(expected), r);

    // copy files on fs
    FileSystem fs = FileSystem.get(hiveConf);
    fs.mkdirs(new Path(getWarehouseDir(), "mybackup"));
    FileUtil.copy(fs, new Path(getWarehouseDir() + "/" + Table.ACIDTBLPART.toString().toLowerCase() + "/p=p1"), fs,
        new Path(getWarehouseDir(), "mybackup"), true, hiveConf);

    // call msk repair to remove partition p1
    runStatementOnDriver("msck repair table " + Table.ACIDTBLPART + " SYNC PARTITIONS");

    r = runStatementOnDriver("select a, b from " + Table.ACIDTBLPART + " order by a, b");
    int[][] expected2 = { { 1, 2 }, { 2, 3 }, { 3, 4 }};
    Assert.assertEquals(stringifyValues(expected2), r);

    // copy the data back
    FileUtil.copy(fs, new Path(getWarehouseDir() + "/" + "mybackup" + "/p=p1"), fs,
        new Path(getWarehouseDir(), Table.ACIDTBLPART.toString().toLowerCase()), true, hiveConf);

    // call msk repair to add the partition back
    runStatementOnDriver("msck repair table " + Table.ACIDTBLPART);

    r = runStatementOnDriver("select a, b from " + Table.ACIDTBLPART + " order by a, b");
    Assert.assertEquals(stringifyValues(expected), r);
  }

  /**
   * Add a new partition to the table that contains a compacted folder with the visibilityTxnId
   * higher than the HighWaterMark in the HMS. This could happen if the HMS data was lost and we try to
   * repair the metadata. The system will set the txnId forward in the HMS.
   * @throws Exception
   */
  @Test
  public void testAddPartitionHighVisibilityId() throws Exception {

    runStatementOnDriver("drop table if exists " + acidTblPartMsck);

    // Insert few rows
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values(1,1,'p1'),(2,2,'p1'),(3,3,'p1')");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values(4,4,'p1')");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p) values(1,2,'p2'),(2,3,'p2'),(3,4,'p2')");

    //run Compaction
    runStatementOnDriver("alter table " + Table.ACIDTBLPART + " partition (p='p1') compact 'minor'");
    runWorker(hiveConf);
    runCleaner(hiveConf);

    List<String> r = runStatementOnDriver("select a, b from " + Table.ACIDTBLPART + " order by a, b");
    int[][] expected = { { 1, 1 }, { 1, 2 }, { 2, 2 }, { 2, 3 }, { 3, 3 }, { 3, 4 }, { 4, 4 } };
    Assert.assertEquals(stringifyValues(expected), r);
    // Create target table
    runStatementOnDriver("create table " + acidTblPartMsck
        + " (a int, b int) partitioned by (p string) clustered by (a) into 2 buckets"
        + " stored as orc TBLPROPERTIES ('transactional'='true')");

    // copy files on fs
    FileSystem fs = FileSystem.get(hiveConf);
    FileUtil.copy(fs, new Path(getWarehouseDir() + "/" + Table.ACIDTBLPART.toString().toLowerCase() + "/p=p1"), fs,
        new Path(getWarehouseDir(), acidTblPartMsck), false, hiveConf);
    FileUtil.copy(fs, new Path(getWarehouseDir() + "/" + Table.ACIDTBLPART.toString().toLowerCase() + "/p=p2"), fs,
        new Path(getWarehouseDir(), acidTblPartMsck), false, hiveConf);

    FileStatus[] fileStatuses = fs.listStatus(new Path(getWarehouseDir(), acidTblPartMsck + "/p=p1"));
    // one compacted delta
    Assert.assertEquals(1, fileStatuses.length);
    // Rename the deltaDir to add a higher visibility transactionId
    Path deltaDir = fileStatuses[0].getPath();
    ParsedDeltaLight parsedDelta =  ParsedDeltaLight.parse(deltaDir);
    long oldTxnId = parsedDelta.getVisibilityTxnId();
    String newDeltaDir = AcidUtils.addVisibilitySuffix(deltaDir.toString().substring(0, deltaDir.toString().length() - 9), oldTxnId + 100);
    fs.rename(deltaDir, new Path(newDeltaDir));

    // call msk repair
    runStatementOnDriver("msck repair table " + acidTblPartMsck);

    Long nextTxnId = txnHandler.openTxns(new OpenTxnRequest(1, "localhost", "me")).getTxn_ids().get(0);
    Assert.assertTrue("TxnId should be incremented", nextTxnId > (oldTxnId + 100));
    txnHandler.abortTxn(new AbortTxnRequest(nextTxnId));
    // We have to wait for the MetastoreConf.TXN_OPENTXN_TIMEOUT otherwise our hypothetical txnId in the visibilityTxnId
    // will be considered open and we will not read the folder
    Thread.sleep(1000);
    r = runStatementOnDriver("select a, b from " + acidTblPartMsck + " order by a, b");
    Assert.assertEquals(stringifyValues(expected), r);

    runStatementOnDriver("drop table if exists " + acidTblPartMsck);
  }

  /**
   * Add one partition to an insert-only table that contains an insert overwrite base folder.
   * @throws Exception
   */
  @Test
  public void testAddPartitionMMInsertOverwrite() throws Exception{
    final String mmTable = "mmtblpartmsck";
    final String sourceTable = "nonacidpartmsck";
    runStatementOnDriver("drop table if exists " + acidTblPartMsck);
    runStatementOnDriver("drop table if exists " + mmTable);
    runStatementOnDriver("drop table if exists " + sourceTable);
    runStatementOnDriver("create table " + mmTable + "(a int, b int) partitioned by (p string) stored as orc"
        + " TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')");
    runStatementOnDriver("create table " + sourceTable + "(a int, b int) partitioned by (p string) stored as orc"
        + " TBLPROPERTIES ('transactional'='false')");

    // Insert few rows
    runStatementOnDriver("insert into " + mmTable + " partition(p) values(1,1,'p1'),(2,2,'p1'),(3,3,'p1')");
    runStatementOnDriver("insert into " + mmTable + " partition(p) values(4,4,'p1')");
    runStatementOnDriver("insert into " + mmTable + " partition(p) values(1,2,'p2'),(2,3,'p2'),(3,4,'p2')");

    // Insert some to the source
    runStatementOnDriver("insert into " + sourceTable + " partition(p) values(10,10,'p1'),(20,20,'p1'),(30,30,'p1')");
    runStatementOnDriver("insert into " + sourceTable + " partition(p) values(40,40,'p1')");

    // Overwrite a partition
    runStatementOnDriver("insert overwrite table " + mmTable + " PARTITION(p='p1') " +
        " select a,b from " + sourceTable + " where " + sourceTable + ".p='p1'");

    List<String> r = runStatementOnDriver("select a, b from " + mmTable+ " order by a, b");
    int[][] expected = { { 1, 2 }, { 2, 3 }, { 3, 4 },{ 10, 10 }, { 20, 20 }, { 30, 30 }, {40, 40} };
    Assert.assertEquals(stringifyValues(expected), r);
    // Create target table
    runStatementOnDriver("create table " + acidTblPartMsck
        + " (a int, b int) partitioned by (p string) clustered by (a) into 2 buckets"
        + " stored as orc TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')");

    // copy files on fs
    FileSystem fs = FileSystem.get(hiveConf);
    FileUtil.copy(fs, new Path(getWarehouseDir() + "/" + mmTable + "/p=p1"), fs,
        new Path(getWarehouseDir(), acidTblPartMsck), false, hiveConf);
    FileUtil.copy(fs, new Path(getWarehouseDir() + "/" + mmTable + "/p=p2"), fs,
        new Path(getWarehouseDir(), acidTblPartMsck), false, hiveConf);

    FileStatus[] fileStatuses = fs.listStatus(new Path(getWarehouseDir(), acidTblPartMsck + "/p=p1"));
    // Two delta and one insert overwrite base
    Assert.assertEquals(3, fileStatuses.length);
    // call msk repair
    runStatementOnDriver("msck repair table " + acidTblPartMsck);

    r = runStatementOnDriver("select a, b from " + acidTblPartMsck + " order by a, b");
    Assert.assertEquals(stringifyValues(expected), r);

    runStatementOnDriver("drop table if exists " + acidTblPartMsck);
    runStatementOnDriver("drop table if exists " + mmTable);
    runStatementOnDriver("drop table if exists " + sourceTable);
  }

  /**
   * For non partitioned acid table the writeId and visibility txnId repair should work the same.
   * @throws Exception ex
   */
  @Test
  public void testNonPartitionedTable() throws Exception {
    String acidTblMsck = "acidtblmsck";
    runStatementOnDriver("drop table if exists " + acidTblMsck);

    // Insert few rows
    runStatementOnDriver("insert into " + Table.ACIDTBL + " values(1,1),(2,2),(3,3)");
    runStatementOnDriver("insert into " + Table.ACIDTBL + " values(4,4)");

    //run Compaction
    runStatementOnDriver("alter table " + Table.ACIDTBL + " compact 'minor'");
    runWorker(hiveConf);

    List<String> r = runStatementOnDriver("select a, b from " + Table.ACIDTBL + " order by a, b");
    int[][] expected = { { 1, 1 }, { 2, 2 }, { 3, 3 }, { 4, 4 } };
    Assert.assertEquals(stringifyValues(expected), r);
    // Create target table

    runStatementOnDriver("create table " + acidTblMsck
        + " (a int, b int) clustered by (a) into 2 buckets"
        + " stored as orc TBLPROPERTIES ('transactional'='true')");

    // copy files on fs
    FileSystem fs = FileSystem.get(hiveConf);
    for (FileStatus status : fs.listStatus(new Path(getWarehouseDir(), Table.ACIDTBL.toString().toLowerCase()))) {
      FileUtil.copy(fs, status.getPath(), fs,
          new Path(getWarehouseDir(), acidTblMsck), false, hiveConf);
    }

    FileStatus[] fileStatuses = fs.listStatus(new Path(getWarehouseDir(), acidTblMsck));
    // two + one delta
    Assert.assertEquals(3, fileStatuses.length);

    // call msk repair
    runStatementOnDriver("msck repair table " + acidTblMsck);

    r = runStatementOnDriver("select a, b from " + acidTblMsck + " order by a, b");
    Assert.assertEquals(stringifyValues(expected), r);

    runStatementOnDriver("drop table if exists " + acidTblMsck);
  }
}
