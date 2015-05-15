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

package org.apache.hadoop.hive.ql;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.orc.FileDump;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * TODO: this should be merged with TestTxnCommands once that is checked in
 * specifically the tests; the supporting code here is just a clone of TestTxnCommands
 */
public class TestTxnCommands2 {
  private static final String TEST_DATA_DIR = new File(System.getProperty("java.io.tmpdir") +
    File.separator + TestTxnCommands2.class.getCanonicalName()
    + "-" + System.currentTimeMillis()
  ).getPath().replaceAll("\\\\", "/");
  private static final String TEST_WAREHOUSE_DIR = TEST_DATA_DIR + "/warehouse";
  //bucket count for test tables; set it to 1 for easier debugging
  private static int BUCKET_COUNT = 2;
  @Rule
  public TestName testName = new TestName();
  private HiveConf hiveConf;
  private Driver d;
  private static enum Table {
    ACIDTBL("acidTbl"),
    ACIDTBLPART("acidTblPart"),
    NONACIDORCTBL("nonAcidOrcTbl"),
    NONACIDPART("nonAcidPart");
    
    private final String name;
    @Override
    public String toString() {
      return name;
    }
    Table(String name) {
      this.name = name;
    }
  }

  @Before
  public void setUp() throws Exception {
    tearDown();
    hiveConf = new HiveConf(this.getClass());
    hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, TEST_WAREHOUSE_DIR);
    TxnDbUtil.setConfValues(hiveConf);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVEENFORCEBUCKETING, true);
    TxnDbUtil.prepDb();
    File f = new File(TEST_WAREHOUSE_DIR);
    if (f.exists()) {
      FileUtil.fullyDelete(f);
    }
    if (!(new File(TEST_WAREHOUSE_DIR).mkdirs())) {
      throw new RuntimeException("Could not create " + TEST_WAREHOUSE_DIR);
    }
    SessionState.start(new SessionState(hiveConf));
    d = new Driver(hiveConf);
    dropTables();
    runStatementOnDriver("create table " + Table.ACIDTBL + "(a int, b int) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("create table " + Table.ACIDTBLPART + "(a int, b int) partitioned by (p string) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("create table " + Table.NONACIDORCTBL + "(a int, b int) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='false')");
    runStatementOnDriver("create table " + Table.NONACIDPART + "(a int, b int) partitioned by (p string) stored as orc TBLPROPERTIES ('transactional'='false')");
  }
  private void dropTables() throws Exception {
    for(Table t : Table.values()) {
      runStatementOnDriver("drop table if exists " + t);
    }
  }
  @After
  public void tearDown() throws Exception {
    try {
      if (d != null) {
     //   runStatementOnDriver("set autocommit true");
        dropTables();
        d.destroy();
        d.close();
        d = null;
      }
      TxnDbUtil.cleanDb();
    } finally {
      FileUtils.deleteDirectory(new File(TEST_DATA_DIR));
    }
  }
  @Ignore("not needed but useful for testing")
  @Test
  public void testNonAcidInsert() throws Exception {
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(1,2)");
    List<String> rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(2,3)");
    List<String> rs1 = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
  }
  @Test
  public void testUpdateMixedCase() throws Exception {
    int[][] tableData = {{1,2},{3,3},{5,3}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(tableData));
    runStatementOnDriver("update " + Table.ACIDTBL + " set B = 7 where A=1");
    List<String> rs = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    int[][] updatedData = {{1,7},{3,3},{5,3}};
    Assert.assertEquals("Update failed", stringifyValues(updatedData), rs);
    runStatementOnDriver("update " + Table.ACIDTBL + " set B = B + 1 where A=1");
    List<String> rs2 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    int[][] updatedData2 = {{1,8},{3,3},{5,3}};
    Assert.assertEquals("Update failed", stringifyValues(updatedData2), rs2);
  }
  @Test
  public void testDeleteIn() throws Exception {
    int[][] tableData = {{1,2},{3,2},{5,2},{1,3},{3,3},{5,3}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) " + makeValuesClause(tableData));
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(1,7),(3,7)");
    //todo: once multistatement txns are supported, add a test to run next 2 statements in a single txn
    runStatementOnDriver("delete from " + Table.ACIDTBL + " where a in(select a from " + Table.NONACIDORCTBL + ")");
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) select a,b from " + Table.NONACIDORCTBL);
    List<String> rs = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    int[][] updatedData = {{1,7},{3,7},{5,2},{5,3}};
    Assert.assertEquals("Bulk update failed", stringifyValues(updatedData), rs);
    runStatementOnDriver("update " + Table.ACIDTBL + " set b=19 where b in(select b from " + Table.NONACIDORCTBL + " where a = 3)");
    List<String> rs2 = runStatementOnDriver("select a,b from " + Table.ACIDTBL + " order by a,b");
    int[][] updatedData2 = {{1,19},{3,19},{5,2},{5,3}};
    Assert.assertEquals("Bulk update2 failed", stringifyValues(updatedData2), rs2);
  }

  /**
   * https://issues.apache.org/jira/browse/HIVE-10151
   */
  @Test
  public void testBucketizedInputFormat() throws Exception {
    int[][] tableData = {{1,2}};
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " partition(p=1) (a,b) " + makeValuesClause(tableData));

    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) select a,b from " + Table.ACIDTBLPART + " where p = 1");
    List<String> rs = runStatementOnDriver("select a,b from " + Table.ACIDTBL);//no order by as it's just 1 row
    Assert.assertEquals("Insert into " + Table.ACIDTBL + " didn't match:", stringifyValues(tableData), rs);

    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) select a,b from " + Table.ACIDTBLPART + " where p = 1");
    List<String> rs2 = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);//no order by as it's just 1 row
    Assert.assertEquals("Insert into " + Table.NONACIDORCTBL + " didn't match:", stringifyValues(tableData), rs2);
  }
  @Test
  public void testInsertOverwriteWithSelfJoin() throws Exception {
    int[][] part1Data = {{1,7}};
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) " + makeValuesClause(part1Data));
    //this works because logically we need S lock on NONACIDORCTBL to read and X lock to write, but
    //LockRequestBuilder dedups locks on the same entity to only keep the highest level lock requested
    runStatementOnDriver("insert overwrite table " + Table.NONACIDORCTBL + " select 2, 9 from " + Table.NONACIDORCTBL + " T inner join " + Table.NONACIDORCTBL + " S on T.a=S.a");
    List<String> rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL + " order by a,b");
    int[][] joinData = {{2,9}};
    Assert.assertEquals("Self join non-part insert overwrite failed", stringifyValues(joinData), rs);
    int[][] part2Data = {{1,8}};
    runStatementOnDriver("insert into " + Table.NONACIDPART + " partition(p=1) (a,b) " + makeValuesClause(part1Data));
    runStatementOnDriver("insert into " + Table.NONACIDPART + " partition(p=2) (a,b) " + makeValuesClause(part2Data));
    //here we need X lock on p=1 partition to write and S lock on 'table' to read which should
    //not block each other since they are part of the same txn
    runStatementOnDriver("insert overwrite table " + Table.NONACIDPART + " partition(p=1) select a,b from " + Table.NONACIDPART);
    List<String> rs2 = runStatementOnDriver("select a,b from " + Table.NONACIDPART + " order by a,b");
    int[][] updatedData = {{1,7},{1,8},{1,8}};
    Assert.assertEquals("Insert overwrite partition failed", stringifyValues(updatedData), rs2);
    //insert overwrite not supported for ACID tables
  }
  /**
   * takes raw data and turns it into a string as if from Driver.getResults()
   * sorts rows in dictionary order
   */
  private List<String> stringifyValues(int[][] rowsIn) {
    assert rowsIn.length > 0;
    int[][] rows = rowsIn.clone();
    Arrays.sort(rows, new RowComp());
    List<String> rs = new ArrayList<String>();
    for(int[] row : rows) {
      assert row.length > 0;
      StringBuilder sb = new StringBuilder();
      for(int value : row) {
        sb.append(value).append("\t");
      }
      sb.setLength(sb.length() - 1);
      rs.add(sb.toString());
    }
    return rs;
  }
  private static final class RowComp implements Comparator<int[]> {
    public int compare(int[] row1, int[] row2) {
      assert row1 != null && row2 != null && row1.length == row2.length;
      for(int i = 0; i < row1.length; i++) {
        int comp = Integer.compare(row1[i], row2[i]);
        if(comp != 0) {
          return comp;
        }
      }
      return 0;
    }
  }
  private String makeValuesClause(int[][] rows) {
    assert rows.length > 0;
    StringBuilder sb = new StringBuilder("values");
    for(int[] row : rows) {
      assert row.length > 0;
      if(row.length > 1) {
        sb.append("(");
      }
      for(int value : row) {
        sb.append(value).append(",");
      }
      sb.setLength(sb.length() - 1);//remove trailing comma
      if(row.length > 1) {
        sb.append(")");
      }
      sb.append(",");
    }
    sb.setLength(sb.length() - 1);//remove trailing comma
    return sb.toString();
  }
  
  private List<String> runStatementOnDriver(String stmt) throws Exception {
    CommandProcessorResponse cpr = d.run(stmt);
    if(cpr.getResponseCode() != 0) {
      throw new RuntimeException(stmt + " failed: " + cpr);
    }
    List<String> rs = new ArrayList<String>();
    d.getResults(rs);
    return rs;
  }
}
