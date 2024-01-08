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
package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * tests for IMPORT/EXPORT of transactional tables.
 */
public class TestTxnExIm extends TxnCommandsBaseForTests {
  private static final Logger LOG = LoggerFactory.getLogger(TestTxnExIm.class);
  private static final String TEST_DATA_DIR = new File(System.getProperty("java.io.tmpdir") +
      File.separator + TestTxnExIm.class.getCanonicalName() + "-" + System.currentTimeMillis()
  ).getPath().replaceAll("\\\\", "/");

  @Override
  protected String getTestDataDir() {
    return TEST_DATA_DIR;
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    hiveConf.set(MetastoreConf.ConfVars.CREATE_TABLES_AS_ACID.getVarname(), "true");
  }

  /**
   * simplest export test.
   */
  @Test
  public void testExportDefaultDb() throws Exception {
    testExport(true);
  }
  @Test
  public void testExportCustomDb() throws Exception {
    testExport(false);
  }
  private void testExport(boolean useDefualtDb) throws Exception {
    int[][] rows1 = {{1, 2}, {3, 4}};
    final String tblName = useDefualtDb ? "T" : "foo.T";
    runStatementOnDriver("drop table if exists " + tblName);
    runStatementOnDriver("drop table if exists TImport ");
    if(!useDefualtDb) {
      runStatementOnDriver("create database foo");
    }
    runStatementOnDriver("create table " + tblName + " (a int, b int) stored as ORC");
    runStatementOnDriver("create table TImport (a int, b int) stored as ORC TBLPROPERTIES " +
        "('transactional'='false')");
    runStatementOnDriver("insert into " + tblName + "(a,b) " + makeValuesClause(rows1));
    List<String> rs = runStatementOnDriver("select * from " + tblName + " order by a,b");
    Assert.assertEquals("Content didn't match rs", stringifyValues(rows1), rs);

    String exportStmt = "export table " + tblName + " to '" + getTestDataDir() + "/export'";
    rs = runStatementOnDriver("explain " + exportStmt);
    StringBuilder sb = new StringBuilder("*** " + exportStmt);
    for (String r : rs) {
      sb.append("\n").append(r);
    }
    LOG.error(sb.toString());

    runStatementOnDriver(exportStmt);
    //verify data
    runStatementOnDriver("import table TImport from '" + getTestDataDir() + "/export'");
    List<String> rs1 = runStatementOnDriver("select * from TImport order by a, b");
    Assert.assertEquals("Content didn't match rs", stringifyValues(rows1), rs1);
  }

  /**
   * The update delete cause MergeFileTask to be executed.
   */
  @Test
  public void testExportMerge() throws Exception {
    int[][] rows1 = {{1, 2}, {3, 4}};
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("drop table if exists TImport ");
    runStatementOnDriver("create table T (a int, b int) stored as ORC");
    runStatementOnDriver("create table TImport (a int, b int) stored as ORC TBLPROPERTIES " +
        "('transactional'='false')");
    runStatementOnDriver("insert into T(a,b) " + makeValuesClause(rows1));
    runStatementOnDriver("update T set b = 17 where a = 1");
    int[][] rows2 = {{1, 17}, {3, 4}};
    List<String> rs = runStatementOnDriver("select * from T order by a,b");
    Assert.assertEquals("Content didn't match rs", stringifyValues(rows2), rs);

    String exportStmt = "export table T to '" + getTestDataDir() + "/export'";
    rs = runStatementOnDriver("explain " + exportStmt);
    StringBuilder sb = new StringBuilder("*** " + exportStmt);
    for (String r : rs) {
      sb.append("\n").append(r);
    }
    LOG.error(sb.toString());

    runStatementOnDriver(exportStmt);
    //verify data
    runStatementOnDriver("import table TImport from '" + getTestDataDir() + "/export'");
    List<String> rs1 = runStatementOnDriver("select * from TImport order by a, b");
    Assert.assertEquals("Content didn't match rs", stringifyValues(rows2), rs1);
  }

  /**
   * export partitioned table with full partition spec.
   */
  @Test
  public void testExportPart() throws Exception {
    int[][] rows1 = {{1, 2, 1}, {3, 4, 2}};
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("drop table if exists TImport ");
    runStatementOnDriver("create table TImport (a int, b int) partitioned by (p int) stored as " +
        "ORC TBLPROPERTIES ('transactional'='false')");
    runStatementOnDriver("create table T (a int, b int) partitioned by (p int) stored as ORC");
    runStatementOnDriver("insert into T partition(p)" + makeValuesClause(rows1));
    runStatementOnDriver("export table T partition(p=1) to '" + getTestDataDir() + "/export'");
      /*
target/tmp/org.apache.hadoop.hive.ql.TestTxnCommands-1519423568221/
├── export
│   ├── _metadata
│   └── p=1
│       └── delta_0000001_0000001_0000
│           └── bucket_00000
*/
    runStatementOnDriver("import table TImport from '" + getTestDataDir() + "/export'");
    List<String> rs1 = runStatementOnDriver("select * from TImport order by a, b");
    int[][] res = {{1, 2, 1}};
    Assert.assertEquals("Content didn't match rs", stringifyValues(res), rs1);
  }

  /**
   * Export partitioned table with partial partition spec.
   */
  @Test
  public void testExportPartPartial() throws Exception {
    int[][] rows1 = {{1, 2, 1, 1}, {3, 4, 2, 2}, {5, 6, 1, 2}, {7, 8, 2, 2}};
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("drop table if exists TImport ");
    runStatementOnDriver("create table TImport (a int, b int) partitioned by (p int, q int) " +
        "stored as ORC TBLPROPERTIES ('transactional'='false')");
    runStatementOnDriver("create table T (a int, b int) partitioned by (p int, q int) stored as " +
        "ORC");
    runStatementOnDriver("insert into T partition(p,q)" + makeValuesClause(rows1));

    runStatementOnDriver("export table T partition(p=1) to '" + getTestDataDir() + "/export'");
    runStatementOnDriver("import table TImport from '" + getTestDataDir() + "/export'");
    List<String> rs1 = runStatementOnDriver("select * from TImport order by a, b");
    int[][] res = {{1, 2, 1, 1}, {5, 6, 1, 2}};
    Assert.assertEquals("Content didn't match rs", stringifyValues(res), rs1);
    /*  Here is the layout we expect
target/tmp/org.apache.hadoop.hive.ql.TestTxnCommands-1521148657811/
├── export
│   ├── _metadata
│   └── p=1
│       ├── q=1
│       │   └── 000002_0
│       └── q=2
│           └── 000001_0
└── warehouse
    ├── acidtbl
    ├── acidtblpart
    ├── nonacidnonbucket
    ├── nonacidorctbl
    ├── nonacidorctbl2
    ├── t
    │   ├── p=1
    │   │   ├── q=1
    │   │   │   └── delta_0000001_0000001_0000
    │   │   │       ├── _orc_acid_version
    │   │   │       └── bucket_00000
    │   │   └── q=2
    │   │       └── delta_0000001_0000001_0000
    │   │           ├── _orc_acid_version
    │   │           └── bucket_00000
    │   └── p=2
    │       └── q=2
    │           └── delta_0000001_0000001_0000
    │               ├── _orc_acid_version
    │               └── bucket_00000
    └── timport
        └── p=1
            ├── q=1
            │   └── 000002_0
            └── q=2
                └── 000001_0

23 directories, 11 files
*/
  }
  /**
   * This specifies partial partition spec omitting top/first columns.
   */
  @Test
  public void testExportPartPartial2() throws Exception {
    int[][] rows1 = {{1, 2, 1, 1}, {3, 4, 2, 2}, {5, 6, 1, 2}, {7, 8, 2, 2}};
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("drop table if exists TImport ");
    runStatementOnDriver("create table TImport (a int, b int) partitioned by (p int, q int)" +
        " stored as ORC TBLPROPERTIES ('transactional'='false')");
    runStatementOnDriver("create table T (a int, b int) partitioned by (p int, q int) " +
        "stored as ORC");
    runStatementOnDriver("insert into T partition(p,q)" + makeValuesClause(rows1));

    runStatementOnDriver("export table T partition(q=2) to '" + getTestDataDir() + "/export'");
    runStatementOnDriver("import table TImport from '" + getTestDataDir() + "/export'");
    List<String> rs1 = runStatementOnDriver("select * from TImport order by a, b");
    int[][] res = {{3, 4, 2, 2}, {5, 6, 1, 2}, {7, 8, 2, 2}};
    Assert.assertEquals("Content didn't match rs", stringifyValues(res), rs1);
  }
  @Test
  public void testExportPartPartial3() throws Exception {
    int[][] rows1 = {{1, 1, 1, 2}, {3, 2, 3, 8}, {5, 1, 2, 6}, {7, 2, 2, 8}};
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("drop table if exists TImport ");
    runStatementOnDriver("create table TImport (a int) partitioned by (p int, q int, r int)" +
        " stored as ORC TBLPROPERTIES ('transactional'='false')");
    runStatementOnDriver("create table T (a int) partitioned by (p int, q int, r int) " +
        "stored as ORC");
    runStatementOnDriver("insert into T partition(p,q,r)" + makeValuesClause(rows1));

    runStatementOnDriver("export table T partition(p=2,r=8) to '" + getTestDataDir() + "/export'");
    runStatementOnDriver("import table TImport from '" + getTestDataDir() + "/export'");
    List<String> rs1 = runStatementOnDriver("select * from TImport order by a");
    int[][] res = {{3, 2, 3, 8}, {7, 2, 2, 8}};
    Assert.assertEquals("Content didn't match rs", stringifyValues(res), rs1);
  }

  @Test
  public void testExportBucketed() throws Exception {
    int[][] rows1 = {{1, 2}, {1, 3}, {2, 4}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + makeValuesClause(rows1));
    runStatementOnDriver("export table " + Table.ACIDTBL + "  to '" + getTestDataDir()
        + "/export'");
    runStatementOnDriver("drop table if exists TImport ");
    runStatementOnDriver("create table TImport (a int, b int) clustered by (a) into 2 buckets" +
        " stored as ORC TBLPROPERTIES ('transactional'='false')");

    runStatementOnDriver("import table TImport from '" + getTestDataDir() + "/export'");
    List<String> rs1 = runStatementOnDriver("select * from TImport order by a, b");
    Assert.assertEquals("Content didn't match rs", stringifyValues(rows1), rs1);
  }

  @Ignore
  @Test
  public void testCTLT() throws Exception {
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("create table T like " + Table.ACIDTBL + " TBLPROPERTIES " +
        "('transactional'='true')");
//      runStatementOnDriver("create table T like " + Table.ACIDTBL);
    List<String> rs = runStatementOnDriver("show create table T");
    StringBuilder sb = new StringBuilder("*show create table");
    for (String r : rs) {
      sb.append("\n").append(r);
    }
    LOG.error(sb.toString());
  }

  /**
   * tests import where target table already exists.
   */
  @Test
  public void testImport() throws Exception {
    testImport(false, true);
  }
  /**
   * tests import where target table already exists.
   */
  @Test
  public void testImportVectorized() throws Exception {
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    testImport(true, true);
  }
  /**
   * tests import where target table does not exists.
   */
  @Test
  public void testImportNoTarget() throws Exception {
    testImport(false, false);
  }
  /**
   * MM tables already work - mm_exim.q
   * Export creates a bunch of metadata in addition to data including all table props/IF/OF etc
   * Import from 'export' can create a table (any name specified) or add data into existing table.
   * If importing into existing table (un-partitioned) it must be empty.
   * If Import is creating a table it will be exactly like exported one except for the name.
   */
  private void testImport(boolean isVectorized, boolean existingTarget) throws Exception {
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("drop table if exists Tstage");
    if(existingTarget) {
      runStatementOnDriver("create table T (a int, b int) stored as orc");
    }
    //Tstage is just a simple way to generate test data
    runStatementOnDriver("create table Tstage (a int, b int) stored as orc " +
        "tblproperties('transactional'='true')");
    //this creates an ORC data file with correct schema under table root
    runStatementOnDriver("insert into Tstage values(1,2),(3,4),(5,6)");
    runStatementOnDriver("export table Tstage to '" + getWarehouseDir() + "/1'");
    //runStatementOnDriver("truncate table Tstage");

    //load into existing empty table T
    runStatementOnDriver("import table T from '" + getWarehouseDir() + "/1'");

    String testQuery = isVectorized ? "select ROW__ID, a, b from T order by ROW__ID" :
        "select ROW__ID, a, b, INPUT__FILE__NAME from T order by ROW__ID";
    String[][] expected = new String[][] {
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t1\t2",
            "t/delta_0000001_0000001_0000/000000_0"},
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t3\t4",
            "t/delta_0000001_0000001_0000/000000_0"},
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":2}\t5\t6",
            "t/delta_0000001_0000001_0000/000000_0"}};
    checkResult(expected, testQuery, isVectorized, "import existing table");

    runStatementOnDriver("update T set a = 0 where b = 6");
    String[][] expected2 = new String[][] {
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t1\t2",
            "t/delta_0000001_0000001_0000/000000_0"},
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t3\t4",
            "t/delta_0000001_0000001_0000/000000_0"},
        {"{\"writeid\":2,\"bucketid\":536870913,\"rowid\":0}\t0\t6",
            "t/delta_0000002_0000002_0001/bucket_00000_0"}};
    checkResult(expected2, testQuery, isVectorized, "update imported table");

    runStatementOnDriver("alter table T compact 'minor'");
    TestTxnCommands2.runWorker(hiveConf);
    String[][] expected3 = new String[][] {
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t1\t2",
            ".*t/delta_0000001_0000002_v000002[6-7]/bucket_00000"},
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t3\t4",
            ".*t/delta_0000001_0000002_v000002[6-7]/bucket_00000"},
        {"{\"writeid\":2,\"bucketid\":536870913,\"rowid\":0}\t0\t6",
            ".*t/delta_0000001_0000002_v000002[6-7]/bucket_00000"}};
    checkResult(expected3, testQuery, isVectorized, "minor compact imported table");

  }

  @Test
  public void testImportPartitioned() throws Exception {
    boolean isVectorized = false;
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("drop table if exists Tstage");
    runStatementOnDriver("create table T (a int, b int) partitioned by (p int) stored as orc");
    //Tstage is just a simple way to generate test data
    runStatementOnDriver("create table Tstage (a int, b int) partitioned by (p int) stored" +
        " as orc tblproperties('transactional'='false')");
    //this creates an ORC data file with correct schema under table root
    runStatementOnDriver("insert into Tstage values(1,2,10),(3,4,11),(5,6,12)");
    //now we have an archive with 3 partitions
    runStatementOnDriver("export table Tstage to '" + getWarehouseDir() + "/1'");

    //make the partition in Target not empty
    runStatementOnDriver("insert into T values(0,0,10)");
    //load partition that doesn't exist in T
    runStatementOnDriver("import table T PARTITION(p=11) from '" + getWarehouseDir() + "/1'");
    //load partition that doesn't exist in T
    runStatementOnDriver("import table T PARTITION(p=12) from '" + getWarehouseDir() + "/1'");
    String testQuery = isVectorized ? "select ROW__ID, a, b from T order by ROW__ID" :
        "select ROW__ID, a, b, INPUT__FILE__NAME from T order by ROW__ID";
    String[][] expected = new String[][] {
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t0\t0",
            "t/p=10/delta_0000001_0000001_0000/bucket_00000_0"},
        {"{\"writeid\":2,\"bucketid\":536870912,\"rowid\":0}\t3\t4",
            "t/p=11/delta_0000002_0000002_0000/000000_0"},
        {"{\"writeid\":3,\"bucketid\":536870912,\"rowid\":0}\t5\t6",
            "t/p=12/delta_0000003_0000003_0000/000000_0"}};
    checkResult(expected, testQuery, isVectorized, "import existing table");
  }

  @Test
  public void testImportPartitionedOrc() throws Exception {
    // Clear and drop table T,Tstage
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("drop table if exists Tstage");

    // Create source table - Tstage
    runStatementOnDriver("create table Tstage (a int, b int) partitioned by (p int) stored"
        + " as orc tblproperties('transactional'='true')");

    // This creates an ORC data file with correct schema under table root
    runStatementOnDriver("insert into Tstage values(1,2,10),(3,4,11),(5,6,12)");
    final int[][] rows = { { 3 } };

    // Check Partitions statistics
    List<String> rsTstagePartitionsProperties = runStatementOnDriver("show partitions Tstage");
    for (String rsTstagePartition : rsTstagePartitionsProperties) {
      List<String> rsPartitionProperties =
          runStatementOnDriver("describe formatted Tstage partition(" + rsTstagePartition + ")");
      Assert.assertEquals("COLUMN_STATS_ACCURATE of partition " + rsTstagePartition + " of Tstage table", true,
          rsPartitionProperties.contains("\tCOLUMN_STATS_ACCURATE\t{\\\"BASIC_STATS\\\":\\\"true\\\"}"));
      Assert.assertEquals(" of partition " + rsTstagePartition + " of Tstage table", true,
          rsPartitionProperties.contains("\tnumRows             \t1                   "));
    }

    // Now we have an archive Tstage with 3 partitions
    runStatementOnDriver("export table Tstage to '" + getWarehouseDir() + "/1'");

    // Load T
    runStatementOnDriver("import table T from '" + getWarehouseDir() + "/1'");

    // Check basic stats in tblproperties of T
    List<String> rsTProperties = runStatementOnDriver("show tblproperties T");
    Assert.assertEquals("COLUMN_STATS_ACCURATE of T table", false,
        rsTProperties.contains("COLUMN_STATS_ACCURATE\t{\"BASIC_STATS\":\"true\"}"));
    Assert.assertEquals("numRows of T table", false, rsTProperties.contains("numRows\t3"));

    // Check Partitions statistics of T
    List<String> rsTPartitionsProperties = runStatementOnDriver("show partitions T");
    for (String rsTPartition : rsTPartitionsProperties) {
      List<String> rsPartitionProperties = runStatementOnDriver("describe formatted T partition(" + rsTPartition + ")");
      Assert.assertEquals("COLUMN_STATS_ACCURATE of partition " + rsTPartition + " of T table", false,
          rsPartitionProperties.contains("\tCOLUMN_STATS_ACCURATE\t{\\\"BASIC_STATS\\\":\\\"true\\\"}"));
      Assert.assertEquals(" of partition " + rsTPartition + " of T table", false,
          rsPartitionProperties.contains("\tnumRows             \t1                   "));
    }

    // Verify the count(*) output
    List<String> rs = runStatementOnDriver("select count(*) from T");
    Assert.assertEquals("Rowcount of imported table", TestTxnCommands2.stringifyValues(rows), rs);
  }

  /**
   * test selective partitioned import where target table needs to be created.
   * export is made from acid table so that target table is created as acid
   */
  @Test
  public void testImportPartitionedCreate() throws Exception {
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("drop table if exists Tstage");
    //Tstage is just a simple way to generate test data
    runStatementOnDriver("create table Tstage (a int, b int) partitioned by (p int) stored" +
        " as orc");
    int[][] data = {{1, 2, 10}, {3, 4, 11}, {5, 6, 12}};
    //this creates an ORC data file with correct schema under table root
    runStatementOnDriver("insert into Tstage" + TestTxnCommands2.makeValuesClause(data));
    //now we have an archive with 3 partitions
    runStatementOnDriver("export table Tstage to '" + getWarehouseDir() + "/1'");

    /*
     * load partition that doesn't exist in T
     * There is some parallelism going on if you load more than 1 partition which I don't
     * understand. In testImportPartitionedCreate2() that's reasonable since each partition is
     * loaded in parallel.  Why it happens here is beyond me.
     * The file name changes from run to run between 000000_0 and 000001_0 and 000002_0
     * The data is correct but this causes ROW__ID.bucketId/file names to change
     */
    runStatementOnDriver("import table T PARTITION(p=10) from '" + getWarehouseDir() + "/1'");
    runStatementOnDriver("import table T PARTITION(p=11) from '" + getWarehouseDir() + "/1'");
    runStatementOnDriver("import table T PARTITION(p=12) from '" + getWarehouseDir() + "/1'");

    //verify data
    List<String> rs = runStatementOnDriver("select a, b, p from T order by a,b,p");
    Assert.assertEquals("reading imported data",
        TestTxnCommands2.stringifyValues(data), rs);
    //verify that we are indeed doing an Acid write (import)
    rs = runStatementOnDriver("select INPUT__FILE__NAME from T order by INPUT__FILE__NAME");
    Assert.assertEquals(3, rs.size());
    Assert.assertTrue(rs.get(0).contains("t/p=10/delta_0000001_0000001_0000/00000"));
    Assert.assertTrue(rs.get(1).contains("t/p=11/delta_0000002_0000002_0000/00000"));
    Assert.assertTrue(rs.get(2).contains("t/p=12/delta_0000003_0000003_0000/00000"));
  }

  /**
   * import all partitions from archive - target table needs to be created.
   * export is made from acid table so that target table is created as acid
   */
  @Test
  public void testImportPartitionedCreate2() throws Exception {
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("drop table if exists Tstage");
    //Tstage is just a simple way to generate test data
    runStatementOnDriver("create table Tstage (a int, b int) partitioned by (p int) stored" +
        " as orc");
    int[][] data = {{1, 2, 10}, {3, 4, 11}, {5, 6, 12}};
    //this creates an ORC data file with correct schema under table root
    runStatementOnDriver("insert into Tstage" + TestTxnCommands2.makeValuesClause(data));
    //now we have an archive with 3 partitions
    runStatementOnDriver("export table Tstage to '" + getWarehouseDir() + "/1'");

    /*
     * load entire archive
     * There is some parallelism going on if you load more than 1 partition
     * The file name changes from run to run between 000000_0 and 000001_0 and 000002_0
     * The data is correct but this causes ROW__ID.bucketId/file names to change
     */
    runStatementOnDriver("import table T from '" + getWarehouseDir() + "/1'");

    //verify data
    List<String> rs = runStatementOnDriver("select a, b, p from T order by a,b,p");
    Assert.assertEquals("reading imported data",
        TestTxnCommands2.stringifyValues(data), rs);
    //verify that we are indeed doing an Acid write (import)
    rs = runStatementOnDriver("select INPUT__FILE__NAME from T order by INPUT__FILE__NAME");
    Assert.assertEquals(3, rs.size());
    Assert.assertTrue(rs.get(0).contains("t/p=10/delta_0000001_0000001_0000/00000"));
    Assert.assertTrue(rs.get(1).contains("t/p=11/delta_0000001_0000001_0000/00000"));
    Assert.assertTrue(rs.get(2).contains("t/p=12/delta_0000001_0000001_0000/00000"));
  }
  @Test
  public void testMM() throws Exception {
    testMM(true, true);
  }
  @Test
  public void testMMFlatSource() throws Exception {
    testMM(true, false);
  }
  @Test
  public void testMMCreate() throws Exception {
    testMM(false, true);
  }
  @Ignore("in this case no transactional tables are involved")
  @Test
  public void testMMCreateFlatSource() throws Exception {
    testMM(false, false);
  }
  private void testMM(boolean existingTable, boolean isSourceMM) throws Exception {
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_CREATE_TABLES_AS_INSERT_ONLY, true);
    hiveConf.setBoolean("mapred.input.dir.recursive", true);

    int[][] data = {{1,2}, {3, 4}, {5, 6}};
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("drop table if exists Tstage");

    if(existingTable) {
      runStatementOnDriver("create table T (a int, b int)");
    }

    runStatementOnDriver("create table Tstage (a int, b int)" +
        (isSourceMM ? "" : " tblproperties('transactional'='false')"));
    runStatementOnDriver("insert into Tstage" + TestTxnCommands2.makeValuesClause(data));
    runStatementOnDriver("export table Tstage to '" + getWarehouseDir() + "/1'");

    runStatementOnDriver("import table T from '" + getWarehouseDir() + "/1'");

    //verify data
    List<String> rs = runStatementOnDriver("select a, b from T order by a, b");
    Assert.assertEquals("reading imported data",
        TestTxnCommands2.stringifyValues(data), rs);
    //verify that we are indeed doing an Acid write (import)
    rs = runStatementOnDriver("select INPUT__FILE__NAME from T order by INPUT__FILE__NAME");
    Assert.assertEquals(3, rs.size());
    for (String s : rs) {
      Assert.assertTrue(s, s.contains("/delta_0000001_0000001_0000/"));
      Assert.assertTrue(s, s.endsWith("/000000_0"));
    }
  }
  private void checkResult(String[][] expectedResult, String query, boolean isVectorized,
      String msg) throws Exception{
    checkResult(expectedResult, query, isVectorized, msg, LOG);
  }

  /**
   * This test will fail - MM export doesn't filter out aborted transaction data.
   */
  @Ignore()
  @Test
  public void testMMExportAborted() throws Exception {
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_CREATE_TABLES_AS_INSERT_ONLY, true);
    hiveConf.setBoolean("mapred.input.dir.recursive", true);
    int[][] data = {{1, 2}, {3, 4}, {5, 6}};
    int[][] dataAbort = {{10, 2}};
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("drop table if exists Tstage");
    runStatementOnDriver("create table T (a int, b int)");
    runStatementOnDriver("create table Tstage (a int, b int)");

    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, true);
    runStatementOnDriver("insert into Tstage" + TestTxnCommands2.makeValuesClause(dataAbort));
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, false);
    runStatementOnDriver("insert into Tstage" + TestTxnCommands2.makeValuesClause(data));
    runStatementOnDriver("export table Tstage to '" + getWarehouseDir() + "/1'");

    runStatementOnDriver("import table T from '" + getWarehouseDir() + "/1'");
    //verify data
    List<String> rs = runStatementOnDriver("select a, b from T order by a, b");
    Assert.assertEquals("reading imported data",
        TestTxnCommands2.stringifyValues(data), rs);

  }

  @Test
  public void testImportOrc() throws Exception {
    // Clear and Drop T and Tstage if exist
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("drop table if exists Tstage");

    // Create source table - Tstage
    runStatementOnDriver("create table Tstage (a int, b int) stored" + " as orc tblproperties('transactional'='true')");

    // This creates an ORC data file with correct schema under table root
    runStatementOnDriver("insert into Tstage values(1,2),(3,4),(5,6)");
    final int[][] rows = { { 3 } };

    // Check Tstage statistics
    List<String> rsTStageProperties = runStatementOnDriver("show tblproperties Tstage");
    Assert.assertEquals("COLUMN_STATS_ACCURATE of Tstage table", true,
        rsTStageProperties.contains("COLUMN_STATS_ACCURATE\t{\"BASIC_STATS\":\"true\"}"));
    Assert.assertEquals("numRows of Tstage table", true, rsTStageProperties.contains("numRows\t3"));
    Assert.assertEquals("numFiles of Tstage table", true, rsTStageProperties.contains("numFiles\t1"));

    // Now we have an archive Tstage table
    runStatementOnDriver("export table Tstage to '" + getWarehouseDir() + "/1'");

    // Load T
    runStatementOnDriver("import table T from '" + getWarehouseDir() + "/1'");

    // Check basic stats in tblproperties T
    List<String> rsTProperties = runStatementOnDriver("show tblproperties T");
    Assert.assertEquals("COLUMN_STATS_ACCURATE of T table", false,
        rsTProperties.contains("COLUMN_STATS_ACCURATE\t{\"BASIC_STATS\":\"true\"}"));
    Assert.assertEquals("numRows of T table", false, rsTProperties.contains("numRows\t3"));

    // Verify the count(*) output
    List<String> rs = runStatementOnDriver("select count(*) from T");
    Assert.assertEquals("Rowcount of imported table", TestTxnCommands2.stringifyValues(rows), rs);
  }
}