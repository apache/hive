package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.BucketCodec;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TestTxnNoBuckets extends TxnCommandsBaseForTests {
  static final private Logger LOG = LoggerFactory.getLogger(TestTxnNoBuckets.class);
  private static final String TEST_DATA_DIR = new File(System.getProperty("java.io.tmpdir") +
    File.separator + TestTxnNoBuckets.class.getCanonicalName()
    + "-" + System.currentTimeMillis()
  ).getPath().replaceAll("\\\\", "/");
  @Rule
  public TestName testName = new TestName();
  @Override
  String getTestDataDir() {
    return TEST_DATA_DIR;
  }
  @Override
  @Before
  public void setUp() throws Exception {
    setUpInternal();
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
  }

  /**
   * Tests that Acid can work with un-bucketed tables.
   */
  @Test
  public void testNoBuckets() throws Exception {
    int[][] sourceVals1 = {{0,0,0},{3,3,3}};
    int[][] sourceVals2 = {{1,1,1},{2,2,2}};
    runStatementOnDriver("drop table if exists tmp");
    runStatementOnDriver("create table tmp (c1 integer, c2 integer, c3 integer) stored as orc");
    runStatementOnDriver("insert into tmp " + makeValuesClause(sourceVals1));
    runStatementOnDriver("insert into tmp " + makeValuesClause(sourceVals2));
    runStatementOnDriver("drop table if exists nobuckets");
    runStatementOnDriver("create table nobuckets (c1 integer, c2 integer, c3 integer) stored " +
      "as orc tblproperties('transactional'='true', 'transactional_properties'='default')");
    String stmt = "insert into nobuckets select * from tmp";
    runStatementOnDriver(stmt);
    List<String> rs = runStatementOnDriver(
      "select ROW__ID, c1, c2, c3, INPUT__FILE__NAME from nobuckets order by ROW__ID");
    Assert.assertEquals("", 4, rs.size());
    LOG.warn("after insert");
    for(String s : rs) {
      LOG.warn(s);
    }
    /**the insert creates 2 output files (presumably because there are 2 input files)
     * The number in the file name is writerId.  This is the number encoded in ROW__ID.bucketId -
     * see {@link org.apache.hadoop.hive.ql.io.BucketCodec}*/
    Assert.assertTrue(rs.get(0), rs.get(0).startsWith("{\"transactionid\":19,\"bucketid\":536870912,\"rowid\":0}\t0\t0\t0\t"));
    Assert.assertTrue(rs.get(0), rs.get(0).endsWith("nobuckets/delta_0000019_0000019_0000/bucket_00000"));
    Assert.assertTrue(rs.get(1), rs.get(1).startsWith("{\"transactionid\":19,\"bucketid\":536870912,\"rowid\":1}\t3\t3\t3\t"));
    Assert.assertTrue(rs.get(1), rs.get(1).endsWith("nobuckets/delta_0000019_0000019_0000/bucket_00000"));
    Assert.assertTrue(rs.get(2), rs.get(2).startsWith("{\"transactionid\":19,\"bucketid\":536936448,\"rowid\":0}\t1\t1\t1\t"));
    Assert.assertTrue(rs.get(2), rs.get(2).endsWith("nobuckets/delta_0000019_0000019_0000/bucket_00001"));
    Assert.assertTrue(rs.get(3), rs.get(3).startsWith("{\"transactionid\":19,\"bucketid\":536936448,\"rowid\":1}\t2\t2\t2\t"));
    Assert.assertTrue(rs.get(3), rs.get(3).endsWith("nobuckets/delta_0000019_0000019_0000/bucket_00001"));

    runStatementOnDriver("update nobuckets set c3 = 17 where c3 in(0,1)");
    rs = runStatementOnDriver("select ROW__ID, c1, c2, c3, INPUT__FILE__NAME from nobuckets order by INPUT__FILE__NAME, ROW__ID");
    LOG.warn("after update");
    for(String s : rs) {
      LOG.warn(s);
    }
    Assert.assertTrue(rs.get(0), rs.get(0).startsWith("{\"transactionid\":19,\"bucketid\":536870912,\"rowid\":1}\t3\t3\t3\t"));
    Assert.assertTrue(rs.get(0), rs.get(0).endsWith("nobuckets/delta_0000019_0000019_0000/bucket_00000"));
    Assert.assertTrue(rs.get(1), rs.get(1).startsWith("{\"transactionid\":19,\"bucketid\":536936448,\"rowid\":1}\t2\t2\t2\t"));
    Assert.assertTrue(rs.get(1), rs.get(1).endsWith("nobuckets/delta_0000019_0000019_0000/bucket_00001"));
    //so update has 1 writer which creates bucket0 where both new rows land
    Assert.assertTrue(rs.get(2), rs.get(2).startsWith("{\"transactionid\":21,\"bucketid\":536870912,\"rowid\":0}\t0\t0\t17\t"));
    Assert.assertTrue(rs.get(2), rs.get(2).endsWith("nobuckets/delta_0000021_0000021_0000/bucket_00000"));
    Assert.assertTrue(rs.get(3), rs.get(3).startsWith("{\"transactionid\":21,\"bucketid\":536870912,\"rowid\":1}\t1\t1\t17\t"));
    Assert.assertTrue(rs.get(3), rs.get(3).endsWith("nobuckets/delta_0000021_0000021_0000/bucket_00000"));

    Set<String> expectedFiles = new HashSet<>();
    //both delete events land in a single bucket0.  Each has a different ROW__ID.bucketId value (even writerId in it is different)
    expectedFiles.add("ts/delete_delta_0000021_0000021_0000/bucket_00000");
    expectedFiles.add("nobuckets/delta_0000019_0000019_0000/bucket_00000");
    expectedFiles.add("nobuckets/delta_0000019_0000019_0000/bucket_00001");
    expectedFiles.add("nobuckets/delta_0000021_0000021_0000/bucket_00000");
    //check that we get the right files on disk
    assertExpectedFileSet(expectedFiles, getWarehouseDir() + "/nobuckets");
    //todo: it would be nice to check the contents of the files... could use orc.FileDump - it has
    // methods to print to a supplied stream but those are package private

    runStatementOnDriver("alter table nobuckets compact 'major'");
    TestTxnCommands2.runWorker(hiveConf);
    rs = runStatementOnDriver("select ROW__ID, c1, c2, c3, INPUT__FILE__NAME from nobuckets order by INPUT__FILE__NAME, ROW__ID");
    LOG.warn("after major compact");
    for(String s : rs) {
      LOG.warn(s);
    }
    /*
├── base_0000021
│   ├── bucket_00000
│   └── bucket_00001
├── delete_delta_0000021_0000021_0000
│   └── bucket_00000
├── delta_0000019_0000019_0000
│   ├── bucket_00000
│   └── bucket_00001
└── delta_0000021_0000021_0000
    └── bucket_00000
    */
    Assert.assertTrue(rs.get(0), rs.get(0).startsWith("{\"transactionid\":19,\"bucketid\":536870912,\"rowid\":1}\t3\t3\t3\t"));
    Assert.assertTrue(rs.get(0), rs.get(0).endsWith("nobuckets/base_0000021/bucket_00000"));
    Assert.assertTrue(rs.get(1), rs.get(1).startsWith("{\"transactionid\":21,\"bucketid\":536870912,\"rowid\":0}\t0\t0\t17\t"));
    Assert.assertTrue(rs.get(1), rs.get(1).endsWith("nobuckets/base_0000021/bucket_00000"));
    Assert.assertTrue(rs.get(2), rs.get(2).startsWith("{\"transactionid\":21,\"bucketid\":536870912,\"rowid\":1}\t1\t1\t17\t"));
    Assert.assertTrue(rs.get(2), rs.get(2).endsWith("nobuckets/base_0000021/bucket_00000"));
    Assert.assertTrue(rs.get(3), rs.get(3).startsWith("{\"transactionid\":19,\"bucketid\":536936448,\"rowid\":1}\t2\t2\t2\t"));
    Assert.assertTrue(rs.get(3), rs.get(3).endsWith("nobuckets/base_0000021/bucket_00001"));

    expectedFiles.clear();
    expectedFiles.add("delete_delta_0000021_0000021_0000/bucket_00000");
    expectedFiles.add("uckets/delta_0000019_0000019_0000/bucket_00000");
    expectedFiles.add("uckets/delta_0000019_0000019_0000/bucket_00001");
    expectedFiles.add("uckets/delta_0000021_0000021_0000/bucket_00000");
    expectedFiles.add("/warehouse/nobuckets/base_0000021/bucket_00000");
    expectedFiles.add("/warehouse/nobuckets/base_0000021/bucket_00001");
    assertExpectedFileSet(expectedFiles, getWarehouseDir() + "/nobuckets");

    TestTxnCommands2.runCleaner(hiveConf);
    rs = runStatementOnDriver("select c1, c2, c3 from nobuckets order by c1, c2, c3");
    int[][] result = {{0,0,17},{1,1,17},{2,2,2},{3,3,3}};
    Assert.assertEquals("Unexpected result after clean", stringifyValues(result), rs);

    expectedFiles.clear();
    expectedFiles.add("nobuckets/base_0000021/bucket_00000");
    expectedFiles.add("nobuckets/base_0000021/bucket_00001");
    assertExpectedFileSet(expectedFiles, getWarehouseDir() + "/nobuckets");
  }

  /**
   * See CTAS tests in TestAcidOnTez
   */
  @Test
  public void testCTAS() throws Exception {
    int[][] values = {{1,2},{3,4}};
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL +  makeValuesClause(values));
    runStatementOnDriver("create table myctas stored as ORC TBLPROPERTIES ('transactional" +
      "'='true', 'transactional_properties'='default') as select a, b from " + Table.NONACIDORCTBL);
    List<String> rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from myctas order by ROW__ID");
    String expected[][] = {
      {"{\"transactionid\":14,\"bucketid\":536870912,\"rowid\":0}\t3\t4", "warehouse/myctas/delta_0000014_0000014_0000/bucket_00000"},
      {"{\"transactionid\":14,\"bucketid\":536870912,\"rowid\":1}\t1\t2", "warehouse/myctas/delta_0000014_0000014_0000/bucket_00000"},
    };
    checkExpected(rs, expected, "Unexpected row count after ctas from non acid table");

    runStatementOnDriver("insert into " + Table.ACIDTBL + makeValuesClause(values));
    runStatementOnDriver("create table myctas2 stored as ORC TBLPROPERTIES ('transactional" +
      "'='true', 'transactional_properties'='default') as select a, b from " + Table.ACIDTBL);
    rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from myctas2 order by ROW__ID");
    String expected2[][] = {
      {"{\"transactionid\":17,\"bucketid\":536870912,\"rowid\":0}\t3\t4", "warehouse/myctas2/delta_0000017_0000017_0000/bucket_00000"},
      {"{\"transactionid\":17,\"bucketid\":536870912,\"rowid\":1}\t1\t2", "warehouse/myctas2/delta_0000017_0000017_0000/bucket_00000"},
    };
    checkExpected(rs, expected2, "Unexpected row count after ctas from acid table");

    runStatementOnDriver("create table myctas3 stored as ORC TBLPROPERTIES ('transactional" +
      "'='true', 'transactional_properties'='default') as select a, b from " + Table.NONACIDORCTBL +
      " union all select a, b from " + Table.ACIDTBL);
    rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from myctas3 order by ROW__ID");
    String expected3[][] = {
      {"{\"transactionid\":19,\"bucketid\":536870912,\"rowid\":0}\t3\t4", "warehouse/myctas3/delta_0000019_0000019_0000/bucket_00000"},
      {"{\"transactionid\":19,\"bucketid\":536870912,\"rowid\":1}\t1\t2", "warehouse/myctas3/delta_0000019_0000019_0000/bucket_00000"},
      {"{\"transactionid\":19,\"bucketid\":536936448,\"rowid\":0}\t3\t4", "warehouse/myctas3/delta_0000019_0000019_0000/bucket_00001"},
      {"{\"transactionid\":19,\"bucketid\":536936448,\"rowid\":1}\t1\t2", "warehouse/myctas3/delta_0000019_0000019_0000/bucket_00001"},
    };
    checkExpected(rs, expected3, "Unexpected row count after ctas from union all query");

    runStatementOnDriver("create table myctas4 stored as ORC TBLPROPERTIES ('transactional" +
      "'='true', 'transactional_properties'='default') as select a, b from " + Table.NONACIDORCTBL +
      " union distinct select a, b from " + Table.ACIDTBL);
    rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from myctas4 order by ROW__ID");
    String expected4[][] = {
      {"{\"transactionid\":21,\"bucketid\":536870912,\"rowid\":0}\t1\t2", "/delta_0000021_0000021_0000/bucket_00000"},
      {"{\"transactionid\":21,\"bucketid\":536870912,\"rowid\":1}\t3\t4", "/delta_0000021_0000021_0000/bucket_00000"},
    };
    checkExpected(rs, expected4, "Unexpected row count after ctas from union distinct query");
  }
  /**
   * Insert into unbucketed acid table from union all query
   * Union All is flattend so nested subdirs are created and acid move drops them since
   * delta dirs have unique names
   */
  @Test
  public void testInsertToAcidWithUnionRemove() throws Exception {
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_OPTIMIZE_UNION_REMOVE, true);
    hiveConf.setVar(HiveConf.ConfVars.HIVEFETCHTASKCONVERSION, "none");
    d.close();
    d = new Driver(hiveConf);
    int[][] values = {{1,2},{3,4},{5,6},{7,8},{9,10}};
    runStatementOnDriver("insert into " + TxnCommandsBaseForTests.Table.ACIDTBL + makeValuesClause(values));//HIVE-17138: this creates 1 delta_0000013_0000013_0000/bucket_00001
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("create table T (a int, b int) stored as ORC  TBLPROPERTIES ('transactional'='true')");
    /*
    So Union All removal kicks in and we get 3 subdirs in staging.
ekoifman:apache-hive-3.0.0-SNAPSHOT-bin ekoifman$ tree /Users/ekoifman/dev/hiverwgit/ql/target/tmp/org.apache.hadoop.hive.ql.TestTxnNoBuckets-1505516390532/warehouse/t/.hive-staging_hive_2017-09-15_16-05-06_895_1123322677843388168-1/
└── -ext-10000
    ├── HIVE_UNION_SUBDIR_19
    │   └── 000000_0
    │       ├── _orc_acid_version
    │       └── delta_0000016_0000016_0001
    ├── HIVE_UNION_SUBDIR_20
    │   └── 000000_0
    │       ├── _orc_acid_version
    │       └── delta_0000016_0000016_0002
    └── HIVE_UNION_SUBDIR_21
        └── 000000_0
            ├── _orc_acid_version
            └── delta_0000016_0000016_0003*/
    runStatementOnDriver("insert into T(a,b) select a, b from " + TxnCommandsBaseForTests.Table.ACIDTBL + " where a between 1 and 3 group by a, b union all select a, b from " + TxnCommandsBaseForTests.Table.ACIDTBL + " where a between 5 and 7 union all select a, b from " + TxnCommandsBaseForTests.Table.ACIDTBL + " where a >= 9");

    List<String> rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from T order by ROW__ID");

    String expected[][] = {
      {"{\"transactionid\":16,\"bucketid\":536870913,\"rowid\":0}\t1\t2", "/delta_0000016_0000016_0001/bucket_00000"},
      {"{\"transactionid\":16,\"bucketid\":536870913,\"rowid\":1}\t3\t4", "/delta_0000016_0000016_0001/bucket_00000"},
      {"{\"transactionid\":16,\"bucketid\":536870914,\"rowid\":0}\t7\t8", "/delta_0000016_0000016_0002/bucket_00000"},
      {"{\"transactionid\":16,\"bucketid\":536870914,\"rowid\":1}\t5\t6", "/delta_0000016_0000016_0002/bucket_00000"},
      {"{\"transactionid\":16,\"bucketid\":536870915,\"rowid\":0}\t9\t10", "/delta_0000016_0000016_0003/bucket_00000"},
    };
    checkExpected(rs, expected, "Unexpected row count after ctas");
  }
  private void checkExpected(List<String> rs, String[][] expected, String msg) {
    LOG.warn(testName.getMethodName() + ": read data(" + msg + "): ");
    for(String s : rs) {
      LOG.warn(s);
    }
    Assert.assertEquals( testName.getMethodName() + ": " + msg, expected.length, rs.size());
    //verify data and layout
    for(int i = 0; i < expected.length; i++) {
      Assert.assertTrue("Actual line " + i + " bc: " + rs.get(i), rs.get(i).startsWith(expected[i][0]));
      Assert.assertTrue("Actual line(file) " + i + " bc: " + rs.get(i), rs.get(i).endsWith(expected[i][1]));
    }
  }
  /**
   * The idea here is to create a non acid table that was written by multiple writers, i.e.
   * unbucketed table that has 000000_0 & 000001_0, for example.  Unfortunately this doesn't work
   * due to 'merge' logic - see comments in the method
   */
  @Ignore
  @Test
  public void testToAcidConversionMultiBucket() throws Exception {
    int[][] values = {{1,2},{2,4},{5,6},{6,8},{9,10}};
    runStatementOnDriver("insert into " + Table.ACIDTBL + makeValuesClause(values));
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("create table T (a int, b int) stored as ORC  TBLPROPERTIES ('transactional'='false')");
    /*T non-acid + non bucketd - 3 writers are created and then followed by merge to create a single output file
    though how the data from union is split between writers is a mystery
    (bucketed tables don't do merge)
   Processing data file file:/Users/ekoifman/dev/hiverwgit/ql/target/tmp/org.apache.hadoop.hive.ql.TestTxnNoBuckets-1505317179157/warehouse/t/.hive-staging_hive_2017-09-13_08-40-30_275_8623609103176711840-1/-ext-10000/000000_0 [length: 515]
{"a":6,"b":8}
{"a":9,"b":10}
{"a":5,"b":6}
{"a":1,"b":2}
{"a":2,"b":4}
________________________________________________________________________________________________________________________

Processing data file file:/Users/ekoifman/dev/hiverwgit/ql/target/tmp/org.apache.hadoop.hive.ql.TestTxnNoBuckets-1505317179157/warehouse/t/.hive-staging_hive_2017-09-13_08-40-30_275_8623609103176711840-1/-ext-10003/000000_0 [length: 242]
{"a":6,"b":8}
________________________________________________________________________________________________________________________

Processing data file file:/Users/ekoifman/dev/hiverwgit/ql/target/tmp/org.apache.hadoop.hive.ql.TestTxnNoBuckets-1505317179157/warehouse/t/.hive-staging_hive_2017-09-13_08-40-30_275_8623609103176711840-1/-ext-10003/000001_0 [length: 244]
{"a":9,"b":10}
{"a":5,"b":6}
________________________________________________________________________________________________________________________

Processing data file file:/Users/ekoifman/dev/hiverwgit/ql/target/tmp/org.apache.hadoop.hive.ql.TestTxnNoBuckets-1505317179157/warehouse/t/.hive-staging_hive_2017-09-13_08-40-30_275_8623609103176711840-1/-ext-10003/000002_0 [length: 242]
{"a":1,"b":2}
{"a":2,"b":4}
 */
    runStatementOnDriver("insert into T(a,b) select a, b from " + Table.ACIDTBL + " where a between 1 and 3 group by a, b union all select a, b from " + Table.ACIDTBL + " where a between 5 and 7 union all select a, b from " + Table.ACIDTBL + " where a >= 9");
    List<String> rs = runStatementOnDriver("select a, b, INPUT__FILE__NAME from T order by a, b, INPUT__FILE__NAME");
    LOG.warn("before converting to acid");
    for(String s : rs) {
      LOG.warn(s);
    }
  }
  @Test
  public void testInsertFromUnion() throws Exception {
    int[][] values = {{1,2},{2,4},{5,6},{6,8},{9,10}};
    runStatementOnDriver("insert into " + Table.NONACIDNONBUCKET + makeValuesClause(values));
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("create table T (a int, b int) stored as ORC  TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("insert into T(a,b) select a, b from " + Table.NONACIDNONBUCKET + " where a between 1 and 3 group by a, b union all select a, b from " + Table.NONACIDNONBUCKET + " where a between 5 and 7 union all select a, b from " + Table.NONACIDNONBUCKET + " where a >= 9");
    List<String> rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from T order by a, b, INPUT__FILE__NAME");
    LOG.warn("before converting to acid");
    for(String s : rs) {
      LOG.warn(s);
    }
    /*
    The number of writers seems to be based on number of MR jobs for the src query.  todo check number of FileSinks
    warehouse/t/.hive-staging_hive_2017-09-13_08-59-28_141_6304543600372946004-1/-ext-10000/000000_0/delta_0000016_0000016_0000/bucket_00000 [length: 648]
    {"operation":0,"originalTransaction":16,"bucket":536870912,"rowId":0,"currentTransaction":16,"row":{"_col0":1,"_col1":2}}
    {"operation":0,"originalTransaction":16,"bucket":536870912,"rowId":1,"currentTransaction":16,"row":{"_col0":2,"_col1":4}}
    ________________________________________________________________________________________________________________________
    warehouse/t/.hive-staging_hive_2017-09-13_08-59-28_141_6304543600372946004-1/-ext-10000/000001_0/delta_0000016_0000016_0000/bucket_00001 [length: 658]
    {"operation":0,"originalTransaction":16,"bucket":536936448,"rowId":0,"currentTransaction":16,"row":{"_col0":5,"_col1":6}}
    {"operation":0,"originalTransaction":16,"bucket":536936448,"rowId":1,"currentTransaction":16,"row":{"_col0":6,"_col1":8}}
    {"operation":0,"originalTransaction":16,"bucket":536936448,"rowId":2,"currentTransaction":16,"row":{"_col0":9,"_col1":10}}
    */
    rs = runStatementOnDriver("select a, b from T order by a, b");
    Assert.assertEquals(stringifyValues(values), rs);
    rs = runStatementOnDriver("select ROW__ID from T group by ROW__ID having count(*) > 1");
    if(rs.size() > 0) {
      Assert.assertEquals("Duplicate ROW__IDs: " + rs.get(0), 0, rs.size());
    }
  }
  /**
   * see HIVE-16177
   * See also {@link TestTxnCommands2#testNonAcidToAcidConversion02()}
   */
  @Test
  public void testToAcidConversion02() throws Exception {
    //create 2 rows in a file 00000_0
    runStatementOnDriver("insert into " + Table.NONACIDNONBUCKET + "(a,b) values(1,2),(1,3)");
    //create 4 rows in a file 000000_0_copy_1
    runStatementOnDriver("insert into " + Table.NONACIDNONBUCKET + "(a,b) values(0,12),(0,13),(1,4),(1,5)");
    //create 1 row in a file 000000_0_copy_2
    runStatementOnDriver("insert into " + Table.NONACIDNONBUCKET + "(a,b) values(1,6)");

    //convert the table to Acid  //todo: remove trans_prop after HIVE-17089
    runStatementOnDriver("alter table " + Table.NONACIDNONBUCKET + " SET TBLPROPERTIES ('transactional'='true', 'transactional_properties'='default')");
    List<String> rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from " +  Table.NONACIDNONBUCKET + " order by ROW__ID");
    LOG.warn("before acid ops (after convert)");
    for(String s : rs) {
      LOG.warn(s);
    }
    //create a some of delta directories
    runStatementOnDriver("insert into " + Table.NONACIDNONBUCKET + "(a,b) values(0,15),(1,16)");
    runStatementOnDriver("update " + Table.NONACIDNONBUCKET + " set b = 120 where a = 0 and b = 12");
    runStatementOnDriver("insert into " + Table.NONACIDNONBUCKET + "(a,b) values(0,17)");
    runStatementOnDriver("delete from " + Table.NONACIDNONBUCKET + " where a = 1 and b = 3");

    rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from " +  Table.NONACIDNONBUCKET + " order by a,b");
    LOG.warn("before compact");
    for(String s : rs) {
      LOG.warn(s);
    }
    Assert.assertEquals(0, BucketCodec.determineVersion(536870912).decodeWriterId(536870912));
    /*
     * All ROW__IDs are unique on read after conversion to acid
     * ROW__IDs are exactly the same before and after compaction
     * Also check the file name (only) after compaction for completeness
     */
    String[][] expected = {
      {"{\"transactionid\":0,\"bucketid\":536870912,\"rowid\":3}\t0\t13",  "bucket_00000", "000000_0_copy_1"},
      {"{\"transactionid\":18,\"bucketid\":536870912,\"rowid\":0}\t0\t15", "bucket_00000", "bucket_00000"},
      {"{\"transactionid\":20,\"bucketid\":536870912,\"rowid\":0}\t0\t17", "bucket_00000", "bucket_00000"},
      {"{\"transactionid\":19,\"bucketid\":536870912,\"rowid\":0}\t0\t120", "bucket_00000", "bucket_00000"},
      {"{\"transactionid\":0,\"bucketid\":536870912,\"rowid\":0}\t1\t2",   "bucket_00000", "000000_0"},
      {"{\"transactionid\":0,\"bucketid\":536870912,\"rowid\":4}\t1\t4",   "bucket_00000", "000000_0_copy_1"},
      {"{\"transactionid\":0,\"bucketid\":536870912,\"rowid\":5}\t1\t5",   "bucket_00000", "000000_0_copy_1"},
      {"{\"transactionid\":0,\"bucketid\":536870912,\"rowid\":6}\t1\t6",   "bucket_00000", "000000_0_copy_2"},
      {"{\"transactionid\":18,\"bucketid\":536870912,\"rowid\":1}\t1\t16", "bucket_00000", "bucket_00000"}
    };
    Assert.assertEquals("Unexpected row count before compaction", expected.length, rs.size());
    for(int i = 0; i < expected.length; i++) {
      Assert.assertTrue("Actual line " + i + " bc: " + rs.get(i), rs.get(i).startsWith(expected[i][0]));
      Assert.assertTrue("Actual line(file) " + i + " bc: " + rs.get(i), rs.get(i).endsWith(expected[i][2]));
    }
    //run Compaction
    runStatementOnDriver("alter table "+ Table.NONACIDNONBUCKET +" compact 'major'");
    TestTxnCommands2.runWorker(hiveConf);
    /*
    nonacidnonbucket/
    ├── 000000_0
    ├── 000000_0_copy_1
    ├── 000000_0_copy_2
    ├── base_0000021
    │   └── bucket_00000
    ├── delete_delta_0000019_0000019_0000
    │   └── bucket_00000
    ├── delete_delta_0000021_0000021_0000
    │   └── bucket_00000
    ├── delta_0000018_0000018_0000
    │   └── bucket_00000
    ├── delta_0000019_0000019_0000
    │   └── bucket_00000
    └── delta_0000020_0000020_0000
        └── bucket_00000

    6 directories, 9 files
    */
    rs = runStatementOnDriver("select ROW__ID, a, b, INPUT__FILE__NAME from " + Table.NONACIDNONBUCKET + " order by a,b");
    LOG.warn("after compact");
    for(String s : rs) {
      LOG.warn(s);
    }
    Assert.assertEquals("Unexpected row count after compaction", expected.length, rs.size());
    for(int i = 0; i < expected.length; i++) {
      Assert.assertTrue("Actual line " + i + " ac: " + rs.get(i), rs.get(i).startsWith(expected[i][0]));
      Assert.assertTrue("Actual line(file) " + i + " ac: " + rs.get(i), rs.get(i).endsWith(expected[i][1]));
    }
    //make sure they are the same before and after compaction
  }
  /**
   * Currently CTAS doesn't support bucketed tables.  Correspondingly Acid only supports CTAS for
   * unbucketed tables.  This test is here to make sure that if CTAS is made to support unbucketed
   * tables, that it raises a red flag for Acid.
   */
  @Test
  public void testCtasBucketed() throws Exception {
    runStatementOnDriver("insert into " + Table.NONACIDNONBUCKET + "(a,b) values(1,2),(1,3)");
    CommandProcessorResponse cpr = runStatementOnDriverNegative("create table myctas " +
      "clustered by (a) into 2 buckets stored as ORC TBLPROPERTIES ('transactional'='true') as " +
      "select a, b from " + Table.NONACIDORCTBL);
    int j = ErrorMsg.CTAS_PARCOL_COEXISTENCE.getErrorCode();//this code doesn't propagate
//    Assert.assertEquals("Wrong msg", ErrorMsg.CTAS_PARCOL_COEXISTENCE.getErrorCode(), cpr.getErrorCode());
    Assert.assertTrue(cpr.getErrorMessage().contains("CREATE-TABLE-AS-SELECT does not support"));
  }
}

