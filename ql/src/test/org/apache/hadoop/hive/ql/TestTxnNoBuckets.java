package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.BucketCodec;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
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
    /*todo: WTF?
    RS for update seems to spray randomly... is that OK?  maybe as long as all resultant files have different names... will they?
    Assuming we name them based on taskId, we should create bucketX and bucketY.
    we delete events can be written to bucketX file it could be useful for filter delete for a split by file name since the insert
    events seem to be written to a proper bucketX file.  In fact this may reduce the number of changes elsewhere like compactor... maybe
    But this limits the parallelism - what is worse, you don't know what the parallelism should be until you have a list of all the
    input files since bucket count is no longer a metadata property.  Also, with late Update split, the file name has already been determined
    from taskId so the Insert part won't end up matching the bucketX property necessarily.
    With early Update split, the Insert can still be an insert - i.e. go to appropriate bucketX.  But deletes will still go wherever (random shuffle)
    unless you know all the bucketX files to be read - may not be worth the trouble.
    * 2nd: something in FS fails.  ArrayIndexOutOfBoundsException: 1 at FileSinkOperator.process(FileSinkOperator.java:779)*/
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
   * all of these pass but don't do exactly the right thing
   * files land as if it's not an acid table "warehouse/myctas4/000000_0"
   * even though in {@link org.apache.hadoop.hive.metastore.TransactionalValidationListener} fires
   * and sees it as transactional table
   * look for QB.isCTAS() and CreateTableDesc() in SemanticAnalyzer
   *
   * On read, these files are treated like non acid to acid conversion
   *
   * see HIVE-15899
   * See CTAS tests in TestAcidOnTez
   */
  @Test
  public void testCTAS() throws Exception {
    int[][] values = {{1,2},{3,4}};
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL +  makeValuesClause(values));
    runStatementOnDriver("create table myctas stored as ORC TBLPROPERTIES ('transactional" +
      "'='true', 'transactional_properties'='default') as select a, b from " + Table.NONACIDORCTBL);
    List<String> rs = runStatementOnDriver("select * from myctas order by a, b");
    Assert.assertEquals(stringifyValues(values), rs);

    runStatementOnDriver("insert into " + Table.ACIDTBL + makeValuesClause(values));
    runStatementOnDriver("create table myctas2 stored as ORC TBLPROPERTIES ('transactional" +
      "'='true', 'transactional_properties'='default') as select a, b from " + Table.ACIDTBL);
    rs = runStatementOnDriver("select * from myctas2 order by a, b");
    Assert.assertEquals(stringifyValues(values), rs);

    runStatementOnDriver("create table myctas3 stored as ORC TBLPROPERTIES ('transactional" +
      "'='true', 'transactional_properties'='default') as select a, b from " + Table.NONACIDORCTBL +
      " union all select a, b from " + Table.ACIDTBL);
    rs = runStatementOnDriver("select * from myctas3 order by a, b");
    Assert.assertEquals(stringifyValues(new int[][] {{1,2},{1,2},{3,4},{3,4}}), rs);

    runStatementOnDriver("create table myctas4 stored as ORC TBLPROPERTIES ('transactional" +
      "'='true', 'transactional_properties'='default') as select a, b from " + Table.NONACIDORCTBL +
      " union distinct select a, b from " + Table.ACIDTBL);
    rs = runStatementOnDriver("select * from myctas4 order by a, b");
    Assert.assertEquals(stringifyValues(values), rs);
  }
  /**
   * see HIVE-16177
   * See also {@link TestTxnCommands2#testNonAcidToAcidConversion02()}  todo need test with > 1 bucket file
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

