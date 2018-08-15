package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class TestTxnConcatenate extends TxnCommandsBaseForTests {
  static final private Logger LOG = LoggerFactory.getLogger(TestTxnConcatenate.class);
  private static final String TEST_DATA_DIR =
      new File(System.getProperty("java.io.tmpdir") +
          File.separator + TestTxnLoadData.class.getCanonicalName()
          + "-" + System.currentTimeMillis()
      ).getPath().replaceAll("\\\\", "/");
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Override
  protected String getTestDataDir() {
    return TEST_DATA_DIR;
  }

  @Test
  public void testConcatenate() throws Exception {
    runStatementOnDriver("insert into " + Table.ACIDTBL + " values(1,2),(4,5)");
    runStatementOnDriver("update " + Table.ACIDTBL + " set b = 4");
    runStatementOnDriver("insert into " + Table.ACIDTBL + " values(5,6),(8,8)");
    String testQuery = "select ROW__ID, a, b, INPUT__FILE__NAME from " + Table.ACIDTBL + " order by a, b";
    String[][] expected = new String[][] {
        {"{\"writeid\":2,\"bucketid\":536936448,\"rowid\":1}\t1\t4",
            "acidtbl/delta_0000002_0000002_0000/bucket_00001"},
        {"{\"writeid\":2,\"bucketid\":536936448,\"rowid\":0}\t4\t4",
            "acidtbl/delta_0000002_0000002_0000/bucket_00001"},
        {"{\"writeid\":3,\"bucketid\":536936448,\"rowid\":1}\t5\t6",
            "acidtbl/delta_0000003_0000003_0000/bucket_00001"},
        {"{\"writeid\":3,\"bucketid\":536936448,\"rowid\":0}\t8\t8",
            "acidtbl/delta_0000003_0000003_0000/bucket_00001"}};
    checkResult(expected, testQuery, false, "check data", LOG);

    /*in UTs, there is no standalone HMS running to kick off compaction so it's done via runWorker()
     but in normal usage 'concatenate' is blocking, */
    hiveConf.setBoolVar(HiveConf.ConfVars.TRANSACTIONAL_CONCATENATE_NOBLOCK, true);
    runStatementOnDriver("alter table " + Table.ACIDTBL + " concatenate");

    TxnStore txnStore = TxnUtils.getTxnStore(hiveConf);
    ShowCompactResponse rsp = txnStore.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.INITIATED_RESPONSE, rsp.getCompacts().get(0).getState());
    runWorker(hiveConf);
    rsp = txnStore.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.CLEANING_RESPONSE, rsp.getCompacts().get(0).getState());
    String[][] expected2 = new String[][] {
        {"{\"writeid\":2,\"bucketid\":536936448,\"rowid\":1}\t1\t4",
            "acidtbl/base_0000003/bucket_00001"},
        {"{\"writeid\":2,\"bucketid\":536936448,\"rowid\":0}\t4\t4",
            "acidtbl/base_0000003/bucket_00001"},
        {"{\"writeid\":3,\"bucketid\":536936448,\"rowid\":1}\t5\t6",
            "acidtbl/base_0000003/bucket_00001"},
        {"{\"writeid\":3,\"bucketid\":536936448,\"rowid\":0}\t8\t8",
            "acidtbl/base_0000003/bucket_00001"}};
    checkResult(expected2, testQuery, false, "check data after concatenate", LOG);
  }
  @Test
  public void testConcatenatePart() throws Exception {
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " values(1,2,'p1'),(4,5,'p2')");
    runStatementOnDriver("update " + Table.ACIDTBLPART + " set b = 4 where p='p1'");
    runStatementOnDriver("insert into " + Table.ACIDTBLPART + " values(5,6,'p1'),(8,8,'p2')");
    String testQuery = "select ROW__ID, a, b, INPUT__FILE__NAME from " + Table.ACIDTBLPART + " order by a, b";
    String[][] expected = new String[][] {
        {"{\"writeid\":2,\"bucketid\":536936448,\"rowid\":0}\t1\t4",
            "acidtblpart/p=p1/delta_0000002_0000002_0000/bucket_00001"},
        {"{\"writeid\":1,\"bucketid\":536936448,\"rowid\":0}\t4\t5",
            "acidtblpart/p=p2/delta_0000001_0000001_0000/bucket_00001"},
        {"{\"writeid\":3,\"bucketid\":536936448,\"rowid\":0}\t5\t6",
            "acidtblpart/p=p1/delta_0000003_0000003_0000/bucket_00001"},
        {"{\"writeid\":3,\"bucketid\":536936448,\"rowid\":0}\t8\t8",
            "acidtblpart/p=p2/delta_0000003_0000003_0000/bucket_00001"}};
    checkResult(expected, testQuery, false, "check data", LOG);

    /*in UTs, there is no standalone HMS running to kick off compaction so it's done via runWorker()
     but in normal usage 'concatenate' is blocking, */
    hiveConf.setBoolVar(HiveConf.ConfVars.TRANSACTIONAL_CONCATENATE_NOBLOCK, true);
    runStatementOnDriver("alter table " + Table.ACIDTBLPART + " PARTITION(p='p1') concatenate");

    TxnStore txnStore = TxnUtils.getTxnStore(hiveConf);
    ShowCompactResponse rsp = txnStore.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.INITIATED_RESPONSE, rsp.getCompacts().get(0).getState());
    runWorker(hiveConf);
    rsp = txnStore.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.CLEANING_RESPONSE, rsp.getCompacts().get(0).getState());
    String[][] expected2 = new String[][] {
        {"{\"writeid\":2,\"bucketid\":536936448,\"rowid\":0}\t1\t4",
            "acidtblpart/p=p1/base_0000003/bucket_00001"},
        {"{\"writeid\":1,\"bucketid\":536936448,\"rowid\":0}\t4\t5",
            "acidtblpart/p=p2/delta_0000001_0000001_0000/bucket_00001"},
        {"{\"writeid\":3,\"bucketid\":536936448,\"rowid\":0}\t5\t6",
            "acidtblpart/p=p1/base_0000003/bucket_00001"},
        {"{\"writeid\":3,\"bucketid\":536936448,\"rowid\":0}\t8\t8",
            "acidtblpart/p=p2/delta_0000003_0000003_0000/bucket_00001"}};

    checkResult(expected2, testQuery, false, "check data after concatenate", LOG);
  }

  @Test
  public void testTruncate() throws Exception {
    MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.CREATE_TABLES_AS_ACID, true);
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("create table T(a int, b int) stored as orc");
    runStatementOnDriver("insert into T values(1,2)");
    runStatementOnDriver("truncate table T");
  }
    @Test
  public void testConcatenateMM() throws Exception {
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_CREATE_TABLES_AS_INSERT_ONLY, true);
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("create table T(a int, b int)");
    runStatementOnDriver("insert into T values(1,2),(4,5)");
    runStatementOnDriver("insert into T values(5,6),(8,8)");
    String testQuery = "select a, b, INPUT__FILE__NAME from T order by a, b";
    String[][] expected = new String[][] {
        {"1\t2",
            "t/delta_0000001_0000001_0000/000000_0"},
        {"4\t5",
            "t/delta_0000001_0000001_0000/000000_0"},
        {"5\t6",
            "t/delta_0000002_0000002_0000/000000_0"},
        {"8\t8",
            "t/delta_0000002_0000002_0000/000000_0"}};
    checkResult(expected, testQuery, false, "check data", LOG);

        /*in UTs, there is no standalone HMS running to kick off compaction so it's done via runWorker()
     but in normal usage 'concatenate' is blocking, */
    hiveConf.setBoolVar(HiveConf.ConfVars.TRANSACTIONAL_CONCATENATE_NOBLOCK, true);
    runStatementOnDriver("alter table T concatenate");

    TxnStore txnStore = TxnUtils.getTxnStore(hiveConf);
    ShowCompactResponse rsp = txnStore.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.INITIATED_RESPONSE, rsp.getCompacts().get(0).getState());
    runWorker(hiveConf);
    rsp = txnStore.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.CLEANING_RESPONSE, rsp.getCompacts().get(0).getState());
    String[][] expected2 = new String[][] {
        {"1\t2",
            "t/base_0000002/000000_0"},
        {"4\t5",
            "t/base_0000002/000000_0"},
        {"5\t6",
            "t/base_0000002/000000_0"},
        {"8\t8",
            "t/base_0000002/000000_0"}};
    checkResult(expected2, testQuery, false, "check data after concatenate", LOG);
  }
}
