package org.apache.hadoop.hive.ql.txn.compactor;

import junit.framework.Assert;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreThread;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.CompactionTxnHandler;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.streaming.DelimitedInputWriter;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.StreamingConnection;
import org.apache.hive.hcatalog.streaming.TransactionBatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class TestCompactor {
  private static final Logger LOG = LoggerFactory.getLogger(TestCompactor.class);
  private static final String TEST_DATA_DIR = HCatUtil.makePathASafeFileName(System.getProperty("java.io.tmpdir") +
    File.separator + TestCompactor.class.getCanonicalName() + "-" + System.currentTimeMillis());
  private static final String BASIC_FILE_NAME = TEST_DATA_DIR + "/basic.input.data";
  private static final String TEST_WAREHOUSE_DIR = TEST_DATA_DIR + "/warehouse";

  @Rule
  public TemporaryFolder stagingFolder = new TemporaryFolder();
  private HiveConf conf;
  IMetaStoreClient msClient;
  private Driver driver;

  @Before
  public void setup() throws Exception {

    File f = new File(TEST_WAREHOUSE_DIR);
    if (f.exists()) {
      FileUtil.fullyDelete(f);
    }
    if(!(new File(TEST_WAREHOUSE_DIR).mkdirs())) {
      throw new RuntimeException("Could not create " + TEST_WAREHOUSE_DIR);
    }

    HiveConf hiveConf = new HiveConf(this.getClass());
    hiveConf.setVar(HiveConf.ConfVars.PREEXECHOOKS, "");
    hiveConf.setVar(HiveConf.ConfVars.POSTEXECHOOKS, "");
    hiveConf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, TEST_WAREHOUSE_DIR);
    hiveConf.setVar(HiveConf.ConfVars.HIVEINPUTFORMAT, HiveInputFormat.class.getName());
    //"org.apache.hadoop.hive.ql.io.HiveInputFormat"

    TxnDbUtil.setConfValues(hiveConf);
    TxnDbUtil.cleanDb();
    TxnDbUtil.prepDb();

    conf = hiveConf;
    msClient = new HiveMetaStoreClient(conf);
    driver = new Driver(hiveConf);
    SessionState.start(new CliSessionState(hiveConf));


    int LOOP_SIZE = 3;
    String[] input = new String[LOOP_SIZE * LOOP_SIZE];
    int k = 0;
    for (int i = 1; i <= LOOP_SIZE; i++) {
      String si = i + "";
      for (int j = 1; j <= LOOP_SIZE; j++) {
        String sj = "S" + j + "S";
        input[k] = si + "\t" + sj;
        k++;
      }
    }
    createTestDataFile(BASIC_FILE_NAME, input);
  }
  @After
  public void tearDown() {
    conf = null;
    if(msClient != null) {
      msClient.close();
    }
    if(driver != null) {
      driver.close();
    }
  }
  
  /**
   * After each major compaction, stats need to be updated on each column of the
   * table/partition which previously had stats.
   * 1. create a bucketed ORC backed table (Orc is currently required by ACID)
   * 2. populate 2 partitions with data
   * 3. compute stats
   * 4. insert some data into the table using StreamingAPI
   * 5. Trigger major compaction (which should update stats)
   * 6. check that stats have been updated
   * @throws Exception
   * todo: 
   * 2. add non-partitioned test
   * 4. add a test with sorted table?
   */
  @Test
  public void testStatsAfterCompactionPartTbl() throws Exception {
    //as of (8/27/2014) Hive 0.14, ACID/Orc requires HiveInputFormat
    String tblName = "compaction_test";
    String tblNameStg = tblName + "_stg";
    List<String> colNames = Arrays.asList("a", "b");
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("drop table if exists " + tblNameStg, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " PARTITIONED BY(bkt INT)" +
      " CLUSTERED BY(a) INTO 4 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC", driver);
    executeStatementOnDriver("CREATE EXTERNAL TABLE " + tblNameStg + "(a INT, b STRING)" +
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n'" +
      " STORED AS TEXTFILE" +
      " LOCATION '" + stagingFolder.newFolder().toURI().getPath() + "'", driver);

    executeStatementOnDriver("load data local inpath '" + BASIC_FILE_NAME +
      "' overwrite into table " + tblNameStg, driver);
    execSelectAndDumpData("select * from " + tblNameStg, driver, "Dumping data for " +
      tblNameStg + " after load:");
    executeStatementOnDriver("FROM " + tblNameStg +
      " INSERT INTO TABLE " + tblName + " PARTITION(bkt=0) " +
      "SELECT a, b where a < 2", driver);
    executeStatementOnDriver("FROM " + tblNameStg +
      " INSERT INTO TABLE " + tblName + " PARTITION(bkt=1) " +
      "SELECT a, b where a >= 2", driver);
    execSelectAndDumpData("select * from " + tblName, driver, "Dumping data for " +
      tblName + " after load:");

    CompactionTxnHandler txnHandler = new CompactionTxnHandler(conf);
    CompactionInfo ci = new CompactionInfo("default", tblName, "bkt=0", CompactionType.MAJOR);
    LOG.debug("List of stats columns before analyze Part1: " + txnHandler.findColumnsWithStats(ci));
    Worker.StatsUpdater su = Worker.StatsUpdater.init(ci, colNames, conf,
      System.getProperty("user.name"));
    su.gatherStats();//compute stats before compaction
    LOG.debug("List of stats columns after analyze Part1: " + txnHandler.findColumnsWithStats(ci));

    CompactionInfo ciPart2 = new CompactionInfo("default", tblName, "bkt=1", CompactionType.MAJOR);
    LOG.debug("List of stats columns before analyze Part2: " + txnHandler.findColumnsWithStats(ci));
    su = Worker.StatsUpdater.init(ciPart2, colNames, conf, System.getProperty("user.name"));
    su.gatherStats();//compute stats before compaction
    LOG.debug("List of stats columns after analyze Part2: " + txnHandler.findColumnsWithStats(ci));
    
    //now make sure we get the stats we expect for partition we are going to add data to later
    Map<String, List<ColumnStatisticsObj>> stats = msClient.getPartitionColumnStatistics(ci.dbname,
      ci.tableName, Arrays.asList(ci.partName), colNames);
    List<ColumnStatisticsObj> colStats = stats.get(ci.partName);
    Assert.assertNotNull("No stats found for partition " + ci.partName, colStats);
    Assert.assertEquals("Expected column 'a' at index 0", "a", colStats.get(0).getColName());
    Assert.assertEquals("Expected column 'b' at index 1", "b", colStats.get(1).getColName());
    LongColumnStatsData colAStats = colStats.get(0).getStatsData().getLongStats();
    Assert.assertEquals("lowValue a", 1, colAStats.getLowValue());
    Assert.assertEquals("highValue a", 1, colAStats.getHighValue());
    Assert.assertEquals("numNulls a", 0, colAStats.getNumNulls());
    Assert.assertEquals("numNdv a", 1, colAStats.getNumDVs());
    StringColumnStatsData colBStats = colStats.get(1).getStatsData().getStringStats();
    Assert.assertEquals("maxColLen b", 3, colBStats.getMaxColLen());
    Assert.assertEquals("avgColLen b", 3.0, colBStats.getAvgColLen());
    Assert.assertEquals("numNulls b", 0, colBStats.getNumNulls());
    Assert.assertEquals("nunDVs", 2, colBStats.getNumDVs());

    //now save stats for partition we won't modify
    stats = msClient.getPartitionColumnStatistics(ciPart2.dbname,
      ciPart2.tableName, Arrays.asList(ciPart2.partName), colNames);
    colStats = stats.get(ciPart2.partName);
    LongColumnStatsData colAStatsPart2 = colStats.get(0).getStatsData().getLongStats();
    StringColumnStatsData colBStatsPart2 = colStats.get(1).getStatsData().getStringStats();


    HiveEndPoint endPt = new HiveEndPoint(null, ci.dbname, ci.tableName, Arrays.asList("0"));
    DelimitedInputWriter writer = new DelimitedInputWriter(new String[] {"a","b"},",", endPt);
    /*next call will eventually end up in HiveEndPoint.createPartitionIfNotExists() which
    makes an operation on Driver
    * and starts it's own CliSessionState and then closes it, which removes it from ThreadLoacal;
    * thus the session
    * created in this class is gone after this; I fixed it in HiveEndPoint*/
    StreamingConnection connection = endPt.newConnection(true);

    TransactionBatch txnBatch =  connection.fetchTransactionBatch(2, writer);
    txnBatch.beginNextTransaction();
    Assert.assertEquals(TransactionBatch.TxnState.OPEN, txnBatch.getCurrentTransactionState());
    txnBatch.write("50,Kiev".getBytes());
    txnBatch.write("51,St. Petersburg".getBytes());
    txnBatch.write("44,Boston".getBytes());
    txnBatch.commit();

    txnBatch.beginNextTransaction();
    txnBatch.write("52,Tel Aviv".getBytes());
    txnBatch.write("53,Atlantis".getBytes());
    txnBatch.write("53,Boston".getBytes());
    txnBatch.commit();

    txnBatch.close();
    connection.close();
    execSelectAndDumpData("select * from " + ci.getFullTableName(), driver, ci.getFullTableName());

    //so now we have written some new data to bkt=0 and it shows up
    CompactionRequest rqst = new CompactionRequest(ci.dbname, ci.tableName, CompactionType.MAJOR);
    rqst.setPartitionname(ci.partName);
    txnHandler.compact(rqst);
    Worker t = new Worker();
    t.setThreadId((int) t.getId());
    t.setHiveConf(conf);
    AtomicBoolean stop = new AtomicBoolean();
    AtomicBoolean looped = new AtomicBoolean();
    stop.set(true);
    t.init(stop, looped);
    t.run();
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("ready for cleaning", compacts.get(0).getState());

    stats = msClient.getPartitionColumnStatistics(ci.dbname, ci.tableName,
      Arrays.asList(ci.partName), colNames);
    colStats = stats.get(ci.partName);
    Assert.assertNotNull("No stats found for partition " + ci.partName, colStats);
    Assert.assertEquals("Expected column 'a' at index 0", "a", colStats.get(0).getColName());
    Assert.assertEquals("Expected column 'b' at index 1", "b", colStats.get(1).getColName());
    colAStats = colStats.get(0).getStatsData().getLongStats();
    Assert.assertEquals("lowValue a", 1, colAStats.getLowValue());
    Assert.assertEquals("highValue a", 53, colAStats.getHighValue());
    Assert.assertEquals("numNulls a", 0, colAStats.getNumNulls());
    Assert.assertEquals("numNdv a", 6, colAStats.getNumDVs());
    colBStats = colStats.get(1).getStatsData().getStringStats();
    Assert.assertEquals("maxColLen b", 14, colBStats.getMaxColLen());
    //cast it to long to get rid of periodic decimal
    Assert.assertEquals("avgColLen b", (long)6.1111111111, (long)colBStats.getAvgColLen());
    Assert.assertEquals("numNulls b", 0, colBStats.getNumNulls());
    Assert.assertEquals("nunDVs", 10, colBStats.getNumDVs());

    //now check that stats for partition we didn't modify did not change
    stats = msClient.getPartitionColumnStatistics(ciPart2.dbname, ciPart2.tableName,
      Arrays.asList(ciPart2.partName), colNames);
    colStats = stats.get(ciPart2.partName);
    Assert.assertEquals("Expected stats for " + ciPart2.partName + " to stay the same",
      colAStatsPart2, colStats.get(0).getStatsData().getLongStats());
    Assert.assertEquals("Expected stats for " + ciPart2.partName + " to stay the same",
      colBStatsPart2, colStats.get(1).getStatsData().getStringStats());
  }

  /**
   * convenience method to execute a select stmt and dump results to log file
   */
  private static void execSelectAndDumpData(String selectStmt, Driver driver, String msg)
    throws  Exception {
    executeStatementOnDriver(selectStmt, driver);
    ArrayList<String> valuesReadFromHiveDriver = new ArrayList<String>();
    driver.getResults(valuesReadFromHiveDriver);
    int rowIdx = 0;
    LOG.debug(msg);
    for(String row : valuesReadFromHiveDriver) {
      LOG.debug(" rowIdx=" + rowIdx++ + ":" + row);
    }
  }
  /**
   * Execute Hive CLI statement
   * @param cmd arbitrary statement to execute
   */
  static void executeStatementOnDriver(String cmd, Driver driver) throws IOException, CommandNeedRetryException {
    LOG.debug("Executing: " + cmd);
    CommandProcessorResponse cpr = driver.run(cmd);
    if(cpr.getResponseCode() != 0) {
      throw new IOException("Failed to execute \"" + cmd + "\". Driver returned: " + cpr);
    }
  }
  static void createTestDataFile(String filename, String[] lines) throws IOException {
    FileWriter writer = null;
    try {
      File file = new File(filename);
      file.deleteOnExit();
      writer = new FileWriter(file);
      for (String line : lines) {
        writer.write(line + "\n");
      }
    } finally {
      if (writer != null) {
        writer.close();
      }
    }

  }
}
