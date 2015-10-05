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

package org.apache.hive.hcatalog.streaming;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.txn.AcidHouseKeeperService;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableLongObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class TestStreaming {
  private static final Logger LOG = LoggerFactory.getLogger(TestStreaming.class);

  public static class RawFileSystem extends RawLocalFileSystem {
    private static final URI NAME;
    static {
      try {
        NAME = new URI("raw:///");
      } catch (URISyntaxException se) {
        throw new IllegalArgumentException("bad uri", se);
      }
    }

    @Override
    public URI getUri() {
      return NAME;
    }


    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
      File file = pathToFile(path);
      if (!file.exists()) {
        throw new FileNotFoundException("Can't find " + path);
      }
      // get close enough
      short mod = 0;
      if (file.canRead()) {
        mod |= 0444;
      }
      if (file.canWrite()) {
        mod |= 0200;
      }
      if (file.canExecute()) {
        mod |= 0111;
      }
      return new FileStatus(file.length(), file.isDirectory(), 1, 1024,
          file.lastModified(), file.lastModified(),
          FsPermission.createImmutable(mod), "owen", "users", path);
    }
  }

  private static final String COL1 = "id";
  private static final String COL2 = "msg";

  private final HiveConf conf;
  private Driver driver;
  private final IMetaStoreClient msClient;

  final String metaStoreURI = null;

  // partitioned table
  private final static String dbName = "testing";
  private final static String tblName = "alerts";
  private final static String[] fieldNames = new String[]{COL1,COL2};
  List<String> partitionVals;
  private static Path partLoc;
  private static Path partLoc2;

  // unpartitioned table
  private final static String dbName2 = "testing2";
  private final static String tblName2 = "alerts";
  private final static String[] fieldNames2 = new String[]{COL1,COL2};


  // for bucket join testing
  private final static String dbName3 = "testing3";
  private final static String tblName3 = "dimensionTable";
  private final static String dbName4 = "testing4";
  private final static String tblName4 = "factTable";
  List<String> partitionVals2;


  private final String PART1_CONTINENT = "Asia";
  private final String PART1_COUNTRY = "India";

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();


  public TestStreaming() throws Exception {
    partitionVals = new ArrayList<String>(2);
    partitionVals.add(PART1_CONTINENT);
    partitionVals.add(PART1_COUNTRY);

    partitionVals2 = new ArrayList<String>(1);
    partitionVals2.add(PART1_COUNTRY);


    conf = new HiveConf(this.getClass());
    conf.set("fs.raw.impl", RawFileSystem.class.getName());
    conf.set("hive.enforce.bucketing", "true");
    TxnDbUtil.setConfValues(conf);
    if (metaStoreURI!=null) {
      conf.setVar(HiveConf.ConfVars.METASTOREURIS, metaStoreURI);
    }
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI, true);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
    dbFolder.create();


    //1) Start from a clean slate (metastore)
    TxnDbUtil.cleanDb();
    TxnDbUtil.prepDb();

    //2) obtain metastore clients
    msClient = new HiveMetaStoreClient(conf);
  }

  @Before
  public void setup() throws Exception {
    SessionState.start(new CliSessionState(conf));
    driver = new Driver(conf);
    driver.setMaxRows(200002);//make sure Driver returns all results
    // drop and recreate the necessary databases and tables
    dropDB(msClient, dbName);

    String[] colNames = new String[] {COL1, COL2};
    String[] colTypes = new String[] {serdeConstants.INT_TYPE_NAME, serdeConstants.STRING_TYPE_NAME};
    String[] bucketCols = new String[] {COL1};
    String loc1 = dbFolder.newFolder(dbName + ".db").toString();
    String[] partNames = new String[]{"Continent", "Country"};
    partLoc = createDbAndTable(driver, dbName, tblName, partitionVals, colNames, colTypes, bucketCols, partNames, loc1, 1);

    dropDB(msClient, dbName2);
    String loc2 = dbFolder.newFolder(dbName2 + ".db").toString();
    partLoc2 = createDbAndTable(driver, dbName2, tblName2, partitionVals, colNames, colTypes, bucketCols, partNames, loc2, 2);

    String loc3 = dbFolder.newFolder("testing5.db").toString();
    createStoreSales("testing5", loc3);

    runDDL(driver, "drop table testBucketing3.streamedtable");
    runDDL(driver, "drop table testBucketing3.finaltable");
    runDDL(driver, "drop table testBucketing3.nobucket");
  }

  @After
  public void cleanup() throws Exception {
    msClient.close();
    driver.close();
  }

  private static List<FieldSchema> getPartitionKeys() {
    List<FieldSchema> fields = new ArrayList<FieldSchema>();
    // Defining partition names in unsorted order
    fields.add(new FieldSchema("continent", serdeConstants.STRING_TYPE_NAME, ""));
    fields.add(new FieldSchema("country", serdeConstants.STRING_TYPE_NAME, ""));
    return fields;
  }

  private void createStoreSales(String dbName, String loc) throws Exception {
    String dbUri = "raw://" + new Path(loc).toUri().toString();
    String tableLoc = dbUri + Path.SEPARATOR + "store_sales";

    boolean success = runDDL(driver, "create database IF NOT EXISTS " + dbName + " location '" + dbUri + "'");
    Assert.assertTrue(success);
    success = runDDL(driver, "use " + dbName);
    Assert.assertTrue(success);

    success = runDDL(driver, "drop table if exists store_sales");
    Assert.assertTrue(success);
    success = runDDL(driver, "create table store_sales\n" +
      "(\n" +
      "    ss_sold_date_sk           int,\n" +
      "    ss_sold_time_sk           int,\n" +
      "    ss_item_sk                int,\n" +
      "    ss_customer_sk            int,\n" +
      "    ss_cdemo_sk               int,\n" +
      "    ss_hdemo_sk               int,\n" +
      "    ss_addr_sk                int,\n" +
      "    ss_store_sk               int,\n" +
      "    ss_promo_sk               int,\n" +
      "    ss_ticket_number          int,\n" +
      "    ss_quantity               int,\n" +
      "    ss_wholesale_cost         decimal(7,2),\n" +
      "    ss_list_price             decimal(7,2),\n" +
      "    ss_sales_price            decimal(7,2),\n" +
      "    ss_ext_discount_amt       decimal(7,2),\n" +
      "    ss_ext_sales_price        decimal(7,2),\n" +
      "    ss_ext_wholesale_cost     decimal(7,2),\n" +
      "    ss_ext_list_price         decimal(7,2),\n" +
      "    ss_ext_tax                decimal(7,2),\n" +
      "    ss_coupon_amt             decimal(7,2),\n" +
      "    ss_net_paid               decimal(7,2),\n" +
      "    ss_net_paid_inc_tax       decimal(7,2),\n" +
      "    ss_net_profit             decimal(7,2)\n" +
      ")\n" +
      " partitioned by (dt string)\n" +
      "clustered by (ss_store_sk, ss_promo_sk)\n" +
      "INTO 4 BUCKETS stored as orc " + " location '" + tableLoc +  "'" + "  TBLPROPERTIES ('orc.compress'='NONE', 'transactional'='true')");
    Assert.assertTrue(success);

    success = runDDL(driver, "alter table store_sales add partition(dt='2015')");
    Assert.assertTrue(success);
  }
  /**
   * make sure it works with table where bucket col is not 1st col
   * @throws Exception
   */
  @Test
  public void testBucketingWhereBucketColIsNotFirstCol() throws Exception {
    List<String> partitionVals = new ArrayList<String>();
    partitionVals.add("2015");
    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, "testing5", "store_sales", partitionVals);
    DelimitedInputWriter writer = new DelimitedInputWriter(new String[] {"ss_sold_date_sk","ss_sold_time_sk", "ss_item_sk",
      "ss_customer_sk", "ss_cdemo_sk", "ss_hdemo_sk", "ss_addr_sk", "ss_store_sk", "ss_promo_sk", "ss_ticket_number", "ss_quantity",
      "ss_wholesale_cost", "ss_list_price", "ss_sales_price", "ss_ext_discount_amt", "ss_ext_sales_price", "ss_ext_wholesale_cost",
      "ss_ext_list_price", "ss_ext_tax", "ss_coupon_amt", "ss_net_paid", "ss_net_paid_inc_tax", "ss_net_profit"},",", endPt);
    StreamingConnection connection = endPt.newConnection(false, null);//should this really be null?

    TransactionBatch txnBatch =  connection.fetchTransactionBatch(2, writer);
    txnBatch.beginNextTransaction();

    StringBuilder row = new StringBuilder();
    for(int i = 0; i < 10; i++) {
      for(int ints = 0; ints < 11; ints++) {
        row.append(ints).append(',');
      }
      for(int decs = 0; decs < 12; decs++) {
        row.append(i + 0.1).append(',');
      }
      row.setLength(row.length() - 1);
      txnBatch.write(row.toString().getBytes());
    }
    txnBatch.commit();
    txnBatch.close();
    connection.close();

    ArrayList<String> res = queryTable(driver, "select row__id.bucketid, * from testing5.store_sales");
    for (String re : res) {
      System.out.println(re);
    }
  }


  // stream data into streaming table with N buckets, then copy the data into another bucketed table
  // check if bucketing in both was done in the same way
  @Test
  public void testStreamBucketingMatchesRegularBucketing() throws Exception {
    int bucketCount = 100;

    String dbUri = "raw://" + new Path(dbFolder.newFolder().toString()).toUri().toString();
    String tableLoc  = "'" + dbUri + Path.SEPARATOR + "streamedtable" + "'";
    String tableLoc2 = "'" + dbUri + Path.SEPARATOR + "finaltable" + "'";
    String tableLoc3 = "'" + dbUri + Path.SEPARATOR + "nobucket" + "'";

    runDDL(driver, "create database testBucketing3");
    runDDL(driver, "use testBucketing3");
    runDDL(driver, "create table streamedtable ( key1 string,key2 int,data string ) clustered by ( key1,key2 ) into "
            + bucketCount + " buckets  stored as orc  location " + tableLoc + " TBLPROPERTIES ('transactional'='true')") ;
//  In 'nobucket' table we capture bucketid from streamedtable to workaround a hive bug that prevents joins two identically bucketed tables
    runDDL(driver, "create table nobucket ( bucketid int, key1 string,key2 int,data string ) location " + tableLoc3) ;
    runDDL(driver, "create table finaltable ( bucketid int, key1 string,key2 int,data string ) clustered by ( key1,key2 ) into "
            + bucketCount + " buckets  stored as orc location " + tableLoc2 + " TBLPROPERTIES ('transactional'='true')");


    String[] records = new String[] {
    "PSFAHYLZVC,29,EPNMA",
    "PPPRKWAYAU,96,VUTEE",
    "MIAOFERCHI,3,WBDSI",
    "CEGQAZOWVN,0,WCUZL",
    "XWAKMNSVQF,28,YJVHU",
    "XBWTSAJWME,2,KDQFO",
    "FUVLQTAXAY,5,LDSDG",
    "QTQMDJMGJH,6,QBOMA",
    "EFLOTLWJWN,71,GHWPS",
    "PEQNAOJHCM,82,CAAFI",
    "MOEKQLGZCP,41,RUACR",
    "QZXMCOPTID,37,LFLWE",
    "EYALVWICRD,13,JEZLC",
    "VYWLZAYTXX,16,DMVZX",
    "OSALYSQIXR,47,HNZVE",
    "JGKVHKCEGQ,25,KSCJB",
    "WQFMMYDHET,12,DTRWA",
    "AJOVAYZKZQ,15,YBKFO",
    "YAQONWCUAU,31,QJNHZ",
    "DJBXUEUOEB,35,IYCBL"
    };

    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, "testBucketing3", "streamedtable", null);
    String[] colNames1 = new String[] { "key1", "key2", "data" };
    DelimitedInputWriter wr = new DelimitedInputWriter(colNames1,",",  endPt);
    StreamingConnection connection = endPt.newConnection(false);

    TransactionBatch txnBatch =  connection.fetchTransactionBatch(2, wr);
    txnBatch.beginNextTransaction();

    for (String record : records) {
      txnBatch.write(record.toString().getBytes());
    }

    txnBatch.commit();
    txnBatch.close();
    connection.close();

    ArrayList<String> res1 = queryTable(driver, "select row__id.bucketid, * from streamedtable order by key2");
    for (String re : res1) {
      System.out.println(re);
    }

    driver.run("insert into nobucket select row__id.bucketid,* from streamedtable");
    runDDL(driver, " insert into finaltable select * from nobucket");
    ArrayList<String> res2 = queryTable(driver, "select row__id.bucketid,* from finaltable where row__id.bucketid<>bucketid");
    for (String s : res2) {
      LOG.error(s);
    }
    Assert.assertTrue(res2.isEmpty());
  }


  private void checkDataWritten(Path partitionPath, long minTxn, long maxTxn, int buckets, int numExpectedFiles,
                                String... records) throws Exception {
    ValidTxnList txns = msClient.getValidTxns();
    AcidUtils.Directory dir = AcidUtils.getAcidState(partitionPath, conf, txns);
    Assert.assertEquals(0, dir.getObsolete().size());
    Assert.assertEquals(0, dir.getOriginalFiles().size());
    List<AcidUtils.ParsedDelta> current = dir.getCurrentDirectories();
    System.out.println("Files found: ");
    for (AcidUtils.ParsedDelta pd : current) System.out.println(pd.getPath().toString());
    Assert.assertEquals(numExpectedFiles, current.size());

    // find the absolute minimum transaction
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (AcidUtils.ParsedDelta pd : current) {
      if (pd.getMaxTransaction() > max) max = pd.getMaxTransaction();
      if (pd.getMinTransaction() < min) min = pd.getMinTransaction();
    }
    Assert.assertEquals(minTxn, min);
    Assert.assertEquals(maxTxn, max);

    InputFormat inf = new OrcInputFormat();
    JobConf job = new JobConf();
    job.set("mapred.input.dir", partitionPath.toString());
    job.set("bucket_count", Integer.toString(buckets));
    job.set(ValidTxnList.VALID_TXNS_KEY, txns.toString());
    InputSplit[] splits = inf.getSplits(job, buckets);
    Assert.assertEquals(buckets, splits.length);
    org.apache.hadoop.mapred.RecordReader<NullWritable, OrcStruct> rr =
            inf.getRecordReader(splits[0], job, Reporter.NULL);

    NullWritable key = rr.createKey();
    OrcStruct value = rr.createValue();
    for (int i = 0; i < records.length; i++) {
      Assert.assertEquals(true, rr.next(key, value));
      Assert.assertEquals(records[i], value.toString());
    }
    Assert.assertEquals(false, rr.next(key, value));
  }

  private void checkNothingWritten(Path partitionPath) throws Exception {
    ValidTxnList txns = msClient.getValidTxns();
    AcidUtils.Directory dir = AcidUtils.getAcidState(partitionPath, conf, txns);
    Assert.assertEquals(0, dir.getObsolete().size());
    Assert.assertEquals(0, dir.getOriginalFiles().size());
    List<AcidUtils.ParsedDelta> current = dir.getCurrentDirectories();
    Assert.assertEquals(0, current.size());
  }

  @Test
  public void testEndpointConnection() throws Exception {
    // 1) Basic
    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName, tblName
            , partitionVals);
    StreamingConnection connection = endPt.newConnection(false, null); //shouldn't throw
    connection.close();

    // 2) Leave partition unspecified
    endPt = new HiveEndPoint(metaStoreURI, dbName, tblName, null);
    endPt.newConnection(false, null).close(); // should not throw
  }

  @Test
  public void testAddPartition() throws Exception {
    List<String> newPartVals = new ArrayList<String>(2);
    newPartVals.add(PART1_CONTINENT);
    newPartVals.add("Nepal");

    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName, tblName
            , newPartVals);

    // Ensure partition is absent
    try {
      msClient.getPartition(endPt.database, endPt.table, endPt.partitionVals);
      Assert.assertTrue("Partition already exists", false);
    } catch (NoSuchObjectException e) {
      // expect this exception
    }

    // Create partition
    Assert.assertNotNull(endPt.newConnection(true, null));

    // Ensure partition is present
    Partition p = msClient.getPartition(endPt.database, endPt.table, endPt.partitionVals);
    Assert.assertNotNull("Did not find added partition", p);
  }

  @Test
  public void testTransactionBatchEmptyCommit() throws Exception {
    // 1)  to partitioned table
    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName, tblName,
            partitionVals);
    DelimitedInputWriter writer = new DelimitedInputWriter(fieldNames,",", endPt);
    StreamingConnection connection = endPt.newConnection(false, null);

    TransactionBatch txnBatch =  connection.fetchTransactionBatch(10, writer);

    txnBatch.beginNextTransaction();
    txnBatch.commit();
    Assert.assertEquals(TransactionBatch.TxnState.COMMITTED
            , txnBatch.getCurrentTransactionState());
    txnBatch.close();
    connection.close();

    // 2) To unpartitioned table
    endPt = new HiveEndPoint(metaStoreURI, dbName2, tblName2, null);
    writer = new DelimitedInputWriter(fieldNames2,",", endPt);
    connection = endPt.newConnection(false, null);

    txnBatch =  connection.fetchTransactionBatch(10, writer);
    txnBatch.beginNextTransaction();
    txnBatch.commit();
    Assert.assertEquals(TransactionBatch.TxnState.COMMITTED
            , txnBatch.getCurrentTransactionState());
    txnBatch.close();
    connection.close();
  }

  /**
   * check that transactions that have not heartbeated and timedout get properly aborted
   * @throws Exception
   */
  @Test
  public void testTimeOutReaper() throws Exception {
    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName2, tblName2, null);
    DelimitedInputWriter writer = new DelimitedInputWriter(fieldNames2,",", endPt);
    StreamingConnection connection = endPt.newConnection(false, null);

    TransactionBatch txnBatch =  connection.fetchTransactionBatch(5, writer);
    txnBatch.beginNextTransaction();
    conf.setTimeVar(HiveConf.ConfVars.HIVE_TIMEDOUT_TXN_REAPER_START, 0, TimeUnit.SECONDS);
    //ensure txn timesout
    conf.setTimeVar(HiveConf.ConfVars.HIVE_TXN_TIMEOUT, 1, TimeUnit.MILLISECONDS);
    AcidHouseKeeperService houseKeeperService = new AcidHouseKeeperService();
    houseKeeperService.start(conf);
    while(houseKeeperService.getIsAliveCounter() <= Integer.MIN_VALUE) {
      Thread.sleep(100);//make sure it has run at least once
    }
    houseKeeperService.stop();
    try {
      //should fail because the TransactionBatch timed out
      txnBatch.commit();
    }
    catch(TransactionError e) {
      Assert.assertTrue("Expected aborted transaction", e.getCause() instanceof TxnAbortedException);
    }
    txnBatch.close();
    txnBatch =  connection.fetchTransactionBatch(10, writer);
    txnBatch.beginNextTransaction();
    txnBatch.commit();
    txnBatch.beginNextTransaction();
    int lastCount = houseKeeperService.getIsAliveCounter();
    houseKeeperService.start(conf);
    while(houseKeeperService.getIsAliveCounter() <= lastCount) {
      Thread.sleep(100);//make sure it has run at least once
    }
    houseKeeperService.stop();
    try {
      //should fail because the TransactionBatch timed out
      txnBatch.commit();
    }
    catch(TransactionError e) {
      Assert.assertTrue("Expected aborted transaction", e.getCause() instanceof TxnAbortedException);
    }
    txnBatch.close();
    connection.close();
  }
  @Test
  public void testTransactionBatchEmptyAbort() throws Exception {
    // 1) to partitioned table
    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName, tblName,
            partitionVals);
    DelimitedInputWriter writer = new DelimitedInputWriter(fieldNames,",", endPt);
    StreamingConnection connection = endPt.newConnection(true);

    TransactionBatch txnBatch =  connection.fetchTransactionBatch(10, writer);
    txnBatch.beginNextTransaction();
    txnBatch.abort();
    Assert.assertEquals(TransactionBatch.TxnState.ABORTED
            , txnBatch.getCurrentTransactionState());
    txnBatch.close();
    connection.close();

    // 2) to unpartitioned table
    endPt = new HiveEndPoint(metaStoreURI, dbName2, tblName2, null);
    writer = new DelimitedInputWriter(fieldNames,",", endPt);
    connection = endPt.newConnection(true);

    txnBatch =  connection.fetchTransactionBatch(10, writer);
    txnBatch.beginNextTransaction();
    txnBatch.abort();
    Assert.assertEquals(TransactionBatch.TxnState.ABORTED
            , txnBatch.getCurrentTransactionState());
    txnBatch.close();
    connection.close();
  }

  @Test
  public void testTransactionBatchCommit_Delimited() throws Exception {
    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName, tblName,
            partitionVals);
    DelimitedInputWriter writer = new DelimitedInputWriter(fieldNames,",", endPt);
    StreamingConnection connection = endPt.newConnection(true);

    // 1st Txn
    TransactionBatch txnBatch =  connection.fetchTransactionBatch(10, writer);
    txnBatch.beginNextTransaction();
    Assert.assertEquals(TransactionBatch.TxnState.OPEN
            , txnBatch.getCurrentTransactionState());
    txnBatch.write("1,Hello streaming".getBytes());
    txnBatch.commit();

    checkDataWritten(partLoc, 1, 10, 1, 1, "{1, Hello streaming}");

    Assert.assertEquals(TransactionBatch.TxnState.COMMITTED
            , txnBatch.getCurrentTransactionState());

    // 2nd Txn
    txnBatch.beginNextTransaction();
    Assert.assertEquals(TransactionBatch.TxnState.OPEN
            , txnBatch.getCurrentTransactionState());
    txnBatch.write("2,Welcome to streaming".getBytes());

    // data should not be visible
    checkDataWritten(partLoc, 1, 10, 1, 1, "{1, Hello streaming}");

    txnBatch.commit();

    checkDataWritten(partLoc, 1, 10, 1, 1, "{1, Hello streaming}",
        "{2, Welcome to streaming}");

    txnBatch.close();
    Assert.assertEquals(TransactionBatch.TxnState.INACTIVE
            , txnBatch.getCurrentTransactionState());


    connection.close();


    // To Unpartitioned table
    endPt = new HiveEndPoint(metaStoreURI, dbName2, tblName2, null);
    writer = new DelimitedInputWriter(fieldNames,",", endPt);
    connection = endPt.newConnection(true);

    // 1st Txn
    txnBatch =  connection.fetchTransactionBatch(10, writer);
    txnBatch.beginNextTransaction();
    Assert.assertEquals(TransactionBatch.TxnState.OPEN
            , txnBatch.getCurrentTransactionState());
    txnBatch.write("1,Hello streaming".getBytes());
    txnBatch.commit();

    Assert.assertEquals(TransactionBatch.TxnState.COMMITTED
            , txnBatch.getCurrentTransactionState());
    connection.close();
  }

  @Test
  public void testTransactionBatchCommit_Json() throws Exception {
    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName, tblName,
            partitionVals);
    StrictJsonWriter writer = new StrictJsonWriter(endPt);
    StreamingConnection connection = endPt.newConnection(true);

    // 1st Txn
    TransactionBatch txnBatch =  connection.fetchTransactionBatch(10, writer);
    txnBatch.beginNextTransaction();
    Assert.assertEquals(TransactionBatch.TxnState.OPEN
            , txnBatch.getCurrentTransactionState());
    String rec1 = "{\"id\" : 1, \"msg\": \"Hello streaming\"}";
    txnBatch.write(rec1.getBytes());
    txnBatch.commit();

    checkDataWritten(partLoc, 1, 10, 1, 1, "{1, Hello streaming}");

    Assert.assertEquals(TransactionBatch.TxnState.COMMITTED
            , txnBatch.getCurrentTransactionState());

    txnBatch.close();
    Assert.assertEquals(TransactionBatch.TxnState.INACTIVE
            , txnBatch.getCurrentTransactionState());

    connection.close();
  }

  @Test
  public void testRemainingTransactions() throws Exception {
    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName, tblName,
            partitionVals);
    DelimitedInputWriter writer = new DelimitedInputWriter(fieldNames,",", endPt);
    StreamingConnection connection = endPt.newConnection(true);

    // 1) test with txn.Commit()
    TransactionBatch txnBatch =  connection.fetchTransactionBatch(10, writer);
    int batch=0;
    int initialCount = txnBatch.remainingTransactions();
    while (txnBatch.remainingTransactions()>0) {
      txnBatch.beginNextTransaction();
      Assert.assertEquals(--initialCount, txnBatch.remainingTransactions());
      for (int rec=0; rec<2; ++rec) {
        Assert.assertEquals(TransactionBatch.TxnState.OPEN
                , txnBatch.getCurrentTransactionState());
        txnBatch.write((batch * rec + ",Hello streaming").getBytes());
      }
      txnBatch.commit();
      Assert.assertEquals(TransactionBatch.TxnState.COMMITTED
              , txnBatch.getCurrentTransactionState());
      ++batch;
    }
    Assert.assertEquals(0, txnBatch.remainingTransactions());
    txnBatch.close();

    Assert.assertEquals(TransactionBatch.TxnState.INACTIVE
            , txnBatch.getCurrentTransactionState());

    // 2) test with txn.Abort()
    txnBatch =  connection.fetchTransactionBatch(10, writer);
    batch=0;
    initialCount = txnBatch.remainingTransactions();
    while (txnBatch.remainingTransactions()>0) {
      txnBatch.beginNextTransaction();
      Assert.assertEquals(--initialCount,txnBatch.remainingTransactions());
      for (int rec=0; rec<2; ++rec) {
        Assert.assertEquals(TransactionBatch.TxnState.OPEN
                , txnBatch.getCurrentTransactionState());
        txnBatch.write((batch * rec + ",Hello streaming").getBytes());
      }
      txnBatch.abort();
      Assert.assertEquals(TransactionBatch.TxnState.ABORTED
              , txnBatch.getCurrentTransactionState());
      ++batch;
    }
    Assert.assertEquals(0, txnBatch.remainingTransactions());
    txnBatch.close();

    Assert.assertEquals(TransactionBatch.TxnState.INACTIVE
            , txnBatch.getCurrentTransactionState());

    connection.close();
  }

  @Test
  public void testTransactionBatchAbort() throws Exception {

    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName, tblName,
            partitionVals);
    DelimitedInputWriter writer = new DelimitedInputWriter(fieldNames,",", endPt);
    StreamingConnection connection = endPt.newConnection(false);


    TransactionBatch txnBatch =  connection.fetchTransactionBatch(10, writer);
    txnBatch.beginNextTransaction();
    txnBatch.write("1,Hello streaming".getBytes());
    txnBatch.write("2,Welcome to streaming".getBytes());
    txnBatch.abort();

    checkNothingWritten(partLoc);

    Assert.assertEquals(TransactionBatch.TxnState.ABORTED
            , txnBatch.getCurrentTransactionState());

    txnBatch.close();
    connection.close();

    checkNothingWritten(partLoc);

  }


  @Test
  public void testTransactionBatchAbortAndCommit() throws Exception {

    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName, tblName,
            partitionVals);
    DelimitedInputWriter writer = new DelimitedInputWriter(fieldNames,",", endPt);
    StreamingConnection connection = endPt.newConnection(false);

    TransactionBatch txnBatch =  connection.fetchTransactionBatch(10, writer);
    txnBatch.beginNextTransaction();
    txnBatch.write("1,Hello streaming".getBytes());
    txnBatch.write("2,Welcome to streaming".getBytes());
    txnBatch.abort();

    checkNothingWritten(partLoc);

    Assert.assertEquals(TransactionBatch.TxnState.ABORTED
            , txnBatch.getCurrentTransactionState());

    txnBatch.beginNextTransaction();
    txnBatch.write("1,Hello streaming".getBytes());
    txnBatch.write("2,Welcome to streaming".getBytes());
    txnBatch.commit();

    checkDataWritten(partLoc, 1, 10, 1, 1, "{1, Hello streaming}",
            "{2, Welcome to streaming}");

    txnBatch.close();
    connection.close();
  }

  @Test
  public void testMultipleTransactionBatchCommits() throws Exception {
    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName, tblName,
            partitionVals);
    DelimitedInputWriter writer = new DelimitedInputWriter(fieldNames,",", endPt);
    StreamingConnection connection = endPt.newConnection(true);

    TransactionBatch txnBatch =  connection.fetchTransactionBatch(10, writer);
    txnBatch.beginNextTransaction();
    txnBatch.write("1,Hello streaming".getBytes());
    txnBatch.commit();

    checkDataWritten(partLoc, 1, 10, 1, 1, "{1, Hello streaming}");

    txnBatch.beginNextTransaction();
    txnBatch.write("2,Welcome to streaming".getBytes());
    txnBatch.commit();

    checkDataWritten(partLoc, 1, 10, 1, 1, "{1, Hello streaming}",
            "{2, Welcome to streaming}");

    txnBatch.close();

    // 2nd Txn Batch
    txnBatch =  connection.fetchTransactionBatch(10, writer);
    txnBatch.beginNextTransaction();
    txnBatch.write("3,Hello streaming - once again".getBytes());
    txnBatch.commit();

    checkDataWritten(partLoc, 1, 20, 1, 2, "{1, Hello streaming}",
            "{2, Welcome to streaming}", "{3, Hello streaming - once again}");

    txnBatch.beginNextTransaction();
    txnBatch.write("4,Welcome to streaming - once again".getBytes());
    txnBatch.commit();

    checkDataWritten(partLoc, 1, 20, 1, 2, "{1, Hello streaming}",
            "{2, Welcome to streaming}", "{3, Hello streaming - once again}",
            "{4, Welcome to streaming - once again}");

    Assert.assertEquals(TransactionBatch.TxnState.COMMITTED
            , txnBatch.getCurrentTransactionState());

    txnBatch.close();

    connection.close();
  }


  @Test
  public void testInterleavedTransactionBatchCommits() throws Exception {
    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName, tblName,
            partitionVals);
    DelimitedInputWriter writer = new DelimitedInputWriter(fieldNames, ",", endPt);
    StreamingConnection connection = endPt.newConnection(false);

    // Acquire 1st Txn Batch
    TransactionBatch txnBatch1 =  connection.fetchTransactionBatch(10, writer);
    txnBatch1.beginNextTransaction();

    // Acquire 2nd Txn Batch
    DelimitedInputWriter writer2 = new DelimitedInputWriter(fieldNames, ",", endPt);
    TransactionBatch txnBatch2 =  connection.fetchTransactionBatch(10, writer2);
    txnBatch2.beginNextTransaction();

    // Interleaved writes to both batches
    txnBatch1.write("1,Hello streaming".getBytes());
    txnBatch2.write("3,Hello streaming - once again".getBytes());

    checkNothingWritten(partLoc);

    txnBatch2.commit();

    checkDataWritten(partLoc, 11, 20, 1, 1, "{3, Hello streaming - once again}");

    txnBatch1.commit();

    checkDataWritten(partLoc, 1, 20, 1, 2, "{1, Hello streaming}", "{3, Hello streaming - once again}");

    txnBatch1.beginNextTransaction();
    txnBatch1.write("2,Welcome to streaming".getBytes());

    txnBatch2.beginNextTransaction();
    txnBatch2.write("4,Welcome to streaming - once again".getBytes());

    checkDataWritten(partLoc, 1, 20, 1, 2, "{1, Hello streaming}", "{3, Hello streaming - once again}");

    txnBatch1.commit();

    checkDataWritten(partLoc, 1, 20, 1, 2, "{1, Hello streaming}",
        "{2, Welcome to streaming}",
        "{3, Hello streaming - once again}");

    txnBatch2.commit();

    checkDataWritten(partLoc, 1, 20, 1, 2, "{1, Hello streaming}",
        "{2, Welcome to streaming}",
        "{3, Hello streaming - once again}",
        "{4, Welcome to streaming - once again}");

    Assert.assertEquals(TransactionBatch.TxnState.COMMITTED
            , txnBatch1.getCurrentTransactionState());
    Assert.assertEquals(TransactionBatch.TxnState.COMMITTED
            , txnBatch2.getCurrentTransactionState());

    txnBatch1.close();
    txnBatch2.close();

    connection.close();
  }

  private static class WriterThd extends Thread {

    private final StreamingConnection conn;
    private final DelimitedInputWriter writer;
    private final String data;
    private Throwable error;

    WriterThd(HiveEndPoint ep, String data) throws Exception {
      super("Writer_" + data);
      writer = new DelimitedInputWriter(fieldNames, ",", ep);
      conn = ep.newConnection(false);
      this.data = data;
      setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread thread, Throwable throwable) {
          error = throwable;
          LOG.error("Thread " + thread.getName() + " died: " + throwable.getMessage(), throwable);
        }
      });
    }

    @Override
    public void run() {
      TransactionBatch txnBatch = null;
      try {
        txnBatch =  conn.fetchTransactionBatch(10, writer);
        while (txnBatch.remainingTransactions() > 0) {
          txnBatch.beginNextTransaction();
          txnBatch.write(data.getBytes());
          txnBatch.write(data.getBytes());
          txnBatch.commit();
        } // while
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        if (txnBatch != null) {
          try {
            txnBatch.close();
          } catch (Exception e) {
            LOG.error("txnBatch.close() failed: " + e.getMessage(), e);
            conn.close();
          }
        }
        try {
          conn.close();
        } catch (Exception e) {
          LOG.error("conn.close() failed: " + e.getMessage(), e);
        }

      }
    }
  }

  @Test
  public void testConcurrentTransactionBatchCommits() throws Exception {
    final HiveEndPoint ep = new HiveEndPoint(metaStoreURI, dbName, tblName, partitionVals);
    List<WriterThd> writers = new ArrayList<WriterThd>(3);
    writers.add(new WriterThd(ep, "1,Matrix"));
    writers.add(new WriterThd(ep, "2,Gandhi"));
    writers.add(new WriterThd(ep, "3,Silence"));

    for(WriterThd w : writers) {
      w.start();
    }
    for(WriterThd w : writers) {
      w.join();
    }
    for(WriterThd w : writers) {
      if(w.error != null) {
        Assert.assertFalse("Writer thread" + w.getName() + " died: " + w.error.getMessage() +
          " See log file for stack trace", true);
      }
    }
  }


  private ArrayList<SampleRec> dumpBucket(Path orcFile) throws IOException {
    org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(new Configuration());
    Reader reader = OrcFile.createReader(orcFile,
            OrcFile.readerOptions(conf).filesystem(fs));

    RecordReader rows = reader.rows(null);
    StructObjectInspector inspector = (StructObjectInspector) reader
            .getObjectInspector();

    System.out.format("Found Bucket File : %s \n", orcFile.getName());
    ArrayList<SampleRec> result = new ArrayList<SampleRec>();
    while (rows.hasNext()) {
      Object row = rows.next(null);
      SampleRec rec = (SampleRec) deserializeDeltaFileRow(row, inspector)[5];
      result.add(rec);
    }

    return result;
  }

  // Assumes stored data schema = [acid fields],string,int,string
  // return array of 6 fields, where the last field has the actual data
  private static Object[] deserializeDeltaFileRow(Object row, StructObjectInspector inspector) {
    List<? extends StructField> fields = inspector.getAllStructFieldRefs();

    WritableIntObjectInspector f0ins = (WritableIntObjectInspector) fields.get(0).getFieldObjectInspector();
    WritableLongObjectInspector f1ins = (WritableLongObjectInspector) fields.get(1).getFieldObjectInspector();
    WritableIntObjectInspector f2ins = (WritableIntObjectInspector) fields.get(2).getFieldObjectInspector();
    WritableLongObjectInspector f3ins = (WritableLongObjectInspector) fields.get(3).getFieldObjectInspector();
    WritableLongObjectInspector f4ins = (WritableLongObjectInspector)  fields.get(4).getFieldObjectInspector();
    StructObjectInspector f5ins = (StructObjectInspector) fields.get(5).getFieldObjectInspector();

    int f0 = f0ins.get(inspector.getStructFieldData(row, fields.get(0)));
    long f1 = f1ins.get(inspector.getStructFieldData(row, fields.get(1)));
    int f2 = f2ins.get(inspector.getStructFieldData(row, fields.get(2)));
    long f3 = f3ins.get(inspector.getStructFieldData(row, fields.get(3)));
    long f4 = f4ins.get(inspector.getStructFieldData(row, fields.get(4)));
    SampleRec f5 = deserializeInner(inspector.getStructFieldData(row, fields.get(5)), f5ins);

    return new Object[] {f0, f1, f2, f3, f4, f5};
  }

  // Assumes row schema => string,int,string
  private static SampleRec deserializeInner(Object row, StructObjectInspector inspector) {
    List<? extends StructField> fields = inspector.getAllStructFieldRefs();

    WritableStringObjectInspector f0ins = (WritableStringObjectInspector) fields.get(0).getFieldObjectInspector();
    WritableIntObjectInspector f1ins = (WritableIntObjectInspector) fields.get(1).getFieldObjectInspector();
    WritableStringObjectInspector f2ins = (WritableStringObjectInspector) fields.get(2).getFieldObjectInspector();

    String f0 = f0ins.getPrimitiveJavaObject(inspector.getStructFieldData(row, fields.get(0)));
    int f1 = f1ins.get(inspector.getStructFieldData(row, fields.get(1)));
    String f2 = f2ins.getPrimitiveJavaObject(inspector.getStructFieldData(row, fields.get(2)));
    return new SampleRec(f0, f1, f2);
  }

  @Test
  public void testBucketing() throws Exception {
    dropDB(msClient, dbName3);
    dropDB(msClient, dbName4);

    // 1) Create two bucketed tables
    String dbLocation = dbFolder.newFolder(dbName3).getCanonicalPath() + ".db";
    dbLocation = dbLocation.replaceAll("\\\\","/"); // for windows paths
    String[] colNames = "key1,key2,data".split(",");
    String[] colTypes = "string,int,string".split(",");
    String[] bucketNames = "key1,key2".split(",");
    int bucketCount = 4;
    createDbAndTable(driver, dbName3, tblName3, null, colNames, colTypes, bucketNames
            , null, dbLocation, bucketCount);

    String dbLocation2 = dbFolder.newFolder(dbName4).getCanonicalPath() + ".db";
    dbLocation2 = dbLocation2.replaceAll("\\\\","/"); // for windows paths
    String[] colNames2 = "key3,key4,data2".split(",");
    String[] colTypes2 = "string,int,string".split(",");
    String[] bucketNames2 = "key3,key4".split(",");
    createDbAndTable(driver, dbName4, tblName4, null, colNames2, colTypes2, bucketNames2
            , null, dbLocation2, bucketCount);


    // 2) Insert data into both tables
    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName3, tblName3, null);
    DelimitedInputWriter writer = new DelimitedInputWriter(colNames,",", endPt);
    StreamingConnection connection = endPt.newConnection(false);

    TransactionBatch txnBatch =  connection.fetchTransactionBatch(2, writer);
    txnBatch.beginNextTransaction();
    txnBatch.write("name0,1,Hello streaming".getBytes());
    txnBatch.write("name2,2,Welcome to streaming".getBytes());
    txnBatch.write("name4,2,more Streaming unlimited".getBytes());
    txnBatch.write("name5,2,even more Streaming unlimited".getBytes());
    txnBatch.commit();


    HiveEndPoint endPt2 = new HiveEndPoint(metaStoreURI, dbName4, tblName4, null);
    DelimitedInputWriter writer2 = new DelimitedInputWriter(colNames2,",", endPt2);
    StreamingConnection connection2 = endPt2.newConnection(false);
    TransactionBatch txnBatch2 =  connection2.fetchTransactionBatch(2, writer2);
    txnBatch2.beginNextTransaction();

    txnBatch2.write("name5,2,fact3".getBytes());  // bucket 0
    txnBatch2.write("name8,2,fact3".getBytes());  // bucket 1
    txnBatch2.write("name0,1,fact1".getBytes());  // bucket 2
    // no data for bucket 3 -- expect 0 length bucket file


    txnBatch2.commit();

    // 3 Check data distribution in  buckets

    HashMap<Integer, ArrayList<SampleRec>> actual1 = dumpAllBuckets(dbLocation, tblName3);
    HashMap<Integer, ArrayList<SampleRec>> actual2 = dumpAllBuckets(dbLocation2, tblName4);
    System.err.println("\n  Table 1");
    System.err.println(actual1);
    System.err.println("\n  Table 2");
    System.err.println(actual2);

    // assert bucket listing is as expected
    Assert.assertEquals("number of buckets does not match expectation", actual1.values().size(), 4);
    Assert.assertEquals("records in bucket does not match expectation", actual1.get(0).size(), 2);
    Assert.assertEquals("records in bucket does not match expectation", actual1.get(1).size(), 1);
    Assert.assertEquals("records in bucket does not match expectation", actual1.get(2).size(), 0);
    Assert.assertEquals("records in bucket does not match expectation", actual1.get(3).size(), 1);


  }


    // assumes un partitioned table
  // returns a map<bucketNum, list<record> >
  private HashMap<Integer, ArrayList<SampleRec>> dumpAllBuckets(String dbLocation, String tableName)
          throws IOException {
    HashMap<Integer, ArrayList<SampleRec>> result = new HashMap<Integer, ArrayList<SampleRec>>();

    for (File deltaDir : new File(dbLocation + "/" + tableName).listFiles()) {
      if(!deltaDir.getName().startsWith("delta"))
        continue;
      File[] bucketFiles = deltaDir.listFiles();
      for (File bucketFile : bucketFiles) {
        if(bucketFile.toString().endsWith("length"))
          continue;
        Integer bucketNum = getBucketNumber(bucketFile);
        ArrayList<SampleRec>  recs = dumpBucket(new Path(bucketFile.toString()));
        result.put(bucketNum, recs);
      }
    }
    return result;
  }

  //assumes bucket_NNNNN format of file name
  private Integer getBucketNumber(File bucketFile) {
    String fname = bucketFile.getName();
    int start = fname.indexOf('_');
    String number = fname.substring(start+1, fname.length());
    return Integer.parseInt(number);
  }

  // delete db and all tables in it
  public static void dropDB(IMetaStoreClient client, String databaseName) {
    try {
      for (String table : client.listTableNamesByFilter(databaseName, "", (short)-1)) {
        client.dropTable(databaseName, table, true, true);
      }
      client.dropDatabase(databaseName);
    } catch (TException e) {
    }

  }



  ///////// -------- UTILS ------- /////////
  // returns Path of the partition created (if any) else Path of table
  public static Path createDbAndTable(Driver driver, String databaseName,
                                      String tableName, List<String> partVals,
                                      String[] colNames, String[] colTypes,
                                      String[] bucketCols,
                                      String[] partNames, String dbLocation, int bucketCount)
          throws Exception {

    String dbUri = "raw://" + new Path(dbLocation).toUri().toString();
    String tableLoc = dbUri + Path.SEPARATOR + tableName;

    runDDL(driver, "create database IF NOT EXISTS " + databaseName + " location '" + dbUri + "'");
    runDDL(driver, "use " + databaseName);
    String crtTbl = "create table " + tableName +
            " ( " +  getTableColumnsStr(colNames,colTypes) + " )" +
            getPartitionStmtStr(partNames) +
            " clustered by ( " + join(bucketCols, ",") + " )" +
            " into " + bucketCount + " buckets " +
            " stored as orc " +
            " location '" + tableLoc +  "'";
    runDDL(driver, crtTbl);
    if(partNames!=null && partNames.length!=0) {
      return addPartition(driver, tableName, partVals, partNames);
    }
    return new Path(tableLoc);
  }

  private static Path addPartition(Driver driver, String tableName, List<String> partVals, String[] partNames) throws QueryFailedException, CommandNeedRetryException, IOException {
    String partSpec = getPartsSpec(partNames, partVals);
    String addPart = "alter table " + tableName + " add partition ( " + partSpec  + " )";
    runDDL(driver, addPart);
    return getPartitionPath(driver, tableName, partSpec);
  }

  private static Path getPartitionPath(Driver driver, String tableName, String partSpec) throws CommandNeedRetryException, IOException {
    ArrayList<String> res = queryTable(driver, "describe extended " + tableName + " PARTITION (" + partSpec + ")");
    String partInfo = res.get(res.size() - 1);
    int start = partInfo.indexOf("location:") + "location:".length();
    int end = partInfo.indexOf(",",start);
    return new Path( partInfo.substring(start,end) );
  }

  private static String getTableColumnsStr(String[] colNames, String[] colTypes) {
    StringBuffer sb = new StringBuffer();
    for (int i=0; i < colNames.length; ++i) {
      sb.append(colNames[i] + " " + colTypes[i]);
      if (i<colNames.length-1) {
        sb.append(",");
      }
    }
    return sb.toString();
  }

  // converts partNames into "partName1 string, partName2 string"
  private static String getTablePartsStr(String[] partNames) {
    if (partNames==null || partNames.length==0) {
      return "";
    }
    StringBuffer sb = new StringBuffer();
    for (int i=0; i < partNames.length; ++i) {
      sb.append(partNames[i] + " string");
      if (i < partNames.length-1) {
        sb.append(",");
      }
    }
    return sb.toString();
  }

  // converts partNames,partVals into "partName1=val1, partName2=val2"
  private static String getPartsSpec(String[] partNames, List<String> partVals) {
    StringBuffer sb = new StringBuffer();
    for (int i=0; i < partVals.size(); ++i) {
      sb.append(partNames[i] + " = '" + partVals.get(i) + "'");
      if(i < partVals.size()-1) {
        sb.append(",");
      }
    }
    return sb.toString();
  }

  private static String join(String[] values, String delimiter) {
    if(values==null)
      return null;
    StringBuffer strbuf = new StringBuffer();

    boolean first = true;

    for (Object value : values)  {
      if (!first) { strbuf.append(delimiter); } else { first = false; }
      strbuf.append(value.toString());
    }

    return strbuf.toString();
  }
  private static String getPartitionStmtStr(String[] partNames) {
    if ( partNames == null || partNames.length == 0) {
      return "";
    }
    return " partitioned by (" + getTablePartsStr(partNames) + " )";
  }

  private static boolean runDDL(Driver driver, String sql) throws QueryFailedException {
    LOG.debug(sql);
    System.out.println(sql);
    int retryCount = 1; // # of times to retry if first attempt fails
    for (int attempt=0; attempt <= retryCount; ++attempt) {
      try {
        //LOG.debug("Running Hive Query: "+ sql);
        CommandProcessorResponse cpr = driver.run(sql);
        if(cpr.getResponseCode() == 0) {
          return true;
        }
        LOG.error("Statement: " + sql + " failed: " + cpr);
      } catch (CommandNeedRetryException e) {
        if (attempt == retryCount) {
          throw new QueryFailedException(sql, e);
        }
        continue;
      }
    } // for
    return false;
  }


  public static ArrayList<String> queryTable(Driver driver, String query)
          throws CommandNeedRetryException, IOException {
    driver.run(query);
    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);
    if(res.isEmpty())
      System.err.println(driver.getErrorMsg());
    return res;
  }

  private static class SampleRec {
    public String field1;
    public int field2;
    public String field3;

    public SampleRec(String field1, int field2, String field3) {
      this.field1 = field1;
      this.field2 = field2;
      this.field3 = field3;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SampleRec that = (SampleRec) o;

      if (field2 != that.field2) return false;
      if (field1 != null ? !field1.equals(that.field1) : that.field1 != null) return false;
      return !(field3 != null ? !field3.equals(that.field3) : that.field3 != null);

    }

    @Override
    public int hashCode() {
      int result = field1 != null ? field1.hashCode() : 0;
      result = 31 * result + field2;
      result = 31 * result + (field3 != null ? field3.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return " { " +
              "'" + field1 + '\'' +
              "," + field2 +
              ",'" + field3 + '\'' +
              " }";
    }
  }
}
