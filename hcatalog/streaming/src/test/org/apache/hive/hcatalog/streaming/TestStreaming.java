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

package org.apache.hive.hcatalog.streaming;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.Validator;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponseElement;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnInfo;
import org.apache.hadoop.hive.metastore.api.TxnState;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.txn.AcidHouseKeeperService;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.BucketCodec;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.txn.compactor.Worker;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.orc.impl.OrcAcidUtils;
import org.apache.orc.tools.FileDump;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.BUCKET_COUNT;


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
  private IDriver driver;
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
    conf
    .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    TxnDbUtil.setConfValues(conf);
    if (metaStoreURI!=null) {
      conf.setVar(HiveConf.ConfVars.METASTOREURIS, metaStoreURI);
    }
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI, true);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
    dbFolder.create();


    //1) Start from a clean slate (metastore)
    TxnDbUtil.cleanDb(conf);
    TxnDbUtil.prepDb(conf);

    //2) obtain metastore clients
    msClient = new HiveMetaStoreClient(conf);
  }

  @Before
  public void setup() throws Exception {
    SessionState.start(new CliSessionState(conf));
    driver = DriverFactory.newDriver(conf);
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
    partLoc2 = createDbAndTable(driver, dbName2, tblName2, null, colNames, colTypes, bucketCols, null, loc2, 2);

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
    StreamingConnection connection = endPt.newConnection(false, "UT_" + Thread.currentThread().getName());
    DelimitedInputWriter writer = new DelimitedInputWriter(new String[] {"ss_sold_date_sk","ss_sold_time_sk", "ss_item_sk",
      "ss_customer_sk", "ss_cdemo_sk", "ss_hdemo_sk", "ss_addr_sk", "ss_store_sk", "ss_promo_sk", "ss_ticket_number", "ss_quantity",
      "ss_wholesale_cost", "ss_list_price", "ss_sales_price", "ss_ext_discount_amt", "ss_ext_sales_price", "ss_ext_wholesale_cost",
      "ss_ext_list_price", "ss_ext_tax", "ss_coupon_amt", "ss_net_paid", "ss_net_paid_inc_tax", "ss_net_profit"},",", endPt, connection);

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

  /**
   * Test that streaming can write to unbucketed table.
   */
  @Test
  public void testNoBuckets() throws Exception {
    queryTable(driver, "drop table if exists default.streamingnobuckets");
    //todo: why does it need transactional_properties?
    queryTable(driver, "create table default.streamingnobuckets (a string, b string) stored as orc TBLPROPERTIES('transactional'='true', 'transactional_properties'='default')");
    queryTable(driver, "insert into default.streamingnobuckets values('foo','bar')");
    List<String> rs = queryTable(driver, "select * from default.streamingnobuckets");
    Assert.assertEquals(1, rs.size());
    Assert.assertEquals("foo\tbar", rs.get(0));
    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, "default", "streamingnobuckets", null);
    String[] colNames1 = new String[] { "a", "b" };
    StreamingConnection connection = endPt.newConnection(false, "UT_" + Thread.currentThread().getName());
    DelimitedInputWriter wr = new DelimitedInputWriter(colNames1,",",  endPt, connection);

    TransactionBatch txnBatch =  connection.fetchTransactionBatch(2, wr);
    txnBatch.beginNextTransaction();
    txnBatch.write("a1,b2".getBytes());
    txnBatch.write("a3,b4".getBytes());
    txnBatch.commit();
    txnBatch.beginNextTransaction();
    txnBatch.write("a5,b6".getBytes());
    txnBatch.write("a7,b8".getBytes());
    txnBatch.commit();
    txnBatch.close();

    Assert.assertEquals("", 0, BucketCodec.determineVersion(536870912).decodeWriterId(536870912));
    rs = queryTable(driver,"select ROW__ID, a, b, INPUT__FILE__NAME from default.streamingnobuckets order by ROW__ID");

    Assert.assertTrue(rs.get(0), rs.get(0).startsWith("{\"transactionid\":16,\"bucketid\":536870912,\"rowid\":0}\tfoo\tbar"));
    Assert.assertTrue(rs.get(0), rs.get(0).endsWith("streamingnobuckets/delta_0000016_0000016_0000/bucket_00000"));
    Assert.assertTrue(rs.get(1), rs.get(1).startsWith("{\"transactionid\":18,\"bucketid\":536870912,\"rowid\":0}\ta1\tb2"));
    Assert.assertTrue(rs.get(1), rs.get(1).endsWith("streamingnobuckets/delta_0000018_0000019/bucket_00000"));
    Assert.assertTrue(rs.get(2), rs.get(2).startsWith("{\"transactionid\":18,\"bucketid\":536870912,\"rowid\":1}\ta3\tb4"));
    Assert.assertTrue(rs.get(2), rs.get(2).endsWith("streamingnobuckets/delta_0000018_0000019/bucket_00000"));
    Assert.assertTrue(rs.get(3), rs.get(3).startsWith("{\"transactionid\":19,\"bucketid\":536870912,\"rowid\":0}\ta5\tb6"));
    Assert.assertTrue(rs.get(3), rs.get(3).endsWith("streamingnobuckets/delta_0000018_0000019/bucket_00000"));
    Assert.assertTrue(rs.get(4), rs.get(4).startsWith("{\"transactionid\":19,\"bucketid\":536870912,\"rowid\":1}\ta7\tb8"));
    Assert.assertTrue(rs.get(4), rs.get(4).endsWith("streamingnobuckets/delta_0000018_0000019/bucket_00000"));

    queryTable(driver, "update default.streamingnobuckets set a=0, b=0 where a='a7'");
    queryTable(driver, "delete from default.streamingnobuckets where a='a1'");
    rs = queryTable(driver, "select a, b from default.streamingnobuckets order by a, b");
    int row = 0;
    Assert.assertEquals("at row=" + row, "0\t0", rs.get(row++));
    Assert.assertEquals("at row=" + row, "a3\tb4", rs.get(row++));
    Assert.assertEquals("at row=" + row, "a5\tb6", rs.get(row++));
    Assert.assertEquals("at row=" + row, "foo\tbar", rs.get(row++));

    queryTable(driver, "alter table default.streamingnobuckets compact 'major'");
    runWorker(conf);
    rs = queryTable(driver,"select ROW__ID, a, b, INPUT__FILE__NAME from default.streamingnobuckets order by ROW__ID");

    Assert.assertTrue(rs.get(0), rs.get(0).startsWith("{\"transactionid\":16,\"bucketid\":536870912,\"rowid\":0}\tfoo\tbar"));
    Assert.assertTrue(rs.get(0), rs.get(0).endsWith("streamingnobuckets/base_0000022/bucket_00000"));
    Assert.assertTrue(rs.get(1), rs.get(1).startsWith("{\"transactionid\":18,\"bucketid\":536870912,\"rowid\":1}\ta3\tb4"));
    Assert.assertTrue(rs.get(1), rs.get(1).endsWith("streamingnobuckets/base_0000022/bucket_00000"));
    Assert.assertTrue(rs.get(2), rs.get(2).startsWith("{\"transactionid\":19,\"bucketid\":536870912,\"rowid\":0}\ta5\tb6"));
    Assert.assertTrue(rs.get(2), rs.get(2).endsWith("streamingnobuckets/base_0000022/bucket_00000"));
    Assert.assertTrue(rs.get(3), rs.get(3).startsWith("{\"transactionid\":21,\"bucketid\":536870912,\"rowid\":0}\t0\t0"));
    Assert.assertTrue(rs.get(3), rs.get(3).endsWith("streamingnobuckets/base_0000022/bucket_00000"));
  }

  /**
   * this is a clone from TestTxnStatement2....
   */
  public static void runWorker(HiveConf hiveConf) throws MetaException {
    AtomicBoolean stop = new AtomicBoolean(true);
    Worker t = new Worker();
    t.setThreadId((int) t.getId());
    t.setConf(hiveConf);
    AtomicBoolean looped = new AtomicBoolean();
    t.init(stop, looped);
    t.run();
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
    StreamingConnection connection = endPt.newConnection(false, "UT_" + Thread.currentThread().getName());

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


  @Test
  public void testTableValidation() throws Exception {
    int bucketCount = 100;

    String dbUri = "raw://" + new Path(dbFolder.newFolder().toString()).toUri().toString();
    String tbl1 = "validation1";
    String tbl2 = "validation2";

    String tableLoc  = "'" + dbUri + Path.SEPARATOR + tbl1 + "'";
    String tableLoc2 = "'" + dbUri + Path.SEPARATOR + tbl2 + "'";

    runDDL(driver, "create database testBucketing3");
    runDDL(driver, "use testBucketing3");

    runDDL(driver, "create table " + tbl1 + " ( key1 string, data string ) clustered by ( key1 ) into "
            + bucketCount + " buckets  stored as orc  location " + tableLoc + " TBLPROPERTIES ('transactional'='false')") ;

    runDDL(driver, "create table " + tbl2 + " ( key1 string, data string ) clustered by ( key1 ) into "
            + bucketCount + " buckets  stored as orc  location " + tableLoc2 + " TBLPROPERTIES ('transactional'='false')") ;


    try {
      HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, "testBucketing3", "validation1", null);
      endPt.newConnection(false, "UT_" + Thread.currentThread().getName());
      Assert.assertTrue("InvalidTable exception was not thrown", false);
    } catch (InvalidTable e) {
      // expecting this exception
    }
    try {
      HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, "testBucketing3", "validation2", null);
      endPt.newConnection(false, "UT_" + Thread.currentThread().getName());
      Assert.assertTrue("InvalidTable exception was not thrown", false);
    } catch (InvalidTable e) {
      // expecting this exception
    }
  }

  /**
   * @deprecated use {@link #checkDataWritten2(Path, long, long, int, String, boolean, String...)} -
   * there is little value in using InputFormat directly
   */
  @Deprecated
  private void checkDataWritten(Path partitionPath, long minTxn, long maxTxn, int buckets, int numExpectedFiles,
                                String... records) throws Exception {
    ValidTxnList txns = msClient.getValidTxns();
    AcidUtils.Directory dir = AcidUtils.getAcidState(partitionPath, conf, txns);
    Assert.assertEquals(0, dir.getObsolete().size());
    Assert.assertEquals(0, dir.getOriginalFiles().size());
    List<AcidUtils.ParsedDelta> current = dir.getCurrentDirectories();
    System.out.println("Files found: ");
    for (AcidUtils.ParsedDelta pd : current) {
      System.out.println(pd.getPath().toString());
    }
    Assert.assertEquals(numExpectedFiles, current.size());

    // find the absolute minimum transaction
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (AcidUtils.ParsedDelta pd : current) {
      if (pd.getMaxTransaction() > max) {
        max = pd.getMaxTransaction();
      }
      if (pd.getMinTransaction() < min) {
        min = pd.getMinTransaction();
      }
    }
    Assert.assertEquals(minTxn, min);
    Assert.assertEquals(maxTxn, max);

    InputFormat inf = new OrcInputFormat();
    JobConf job = new JobConf();
    job.set("mapred.input.dir", partitionPath.toString());
    job.set(BUCKET_COUNT, Integer.toString(buckets));
    job.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS, "id,msg");
    job.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES, "bigint:string");
    AcidUtils.setAcidOperationalProperties(job, true, null);
    job.setBoolean(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, true);
    job.set(ValidTxnList.VALID_TXNS_KEY, txns.toString());
    InputSplit[] splits = inf.getSplits(job, buckets);
    Assert.assertEquals(numExpectedFiles, splits.length);
    org.apache.hadoop.mapred.RecordReader<NullWritable, OrcStruct> rr =
            inf.getRecordReader(splits[0], job, Reporter.NULL);

    NullWritable key = rr.createKey();
    OrcStruct value = rr.createValue();
    for (String record : records) {
      Assert.assertEquals(true, rr.next(key, value));
      Assert.assertEquals(record, value.toString());
    }
    Assert.assertEquals(false, rr.next(key, value));
  }
  /**
   * @param validationQuery query to read from table to compare data against {@code records}
   * @param records expected data.  each row is CVS list of values
   */
  private void checkDataWritten2(Path partitionPath, long minTxn, long maxTxn, int numExpectedFiles,
                                String validationQuery, boolean vectorize, String... records) throws Exception {
    ValidTxnList txns = msClient.getValidTxns();
    AcidUtils.Directory dir = AcidUtils.getAcidState(partitionPath, conf, txns);
    Assert.assertEquals(0, dir.getObsolete().size());
    Assert.assertEquals(0, dir.getOriginalFiles().size());
    List<AcidUtils.ParsedDelta> current = dir.getCurrentDirectories();
    System.out.println("Files found: ");
    for (AcidUtils.ParsedDelta pd : current) {
      System.out.println(pd.getPath().toString());
    }
    Assert.assertEquals(numExpectedFiles, current.size());

    // find the absolute minimum transaction
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (AcidUtils.ParsedDelta pd : current) {
      if (pd.getMaxTransaction() > max) {
        max = pd.getMaxTransaction();
      }
      if (pd.getMinTransaction() < min) {
        min = pd.getMinTransaction();
      }
    }
    Assert.assertEquals(minTxn, min);
    Assert.assertEquals(maxTxn, max);
    boolean isVectorizationEnabled = conf.getBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED);
    if(vectorize) {
      conf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    }

    String currStrategy = conf.getVar(HiveConf.ConfVars.HIVE_ORC_SPLIT_STRATEGY);
    for(String strategy : ((Validator.StringSet)HiveConf.ConfVars.HIVE_ORC_SPLIT_STRATEGY.getValidator()).getExpected()) {
      //run it with each split strategy - make sure there are differences
      conf.setVar(HiveConf.ConfVars.HIVE_ORC_SPLIT_STRATEGY, strategy.toUpperCase());
      List<String> actualResult = queryTable(driver, validationQuery);
      for (int i = 0; i < actualResult.size(); i++) {
        Assert.assertEquals("diff at [" + i + "].  actual=" + actualResult + " expected=" +
          Arrays.toString(records), records[i], actualResult.get(i));
      }
    }
    conf.setVar(HiveConf.ConfVars.HIVE_ORC_SPLIT_STRATEGY, currStrategy);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, isVectorizationEnabled);
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
    // For partitioned table, partitionVals are specified
    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName, tblName, partitionVals);
    StreamingConnection connection = endPt.newConnection(false, "UT_" + Thread.currentThread().getName()); //shouldn't throw
    connection.close();

    // For unpartitioned table, partitionVals are not specified
    endPt = new HiveEndPoint(metaStoreURI, dbName2, tblName2, null);
    endPt.newConnection(false, "UT_" + Thread.currentThread().getName()).close(); // should not throw

    // For partitioned table, partitionVals are not specified
    try {
      endPt = new HiveEndPoint(metaStoreURI, dbName, tblName, null);
      connection = endPt.newConnection(true, "UT_" + Thread.currentThread().getName());
      Assert.assertTrue("ConnectionError was not thrown", false);
      connection.close();
    } catch (ConnectionError e) {
      // expecting this exception
      String errMsg = "doesn't specify any partitions for partitioned table";
      Assert.assertTrue(e.toString().endsWith(errMsg));
    }

    // For unpartitioned table, partition values are specified
    try {
      endPt = new HiveEndPoint(metaStoreURI, dbName2, tblName2, partitionVals);
      connection = endPt.newConnection(false, "UT_" + Thread.currentThread().getName());
      Assert.assertTrue("ConnectionError was not thrown", false);
      connection.close();
    } catch (ConnectionError e) {
      // expecting this exception
      String errMsg = "specifies partitions for unpartitioned table";
      Assert.assertTrue(e.toString().endsWith(errMsg));
    }
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
    Assert.assertNotNull(endPt.newConnection(true, "UT_" + Thread.currentThread().getName()));

    // Ensure partition is present
    Partition p = msClient.getPartition(endPt.database, endPt.table, endPt.partitionVals);
    Assert.assertNotNull("Did not find added partition", p);
  }

  @Test
  public void testTransactionBatchEmptyCommit() throws Exception {
    // 1)  to partitioned table
    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName, tblName,
            partitionVals);
    StreamingConnection connection = endPt.newConnection(false, "UT_" + Thread.currentThread().getName());
    DelimitedInputWriter writer = new DelimitedInputWriter(fieldNames,",", endPt, connection);

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
    connection = endPt.newConnection(false, "UT_" + Thread.currentThread().getName());

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
    StreamingConnection connection = endPt.newConnection(false, "UT_" + Thread.currentThread().getName());

    TransactionBatch txnBatch =  connection.fetchTransactionBatch(5, writer);
    txnBatch.beginNextTransaction();
    conf.setTimeVar(HiveConf.ConfVars.HIVE_TIMEDOUT_TXN_REAPER_START, 0, TimeUnit.SECONDS);
    //ensure txn timesout
    conf.setTimeVar(HiveConf.ConfVars.HIVE_TXN_TIMEOUT, 1, TimeUnit.MILLISECONDS);
    AcidHouseKeeperService houseKeeperService = new AcidHouseKeeperService();
    houseKeeperService.setConf(conf);
    houseKeeperService.run();
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
    houseKeeperService.run();
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
  public void testHeartbeat() throws Exception {
    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName2, tblName2, null);
    StreamingConnection connection = endPt.newConnection(false, "UT_" + Thread.currentThread().getName());
    DelimitedInputWriter writer = new DelimitedInputWriter(fieldNames2,",", endPt, connection);

    TransactionBatch txnBatch =  connection.fetchTransactionBatch(5, writer);
    txnBatch.beginNextTransaction();
    //todo: this should ideally check Transaction heartbeat as well, but heartbeat
    //timestamp is not reported yet
    //GetOpenTxnsInfoResponse txnresp = msClient.showTxns();
    ShowLocksRequest request = new ShowLocksRequest();
    request.setDbname(dbName2);
    request.setTablename(tblName2);
    ShowLocksResponse response = msClient.showLocks(request);
    Assert.assertEquals("Wrong nubmer of locks: " + response, 1, response.getLocks().size());
    ShowLocksResponseElement lock = response.getLocks().get(0);
    long acquiredAt = lock.getAcquiredat();
    long heartbeatAt = lock.getLastheartbeat();
    txnBatch.heartbeat();
    response = msClient.showLocks(request);
    Assert.assertEquals("Wrong number of locks2: " + response, 1, response.getLocks().size());
    lock = response.getLocks().get(0);
    Assert.assertEquals("Acquired timestamp didn't match", acquiredAt, lock.getAcquiredat());
    Assert.assertTrue("Expected new heartbeat (" + lock.getLastheartbeat() +
      ") == old heartbeat(" + heartbeatAt +")", lock.getLastheartbeat() == heartbeatAt);
    txnBatch.close();
    int txnBatchSize = 200;
    txnBatch = connection.fetchTransactionBatch(txnBatchSize, writer);
    for(int i = 0; i < txnBatchSize; i++) {
      txnBatch.beginNextTransaction();
      if(i % 47 == 0) {
        txnBatch.heartbeat();
      }
      if(i % 10 == 0) {
        txnBatch.abort();
      }
      else {
        txnBatch.commit();
      }
      if(i % 37 == 0) {
        txnBatch.heartbeat();
      }
    }

  }
  @Test
  public void testTransactionBatchEmptyAbort() throws Exception {
    // 1) to partitioned table
    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName, tblName,
            partitionVals);
    StreamingConnection connection = endPt.newConnection(true, "UT_" + Thread.currentThread().getName());
    DelimitedInputWriter writer = new DelimitedInputWriter(fieldNames,",", endPt, connection);

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
    connection = endPt.newConnection(true, "UT_" + Thread.currentThread().getName());

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
    testTransactionBatchCommit_Delimited(null);
  }
  @Test
  public void testTransactionBatchCommit_DelimitedUGI() throws Exception {
    testTransactionBatchCommit_Delimited(Utils.getUGI());
  }
  private void testTransactionBatchCommit_Delimited(UserGroupInformation ugi) throws Exception {
    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName, tblName,
      partitionVals);
    StreamingConnection connection = endPt.newConnection(true, conf, ugi, "UT_" + Thread.currentThread().getName());
    DelimitedInputWriter writer = new DelimitedInputWriter(fieldNames,",", endPt, conf, connection);

    // 1st Txn
    TransactionBatch txnBatch =  connection.fetchTransactionBatch(10, writer);
    txnBatch.beginNextTransaction();
    Assert.assertEquals(TransactionBatch.TxnState.OPEN
      , txnBatch.getCurrentTransactionState());
    txnBatch.write("1,Hello streaming".getBytes());
    txnBatch.commit();

    checkDataWritten(partLoc, 15, 24, 1, 1, "{1, Hello streaming}");

    Assert.assertEquals(TransactionBatch.TxnState.COMMITTED
      , txnBatch.getCurrentTransactionState());

    // 2nd Txn
    txnBatch.beginNextTransaction();
    Assert.assertEquals(TransactionBatch.TxnState.OPEN
      , txnBatch.getCurrentTransactionState());
    txnBatch.write("2,Welcome to streaming".getBytes());

    // data should not be visible
    checkDataWritten(partLoc, 15, 24, 1, 1, "{1, Hello streaming}");

    txnBatch.commit();

    checkDataWritten(partLoc, 15, 24, 1, 1, "{1, Hello streaming}",
      "{2, Welcome to streaming}");

    txnBatch.close();
    Assert.assertEquals(TransactionBatch.TxnState.INACTIVE
      , txnBatch.getCurrentTransactionState());


    connection.close();


    // To Unpartitioned table
    endPt = new HiveEndPoint(metaStoreURI, dbName2, tblName2, null);
    connection = endPt.newConnection(true, conf, ugi, "UT_" + Thread.currentThread().getName());
    writer = new DelimitedInputWriter(fieldNames,",", endPt, conf, connection);

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
  public void testTransactionBatchCommit_Regex() throws Exception {
    testTransactionBatchCommit_Regex(null);
  }
  @Test
  public void testTransactionBatchCommit_RegexUGI() throws Exception {
    testTransactionBatchCommit_Regex(Utils.getUGI());
  }
  private void testTransactionBatchCommit_Regex(UserGroupInformation ugi) throws Exception {
    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName, tblName,
      partitionVals);
    StreamingConnection connection = endPt.newConnection(true, conf, ugi, "UT_" + Thread.currentThread().getName());
    String regex = "([^,]*),(.*)";
    StrictRegexWriter writer = new StrictRegexWriter(regex, endPt, conf, connection);

    // 1st Txn
    TransactionBatch txnBatch =  connection.fetchTransactionBatch(10, writer);
    txnBatch.beginNextTransaction();
    Assert.assertEquals(TransactionBatch.TxnState.OPEN
      , txnBatch.getCurrentTransactionState());
    txnBatch.write("1,Hello streaming".getBytes());
    txnBatch.commit();

    checkDataWritten(partLoc, 15, 24, 1, 1, "{1, Hello streaming}");

    Assert.assertEquals(TransactionBatch.TxnState.COMMITTED
      , txnBatch.getCurrentTransactionState());

    // 2nd Txn
    txnBatch.beginNextTransaction();
    Assert.assertEquals(TransactionBatch.TxnState.OPEN
      , txnBatch.getCurrentTransactionState());
    txnBatch.write("2,Welcome to streaming".getBytes());

    // data should not be visible
    checkDataWritten(partLoc, 15, 24, 1, 1, "{1, Hello streaming}");

    txnBatch.commit();

    checkDataWritten(partLoc, 15, 24, 1, 1, "{1, Hello streaming}",
      "{2, Welcome to streaming}");

    txnBatch.close();
    Assert.assertEquals(TransactionBatch.TxnState.INACTIVE
      , txnBatch.getCurrentTransactionState());


    connection.close();


    // To Unpartitioned table
    endPt = new HiveEndPoint(metaStoreURI, dbName2, tblName2, null);
    connection = endPt.newConnection(true, conf, ugi, "UT_" + Thread.currentThread().getName());
    regex = "([^:]*):(.*)";
    writer = new StrictRegexWriter(regex, endPt, conf, connection);

    // 1st Txn
    txnBatch =  connection.fetchTransactionBatch(10, writer);
    txnBatch.beginNextTransaction();
    Assert.assertEquals(TransactionBatch.TxnState.OPEN
      , txnBatch.getCurrentTransactionState());
    txnBatch.write("1:Hello streaming".getBytes());
    txnBatch.commit();

    Assert.assertEquals(TransactionBatch.TxnState.COMMITTED
      , txnBatch.getCurrentTransactionState());
    connection.close();
  }

  @Test
  public void testTransactionBatchCommit_Json() throws Exception {
    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName, tblName,
            partitionVals);
    StreamingConnection connection = endPt.newConnection(true, "UT_" + Thread.currentThread().getName());
    StrictJsonWriter writer = new StrictJsonWriter(endPt, connection);

    // 1st Txn
    TransactionBatch txnBatch =  connection.fetchTransactionBatch(10, writer);
    txnBatch.beginNextTransaction();
    Assert.assertEquals(TransactionBatch.TxnState.OPEN
            , txnBatch.getCurrentTransactionState());
    String rec1 = "{\"id\" : 1, \"msg\": \"Hello streaming\"}";
    txnBatch.write(rec1.getBytes());
    txnBatch.commit();

    checkDataWritten(partLoc, 15, 24, 1, 1, "{1, Hello streaming}");

    Assert.assertEquals(TransactionBatch.TxnState.COMMITTED
            , txnBatch.getCurrentTransactionState());

    txnBatch.close();
    Assert.assertEquals(TransactionBatch.TxnState.INACTIVE
            , txnBatch.getCurrentTransactionState());

    connection.close();
    List<String> rs = queryTable(driver, "select * from " + dbName + "." + tblName);
    Assert.assertEquals(1, rs.size());
  }

  @Test
  public void testRemainingTransactions() throws Exception {
    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName, tblName,
            partitionVals);
    DelimitedInputWriter writer = new DelimitedInputWriter(fieldNames,",", endPt);
    StreamingConnection connection = endPt.newConnection(true, "UT_" + Thread.currentThread().getName());

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
    StreamingConnection connection = endPt.newConnection(false, "UT_" + Thread.currentThread().getName());
    DelimitedInputWriter writer = new DelimitedInputWriter(fieldNames,",", endPt, connection);


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
    String agentInfo = "UT_" + Thread.currentThread().getName();
    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName, tblName,
            partitionVals);
    StreamingConnection connection = endPt.newConnection(false, agentInfo);
    DelimitedInputWriter writer = new DelimitedInputWriter(fieldNames,",", endPt, connection);

    TransactionBatch txnBatch =  connection.fetchTransactionBatch(10, writer);
    txnBatch.beginNextTransaction();
    txnBatch.write("1,Hello streaming".getBytes());
    txnBatch.write("2,Welcome to streaming".getBytes());
    ShowLocksResponse resp = msClient.showLocks(new ShowLocksRequest());
    Assert.assertEquals("LockCount", 1, resp.getLocksSize());
    Assert.assertEquals("LockType", LockType.SHARED_READ, resp.getLocks().get(0).getType());
    Assert.assertEquals("LockState", LockState.ACQUIRED, resp.getLocks().get(0).getState());
    Assert.assertEquals("AgentInfo", agentInfo, resp.getLocks().get(0).getAgentInfo());
    txnBatch.abort();

    checkNothingWritten(partLoc);

    Assert.assertEquals(TransactionBatch.TxnState.ABORTED
            , txnBatch.getCurrentTransactionState());

    txnBatch.beginNextTransaction();
    txnBatch.write("1,Hello streaming".getBytes());
    txnBatch.write("2,Welcome to streaming".getBytes());
    txnBatch.commit();

    checkDataWritten(partLoc, 14, 23, 1, 1, "{1, Hello streaming}",
            "{2, Welcome to streaming}");

    txnBatch.close();
    connection.close();
  }

  @Test
  public void testMultipleTransactionBatchCommits() throws Exception {
    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName, tblName,
            partitionVals);
    DelimitedInputWriter writer = new DelimitedInputWriter(fieldNames,",", endPt);
    StreamingConnection connection = endPt.newConnection(true, "UT_" + Thread.currentThread().getName());

    TransactionBatch txnBatch =  connection.fetchTransactionBatch(10, writer);
    txnBatch.beginNextTransaction();
    txnBatch.write("1,Hello streaming".getBytes());
    txnBatch.commit();
    String validationQuery = "select id, msg from " + dbName + "." + tblName + " order by id, msg";
    checkDataWritten2(partLoc, 15, 24, 1, validationQuery, false, "1\tHello streaming");

    txnBatch.beginNextTransaction();
    txnBatch.write("2,Welcome to streaming".getBytes());
    txnBatch.commit();

    checkDataWritten2(partLoc, 15, 24,  1, validationQuery, true, "1\tHello streaming",
            "2\tWelcome to streaming");

    txnBatch.close();

    // 2nd Txn Batch
    txnBatch =  connection.fetchTransactionBatch(10, writer);
    txnBatch.beginNextTransaction();
    txnBatch.write("3,Hello streaming - once again".getBytes());
    txnBatch.commit();

    checkDataWritten2(partLoc, 15, 40,  2, validationQuery, false, "1\tHello streaming",
            "2\tWelcome to streaming", "3\tHello streaming - once again");

    txnBatch.beginNextTransaction();
    txnBatch.write("4,Welcome to streaming - once again".getBytes());
    txnBatch.commit();

    checkDataWritten2(partLoc, 15, 40,  2, validationQuery, true, "1\tHello streaming",
            "2\tWelcome to streaming", "3\tHello streaming - once again",
            "4\tWelcome to streaming - once again");

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
    StreamingConnection connection = endPt.newConnection(false, "UT_" + Thread.currentThread().getName());

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

    String validationQuery = "select id, msg from " + dbName + "." + tblName + " order by id, msg";
    checkDataWritten2(partLoc, 24, 33, 1,
      validationQuery, true, "3\tHello streaming - once again");

    txnBatch1.commit();
    /*now both batches have committed (but not closed) so we for each primary file we expect a side
    file to exist and indicate the true length of primary file*/
    FileSystem fs = partLoc.getFileSystem(conf);
    AcidUtils.Directory dir = AcidUtils.getAcidState(partLoc, conf, msClient.getValidTxns());
    for(AcidUtils.ParsedDelta pd : dir.getCurrentDirectories()) {
      for(FileStatus stat : fs.listStatus(pd.getPath(), AcidUtils.bucketFileFilter)) {
        Path lengthFile = OrcAcidUtils.getSideFile(stat.getPath());
        Assert.assertTrue(lengthFile + " missing", fs.exists(lengthFile));
        long lengthFileSize = fs.getFileStatus(lengthFile).getLen();
        Assert.assertTrue("Expected " + lengthFile + " to be non empty. lengh=" +
          lengthFileSize, lengthFileSize > 0);
        long logicalLength = AcidUtils.getLogicalLength(fs, stat);
        long actualLength = stat.getLen();
        Assert.assertTrue("", logicalLength == actualLength);
      }
    }
    checkDataWritten2(partLoc, 14, 33, 2,
      validationQuery, false,"1\tHello streaming", "3\tHello streaming - once again");

    txnBatch1.beginNextTransaction();
    txnBatch1.write("2,Welcome to streaming".getBytes());

    txnBatch2.beginNextTransaction();
    txnBatch2.write("4,Welcome to streaming - once again".getBytes());
    //here each batch has written data and committed (to bucket0 since table only has 1 bucket)
    //so each of 2 deltas has 1 bucket0 and 1 bucket0_flush_length.  Furthermore, each bucket0
    //has now received more data(logically - it's buffered) but it is not yet committed.
    //lets check that side files exist, etc
    dir = AcidUtils.getAcidState(partLoc, conf, msClient.getValidTxns());
    for(AcidUtils.ParsedDelta pd : dir.getCurrentDirectories()) {
      for(FileStatus stat : fs.listStatus(pd.getPath(), AcidUtils.bucketFileFilter)) {
        Path lengthFile = OrcAcidUtils.getSideFile(stat.getPath());
        Assert.assertTrue(lengthFile + " missing", fs.exists(lengthFile));
        long lengthFileSize = fs.getFileStatus(lengthFile).getLen();
        Assert.assertTrue("Expected " + lengthFile + " to be non empty. lengh=" +
          lengthFileSize, lengthFileSize > 0);
        long logicalLength = AcidUtils.getLogicalLength(fs, stat);
        long actualLength = stat.getLen();
        Assert.assertTrue("", logicalLength <= actualLength);
      }
    }
    checkDataWritten2(partLoc, 14, 33, 2,
      validationQuery, true,"1\tHello streaming", "3\tHello streaming - once again");

    txnBatch1.commit();

    checkDataWritten2(partLoc, 14, 33, 2,
      validationQuery, false, "1\tHello streaming",
        "2\tWelcome to streaming",
        "3\tHello streaming - once again");

    txnBatch2.commit();

    checkDataWritten2(partLoc, 14, 33, 2,
      validationQuery, true, "1\tHello streaming",
        "2\tWelcome to streaming",
        "3\tHello streaming - once again",
        "4\tWelcome to streaming - once again");

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
      conn = ep.newConnection(false, "UT_" + Thread.currentThread().getName());
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

    RecordReader rows = reader.rows();
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
    String agentInfo = "UT_" + Thread.currentThread().getName();
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
    StreamingConnection connection = endPt.newConnection(false, agentInfo);
    DelimitedInputWriter writer = new DelimitedInputWriter(colNames,",", endPt, connection);

    TransactionBatch txnBatch =  connection.fetchTransactionBatch(2, writer);
    txnBatch.beginNextTransaction();
    txnBatch.write("name0,1,Hello streaming".getBytes());
    txnBatch.write("name2,2,Welcome to streaming".getBytes());
    txnBatch.write("name4,2,more Streaming unlimited".getBytes());
    txnBatch.write("name5,2,even more Streaming unlimited".getBytes());
    txnBatch.commit();


    HiveEndPoint endPt2 = new HiveEndPoint(metaStoreURI, dbName4, tblName4, null);
    StreamingConnection connection2 = endPt2.newConnection(false, agentInfo);
    DelimitedInputWriter writer2 = new DelimitedInputWriter(colNames2,",", endPt2, connection);
    TransactionBatch txnBatch2 =  connection2.fetchTransactionBatch(2, writer2);
    txnBatch2.beginNextTransaction();

    txnBatch2.write("name5,2,fact3".getBytes());  // bucket 0
    txnBatch2.write("name8,2,fact3".getBytes());  // bucket 1
    txnBatch2.write("name0,1,fact1".getBytes());  // bucket 2

    txnBatch2.commit();

    // 3 Check data distribution in  buckets

    HashMap<Integer, ArrayList<SampleRec>> actual1 = dumpAllBuckets(dbLocation, tblName3);
    HashMap<Integer, ArrayList<SampleRec>> actual2 = dumpAllBuckets(dbLocation2, tblName4);
    System.err.println("\n  Table 1");
    System.err.println(actual1);
    System.err.println("\n  Table 2");
    System.err.println(actual2);

    // assert bucket listing is as expected
    Assert.assertEquals("number of buckets does not match expectation", actual1.values().size(), 3);
    Assert.assertEquals("records in bucket does not match expectation", actual1.get(0).size(), 2);
    Assert.assertEquals("records in bucket does not match expectation", actual1.get(1).size(), 1);
    Assert.assertTrue("bucket 2 shouldn't have been created", actual1.get(2) == null);
    Assert.assertEquals("records in bucket does not match expectation", actual1.get(3).size(), 1);
  }
  private void runCmdOnDriver(String cmd) throws QueryFailedException {
    boolean t = runDDL(driver, cmd);
    Assert.assertTrue(cmd + " failed", t);
  }


  @Test
  public void testFileDump() throws Exception {
    String agentInfo = "UT_" + Thread.currentThread().getName();
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
    StreamingConnection connection = endPt.newConnection(false, agentInfo);
    DelimitedInputWriter writer = new DelimitedInputWriter(colNames,",", endPt, connection);

    TransactionBatch txnBatch =  connection.fetchTransactionBatch(2, writer);
    txnBatch.beginNextTransaction();
    txnBatch.write("name0,1,Hello streaming".getBytes());
    txnBatch.write("name2,2,Welcome to streaming".getBytes());
    txnBatch.write("name4,2,more Streaming unlimited".getBytes());
    txnBatch.write("name5,2,even more Streaming unlimited".getBytes());
    txnBatch.commit();

    PrintStream origErr = System.err;
    ByteArrayOutputStream myErr = new ByteArrayOutputStream();

    // replace stderr and run command
    System.setErr(new PrintStream(myErr));
    FileDump.main(new String[]{dbLocation});
    System.err.flush();
    System.setErr(origErr);

    String errDump = new String(myErr.toByteArray());
    Assert.assertEquals(false, errDump.contains("file(s) are corrupted"));
    // since this test runs on local file system which does not have an API to tell if files or
    // open or not, we are testing for negative case even though the bucket files are still open
    // for writes (transaction batch not closed yet)
    Assert.assertEquals(false, errDump.contains("is still open for writes."));

    HiveEndPoint endPt2 = new HiveEndPoint(metaStoreURI, dbName4, tblName4, null);
    DelimitedInputWriter writer2 = new DelimitedInputWriter(colNames2,",", endPt2);
    StreamingConnection connection2 = endPt2.newConnection(false, agentInfo);
    TransactionBatch txnBatch2 =  connection2.fetchTransactionBatch(2, writer2);
    txnBatch2.beginNextTransaction();

    txnBatch2.write("name5,2,fact3".getBytes());  // bucket 0
    txnBatch2.write("name8,2,fact3".getBytes());  // bucket 1
    txnBatch2.write("name0,1,fact1".getBytes());  // bucket 2
    // no data for bucket 3 -- expect 0 length bucket file

    txnBatch2.commit();

    origErr = System.err;
    myErr = new ByteArrayOutputStream();

    // replace stderr and run command
    System.setErr(new PrintStream(myErr));
    FileDump.main(new String[]{dbLocation});
    System.out.flush();
    System.err.flush();
    System.setErr(origErr);

    errDump = new String(myErr.toByteArray());
    Assert.assertEquals(false, errDump.contains("Exception"));
    Assert.assertEquals(false, errDump.contains("file(s) are corrupted"));
    Assert.assertEquals(false, errDump.contains("is still open for writes."));
  }

  @Test
  public void testFileDumpCorruptDataFiles() throws Exception {
    dropDB(msClient, dbName3);

    // 1) Create two bucketed tables
    String dbLocation = dbFolder.newFolder(dbName3).getCanonicalPath() + ".db";
    dbLocation = dbLocation.replaceAll("\\\\","/"); // for windows paths
    String[] colNames = "key1,key2,data".split(",");
    String[] colTypes = "string,int,string".split(",");
    String[] bucketNames = "key1,key2".split(",");
    int bucketCount = 4;
    createDbAndTable(driver, dbName3, tblName3, null, colNames, colTypes, bucketNames
        , null, dbLocation, bucketCount);

    // 2) Insert data into both tables
    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName3, tblName3, null);
    StreamingConnection connection = endPt.newConnection(false, "UT_" + Thread.currentThread().getName());
    DelimitedInputWriter writer = new DelimitedInputWriter(colNames,",", endPt, connection);

    // we need side file for this test, so we create 2 txn batch and test with only one
    TransactionBatch txnBatch =  connection.fetchTransactionBatch(2, writer);
    txnBatch.beginNextTransaction();
    txnBatch.write("name0,1,Hello streaming".getBytes());
    txnBatch.write("name2,2,Welcome to streaming".getBytes());
    txnBatch.write("name4,2,more Streaming unlimited".getBytes());
    txnBatch.write("name5,2,even more Streaming unlimited".getBytes());
    txnBatch.commit();

    // intentionally corrupt some files
    Path path = new Path(dbLocation);
    Collection<String> files = FileDump.getAllFilesInPath(path, conf);
    int readableFooter = -1;
    for (String file : files) {
      if (file.contains("bucket_00000")) {
        // empty out the file
        corruptDataFile(file, conf, Integer.MIN_VALUE);
      } else if (file.contains("bucket_00001")) {
        corruptDataFile(file, conf, -1);
      } else if (file.contains("bucket_00002")) {
        Assert.assertFalse("bucket 2 shouldn't have been created", true);
      } else if (file.contains("bucket_00003")) {
        corruptDataFile(file, conf, 100);
      }
    }

    PrintStream origErr = System.err;
    ByteArrayOutputStream myErr = new ByteArrayOutputStream();

    // replace stderr and run command
    System.setErr(new PrintStream(myErr));
    FileDump.main(new String[]{dbLocation});
    System.err.flush();
    System.setErr(origErr);

    String errDump = new String(myErr.toByteArray());
    Assert.assertEquals(false, errDump.contains("Exception"));
    Assert.assertEquals(true, errDump.contains("3 file(s) are corrupted"));
    Assert.assertEquals(false, errDump.contains("is still open for writes."));

    origErr = System.err;
    myErr = new ByteArrayOutputStream();

    // replace stderr and run command
    System.setErr(new PrintStream(myErr));
    FileDump.main(new String[]{dbLocation, "--recover", "--skip-dump"});
    System.err.flush();
    System.setErr(origErr);

    errDump = new String(myErr.toByteArray());
    Assert.assertEquals(true, errDump.contains("bucket_00000 recovered successfully!"));
    Assert.assertEquals(true, errDump.contains("No readable footers found. Creating empty orc file."));
    Assert.assertEquals(true, errDump.contains("bucket_00001 recovered successfully!"));
    Assert.assertEquals(true, errDump.contains("bucket_00003 recovered successfully!"));
    Assert.assertEquals(false, errDump.contains("Exception"));
    Assert.assertEquals(false, errDump.contains("is still open for writes."));

    // test after recovery
    origErr = System.err;
    myErr = new ByteArrayOutputStream();

    // replace stdout and run command
    System.setErr(new PrintStream(myErr));
    FileDump.main(new String[]{dbLocation});
    System.err.flush();
    System.setErr(origErr);

    errDump = new String(myErr.toByteArray());
    Assert.assertEquals(false, errDump.contains("Exception"));
    Assert.assertEquals(false, errDump.contains("file(s) are corrupted"));
    Assert.assertEquals(false, errDump.contains("is still open for writes."));

    // after recovery there shouldn't be any *_flush_length files
    files = FileDump.getAllFilesInPath(path, conf);
    for (String file : files) {
      Assert.assertEquals(false, file.contains("_flush_length"));
    }

    txnBatch.close();
  }

  private void corruptDataFile(final String file, final Configuration conf, final int addRemoveBytes)
      throws Exception {
    Path bPath = new Path(file);
    Path cPath = new Path(bPath.getParent(), bPath.getName() + ".corrupt");
    FileSystem fs = bPath.getFileSystem(conf);
    FileStatus fileStatus = fs.getFileStatus(bPath);
    int len = addRemoveBytes == Integer.MIN_VALUE ? 0 : (int) fileStatus.getLen() + addRemoveBytes;
    byte[] buffer = new byte[len];
    FSDataInputStream fdis = fs.open(bPath);
    fdis.readFully(0, buffer, 0, (int) Math.min(fileStatus.getLen(), buffer.length));
    fdis.close();
    FSDataOutputStream fdos = fs.create(cPath, true);
    fdos.write(buffer, 0, buffer.length);
    fdos.close();
    fs.delete(bPath, false);
    fs.rename(cPath, bPath);
  }

  @Test
  public void testFileDumpCorruptSideFiles() throws Exception {
    dropDB(msClient, dbName3);

    // 1) Create two bucketed tables
    String dbLocation = dbFolder.newFolder(dbName3).getCanonicalPath() + ".db";
    dbLocation = dbLocation.replaceAll("\\\\","/"); // for windows paths
    String[] colNames = "key1,key2,data".split(",");
    String[] colTypes = "string,int,string".split(",");
    String[] bucketNames = "key1,key2".split(",");
    int bucketCount = 4;
    createDbAndTable(driver, dbName3, tblName3, null, colNames, colTypes, bucketNames
        , null, dbLocation, bucketCount);

    // 2) Insert data into both tables
    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, dbName3, tblName3, null);
    StreamingConnection connection = endPt.newConnection(false, "UT_" + Thread.currentThread().getName());
    DelimitedInputWriter writer = new DelimitedInputWriter(colNames,",", endPt, connection);

    TransactionBatch txnBatch =  connection.fetchTransactionBatch(2, writer);
    txnBatch.beginNextTransaction();
    txnBatch.write("name0,1,Hello streaming".getBytes());
    txnBatch.write("name2,2,Welcome to streaming".getBytes());
    txnBatch.write("name4,2,more Streaming unlimited".getBytes());
    txnBatch.write("name5,2,even more Streaming unlimited".getBytes());
    txnBatch.write("name6,3,aHello streaming".getBytes());
    txnBatch.commit();

    Map<String,List<Long>> offsetMap = new HashMap<String,List<Long>>();
    recordOffsets(conf, dbLocation, offsetMap);

    txnBatch.beginNextTransaction();
    txnBatch.write("name01,11,-Hello streaming".getBytes());
    txnBatch.write("name21,21,-Welcome to streaming".getBytes());
    txnBatch.write("name41,21,-more Streaming unlimited".getBytes());
    txnBatch.write("name51,21,-even more Streaming unlimited".getBytes());
    txnBatch.write("name02,12,--Hello streaming".getBytes());
    txnBatch.write("name22,22,--Welcome to streaming".getBytes());
    txnBatch.write("name42,22,--more Streaming unlimited".getBytes());
    txnBatch.write("name52,22,--even more Streaming unlimited".getBytes());
    txnBatch.write("name7,4,aWelcome to streaming".getBytes());
    txnBatch.write("name8,5,amore Streaming unlimited".getBytes());
    txnBatch.write("name9,6,aeven more Streaming unlimited".getBytes());
    txnBatch.write("name10,7,bHello streaming".getBytes());
    txnBatch.write("name11,8,bWelcome to streaming".getBytes());
    txnBatch.write("name12,9,bmore Streaming unlimited".getBytes());
    txnBatch.write("name13,10,beven more Streaming unlimited".getBytes());
    txnBatch.commit();

    recordOffsets(conf, dbLocation, offsetMap);

    // intentionally corrupt some files
    Path path = new Path(dbLocation);
    Collection<String> files = FileDump.getAllFilesInPath(path, conf);
    for (String file : files) {
      if (file.contains("bucket_00000")) {
        corruptSideFile(file, conf, offsetMap, "bucket_00000", -1); // corrupt last entry
      } else if (file.contains("bucket_00001")) {
        corruptSideFile(file, conf, offsetMap, "bucket_00001", 0); // empty out side file
      } else if (file.contains("bucket_00002")) {
        corruptSideFile(file, conf, offsetMap, "bucket_00002", 3); // total 3 entries (2 valid + 1 fake)
      } else if (file.contains("bucket_00003")) {
        corruptSideFile(file, conf, offsetMap, "bucket_00003", 10); // total 10 entries (2 valid + 8 fake)
      }
    }

    PrintStream origErr = System.err;
    ByteArrayOutputStream myErr = new ByteArrayOutputStream();

    // replace stderr and run command
    System.setErr(new PrintStream(myErr));
    FileDump.main(new String[]{dbLocation});
    System.err.flush();
    System.setErr(origErr);

    String errDump = new String(myErr.toByteArray());
    Assert.assertEquals(true, errDump.contains("bucket_00000_flush_length [length: 11"));
    Assert.assertEquals(true, errDump.contains("bucket_00001_flush_length [length: 0"));
    Assert.assertEquals(true, errDump.contains("bucket_00002_flush_length [length: 24"));
    Assert.assertEquals(true, errDump.contains("bucket_00003_flush_length [length: 80"));
    Assert.assertEquals(false, errDump.contains("Exception"));
    Assert.assertEquals(true, errDump.contains("4 file(s) are corrupted"));
    Assert.assertEquals(false, errDump.contains("is still open for writes."));

    origErr = System.err;
    myErr = new ByteArrayOutputStream();

    // replace stderr and run command
    System.setErr(new PrintStream(myErr));
    FileDump.main(new String[]{dbLocation, "--recover", "--skip-dump"});
    System.err.flush();
    System.setErr(origErr);

    errDump = new String(myErr.toByteArray());
    Assert.assertEquals(true, errDump.contains("bucket_00000 recovered successfully!"));
    Assert.assertEquals(true, errDump.contains("bucket_00001 recovered successfully!"));
    Assert.assertEquals(true, errDump.contains("bucket_00002 recovered successfully!"));
    Assert.assertEquals(true, errDump.contains("bucket_00003 recovered successfully!"));
    List<Long> offsets = offsetMap.get("bucket_00000");
    Assert.assertEquals(true, errDump.contains("Readable footerOffsets: " + offsets.toString()));
    offsets = offsetMap.get("bucket_00001");
    Assert.assertEquals(true, errDump.contains("Readable footerOffsets: " + offsets.toString()));
    offsets = offsetMap.get("bucket_00002");
    Assert.assertEquals(true, errDump.contains("Readable footerOffsets: " + offsets.toString()));
    offsets = offsetMap.get("bucket_00003");
    Assert.assertEquals(true, errDump.contains("Readable footerOffsets: " + offsets.toString()));
    Assert.assertEquals(false, errDump.contains("Exception"));
    Assert.assertEquals(false, errDump.contains("is still open for writes."));

    // test after recovery
    origErr = System.err;
    myErr = new ByteArrayOutputStream();

    // replace stdout and run command
    System.setErr(new PrintStream(myErr));
    FileDump.main(new String[]{dbLocation});
    System.err.flush();
    System.setErr(origErr);

    errDump = new String(myErr.toByteArray());
    Assert.assertEquals(false, errDump.contains("Exception"));
    Assert.assertEquals(false, errDump.contains("file(s) are corrupted"));
    Assert.assertEquals(false, errDump.contains("is still open for writes."));

    // after recovery there shouldn't be any *_flush_length files
    files = FileDump.getAllFilesInPath(path, conf);
    for (String file : files) {
      Assert.assertEquals(false, file.contains("_flush_length"));
    }

    txnBatch.close();
  }

  private void corruptSideFile(final String file, final HiveConf conf,
      final Map<String, List<Long>> offsetMap, final String key, final int numEntries)
      throws IOException {
    Path dataPath = new Path(file);
    Path sideFilePath = OrcAcidUtils.getSideFile(dataPath);
    Path cPath = new Path(sideFilePath.getParent(), sideFilePath.getName() + ".corrupt");
    FileSystem fs = sideFilePath.getFileSystem(conf);
    List<Long> offsets = offsetMap.get(key);
    long lastOffset = offsets.get(offsets.size() - 1);
    FSDataOutputStream fdos = fs.create(cPath, true);
    // corrupt last entry
    if (numEntries < 0) {
      byte[] lastOffsetBytes = longToBytes(lastOffset);
      for (int i = 0; i < offsets.size() - 1; i++) {
        fdos.writeLong(offsets.get(i));
      }

      fdos.write(lastOffsetBytes, 0, 3);
    } else if (numEntries > 0) {
      int firstRun = Math.min(offsets.size(), numEntries);
      // add original entries
      for (int i=0; i < firstRun; i++) {
        fdos.writeLong(offsets.get(i));
      }

      // add fake entries
      int remaining = numEntries - firstRun;
      for (int i = 0; i < remaining; i++) {
        fdos.writeLong(lastOffset + ((i + 1) * 100));
      }
    }

    fdos.close();
    fs.delete(sideFilePath, false);
    fs.rename(cPath, sideFilePath);
  }

  private  byte[] longToBytes(long x) {
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.putLong(x);
    return buffer.array();
  }

  private void recordOffsets(final HiveConf conf, final String dbLocation,
      final Map<String, List<Long>> offsetMap) throws IOException {
    Path path = new Path(dbLocation);
    Collection<String> files = FileDump.getAllFilesInPath(path, conf);
    for (String file: files) {
      Path bPath = new Path(file);
      FileSystem fs = bPath.getFileSystem(conf);
      FileStatus fileStatus = fs.getFileStatus(bPath);
      long len = fileStatus.getLen();

      if (file.contains("bucket_00000")) {
        if (offsetMap.containsKey("bucket_00000")) {
          List<Long> offsets = offsetMap.get("bucket_00000");
          offsets.add(len);
          offsetMap.put("bucket_00000", offsets);
        } else {
          List<Long> offsets = new ArrayList<Long>();
          offsets.add(len);
          offsetMap.put("bucket_00000", offsets);
        }
      } else if (file.contains("bucket_00001")) {
        if (offsetMap.containsKey("bucket_00001")) {
          List<Long> offsets = offsetMap.get("bucket_00001");
          offsets.add(len);
          offsetMap.put("bucket_00001", offsets);
        } else {
          List<Long> offsets = new ArrayList<Long>();
          offsets.add(len);
          offsetMap.put("bucket_00001", offsets);
        }
      } else if (file.contains("bucket_00002")) {
        if (offsetMap.containsKey("bucket_00002")) {
          List<Long> offsets = offsetMap.get("bucket_00002");
          offsets.add(len);
          offsetMap.put("bucket_00002", offsets);
        } else {
          List<Long> offsets = new ArrayList<Long>();
          offsets.add(len);
          offsetMap.put("bucket_00002", offsets);
        }
      } else if (file.contains("bucket_00003")) {
        if (offsetMap.containsKey("bucket_00003")) {
          List<Long> offsets = offsetMap.get("bucket_00003");
          offsets.add(len);
          offsetMap.put("bucket_00003", offsets);
        } else {
          List<Long> offsets = new ArrayList<Long>();
          offsets.add(len);
          offsetMap.put("bucket_00003", offsets);
        }
      }
    }
  }

  @Test
  public void testErrorHandling() throws Exception {
    String agentInfo = "UT_" + Thread.currentThread().getName();
    runCmdOnDriver("create database testErrors");
    runCmdOnDriver("use testErrors");
    runCmdOnDriver("create table T(a int, b int) clustered by (b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");

    HiveEndPoint endPt = new HiveEndPoint(metaStoreURI, "testErrors", "T", null);
    StreamingConnection connection = endPt.newConnection(false, agentInfo);
    DelimitedInputWriter innerWriter = new DelimitedInputWriter("a,b".split(","),",", endPt, connection);
    FaultyWriter writer = new FaultyWriter(innerWriter);

    TransactionBatch txnBatch =  connection.fetchTransactionBatch(2, writer);
    txnBatch.close();
    txnBatch.heartbeat();//this is no-op on closed batch
    txnBatch.abort();//ditto
    GetOpenTxnsInfoResponse r = msClient.showTxns();
    Assert.assertEquals("HWM didn't match", 17, r.getTxn_high_water_mark());
    List<TxnInfo> ti = r.getOpen_txns();
    Assert.assertEquals("wrong status ti(0)", TxnState.ABORTED, ti.get(0).getState());
    Assert.assertEquals("wrong status ti(1)", TxnState.ABORTED, ti.get(1).getState());

    Exception expectedEx = null;
    try {
      txnBatch.beginNextTransaction();
    }
    catch(IllegalStateException ex) {
      expectedEx = ex;
    }
    Assert.assertTrue("beginNextTransaction() should have failed",
      expectedEx != null && expectedEx.getMessage().contains("has been closed()"));
    expectedEx = null;
    try {
      txnBatch.write("name0,1,Hello streaming".getBytes());
    }
    catch(IllegalStateException ex) {
      expectedEx = ex;
    }
    Assert.assertTrue("write()  should have failed",
      expectedEx != null && expectedEx.getMessage().contains("has been closed()"));
    expectedEx = null;
    try {
      txnBatch.commit();
    }
    catch(IllegalStateException ex) {
      expectedEx = ex;
    }
    Assert.assertTrue("commit() should have failed",
      expectedEx != null && expectedEx.getMessage().contains("has been closed()"));

    txnBatch =  connection.fetchTransactionBatch(2, writer);
    txnBatch.beginNextTransaction();
    txnBatch.write("name2,2,Welcome to streaming".getBytes());
    txnBatch.write("name4,2,more Streaming unlimited".getBytes());
    txnBatch.write("name5,2,even more Streaming unlimited".getBytes());
    txnBatch.commit();

    //test toString()
    String s = txnBatch.toString();
    Assert.assertTrue("Actual: " + s, s.contains("LastUsed " + JavaUtils.txnIdToString(txnBatch.getCurrentTxnId())));
    Assert.assertTrue("Actual: " + s, s.contains("TxnStatus[CO]"));

    expectedEx = null;
    txnBatch.beginNextTransaction();
    writer.enableErrors();
    try {
      txnBatch.write("name6,2,Doh!".getBytes());
    }
    catch(StreamingIOFailure ex) {
      expectedEx = ex;
      txnBatch.getCurrentTransactionState();
      txnBatch.getCurrentTxnId();//test it doesn't throw ArrayIndexOutOfBounds...
    }
    Assert.assertTrue("Wrong exception: " + (expectedEx != null ? expectedEx.getMessage() : "?"),
      expectedEx != null && expectedEx.getMessage().contains("Simulated fault occurred"));
    expectedEx = null;
    try {
      txnBatch.commit();
    }
    catch(IllegalStateException ex) {
      expectedEx = ex;
    }
    Assert.assertTrue("commit() should have failed",
      expectedEx != null && expectedEx.getMessage().contains("has been closed()"));

    //test toString()
    s = txnBatch.toString();
    Assert.assertTrue("Actual: " + s, s.contains("LastUsed " + JavaUtils.txnIdToString(txnBatch.getCurrentTxnId())));
    Assert.assertTrue("Actual: " + s, s.contains("TxnStatus[CA]"));

    r = msClient.showTxns();
    Assert.assertEquals("HWM didn't match", 19, r.getTxn_high_water_mark());
    ti = r.getOpen_txns();
    Assert.assertEquals("wrong status ti(0)", TxnState.ABORTED, ti.get(0).getState());
    Assert.assertEquals("wrong status ti(1)", TxnState.ABORTED, ti.get(1).getState());
    //txnid 3 was committed and thus not open
    Assert.assertEquals("wrong status ti(2)", TxnState.ABORTED, ti.get(2).getState());

    writer.disableErrors();
    txnBatch =  connection.fetchTransactionBatch(2, writer);
    txnBatch.beginNextTransaction();
    txnBatch.write("name2,2,Welcome to streaming".getBytes());
    writer.enableErrors();
    expectedEx = null;
    try {
      txnBatch.commit();
    }
    catch(StreamingIOFailure ex) {
      expectedEx = ex;
    }
    Assert.assertTrue("Wrong exception: " + (expectedEx != null ? expectedEx.getMessage() : "?"),
      expectedEx != null && expectedEx.getMessage().contains("Simulated fault occurred"));

    r = msClient.showTxns();
    Assert.assertEquals("HWM didn't match", 21, r.getTxn_high_water_mark());
    ti = r.getOpen_txns();
    Assert.assertEquals("wrong status ti(3)", TxnState.ABORTED, ti.get(3).getState());
    Assert.assertEquals("wrong status ti(4)", TxnState.ABORTED, ti.get(4).getState());

    txnBatch.abort();
  }

    // assumes un partitioned table
  // returns a map<bucketNum, list<record> >
  private HashMap<Integer, ArrayList<SampleRec>> dumpAllBuckets(String dbLocation, String tableName)
          throws IOException {
    HashMap<Integer, ArrayList<SampleRec>> result = new HashMap<Integer, ArrayList<SampleRec>>();

    for (File deltaDir : new File(dbLocation + "/" + tableName).listFiles()) {
      if(!deltaDir.getName().startsWith("delta")) {
        continue;
      }
      File[] bucketFiles = deltaDir.listFiles();
      for (File bucketFile : bucketFiles) {
        if(bucketFile.toString().endsWith("length")) {
          continue;
        }
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
  public static Path createDbAndTable(IDriver driver, String databaseName,
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
            " location '" + tableLoc +  "'" +
            " TBLPROPERTIES ('transactional'='true') ";
    runDDL(driver, crtTbl);
    if(partNames!=null && partNames.length!=0) {
      return addPartition(driver, tableName, partVals, partNames);
    }
    return new Path(tableLoc);
  }

  private static Path addPartition(IDriver driver, String tableName, List<String> partVals, String[] partNames)
      throws Exception {
    String partSpec = getPartsSpec(partNames, partVals);
    String addPart = "alter table " + tableName + " add partition ( " + partSpec  + " )";
    runDDL(driver, addPart);
    return getPartitionPath(driver, tableName, partSpec);
  }

  private static Path getPartitionPath(IDriver driver, String tableName, String partSpec) throws Exception {
    ArrayList<String> res = queryTable(driver, "describe extended " + tableName + " PARTITION (" + partSpec + ")");
    String partInfo = res.get(res.size() - 1);
    int start = partInfo.indexOf("location:") + "location:".length();
    int end = partInfo.indexOf(",",start);
    return new Path( partInfo.substring(start,end) );
  }

  private static String getTableColumnsStr(String[] colNames, String[] colTypes) {
    StringBuilder sb = new StringBuilder();
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
    StringBuilder sb = new StringBuilder();
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
    StringBuilder sb = new StringBuilder();
    for (int i=0; i < partVals.size(); ++i) {
      sb.append(partNames[i] + " = '" + partVals.get(i) + "'");
      if(i < partVals.size()-1) {
        sb.append(",");
      }
    }
    return sb.toString();
  }

  private static String join(String[] values, String delimiter) {
    if(values==null) {
      return null;
    }
    StringBuilder strbuf = new StringBuilder();

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

  private static boolean runDDL(IDriver driver, String sql) throws QueryFailedException {
    LOG.debug(sql);
    System.out.println(sql);
    //LOG.debug("Running Hive Query: "+ sql);
    CommandProcessorResponse cpr = driver.run(sql);
    if (cpr.getResponseCode() == 0) {
      return true;
    }
    LOG.error("Statement: " + sql + " failed: " + cpr);
    return false;
  }


  public static ArrayList<String> queryTable(IDriver driver, String query) throws IOException {
    CommandProcessorResponse cpr = driver.run(query);
    if(cpr.getResponseCode() != 0) {
      throw new RuntimeException(query + " failed: " + cpr);
    }
    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);
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
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      SampleRec that = (SampleRec) o;

      if (field2 != that.field2) {
        return false;
      }
      if (field1 != null ? !field1.equals(that.field1) : that.field1 != null) {
        return false;
      }
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
  /**
   * This is test-only wrapper around the real RecordWriter.
   * It can simulate faults from lower levels to test error handling logic.
   */
  private static final class FaultyWriter implements RecordWriter {
    private final RecordWriter delegate;
    private boolean shouldThrow = false;

    private FaultyWriter(RecordWriter delegate) {
      assert delegate != null;
      this.delegate = delegate;
    }
    @Override
    public void write(long transactionId, byte[] record) throws StreamingException {
      delegate.write(transactionId, record);
      produceFault();
    }
    @Override
    public void flush() throws StreamingException {
      delegate.flush();
      produceFault();
    }
    @Override
    public void clear() throws StreamingException {
      delegate.clear();
    }
    @Override
    public void newBatch(Long minTxnId, Long maxTxnID) throws StreamingException {
      delegate.newBatch(minTxnId, maxTxnID);
    }
    @Override
    public void closeBatch() throws StreamingException {
      delegate.closeBatch();
    }

    /**
     * allows testing of "unexpected" errors
     * @throws StreamingIOFailure
     */
    private void produceFault() throws StreamingIOFailure {
      if(shouldThrow) {
        throw new StreamingIOFailure("Simulated fault occurred");
      }
    }
    void enableErrors() {
      shouldThrow = true;
    }
    void disableErrors() {
      shouldThrow = false;
    }
  }
}
