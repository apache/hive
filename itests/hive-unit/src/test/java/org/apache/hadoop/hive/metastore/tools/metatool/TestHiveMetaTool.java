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

package org.apache.hadoop.hive.metastore.tools.metatool;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


import org.json.JSONObject;
import org.json.JSONArray;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils.AvroTableProperties;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;
import org.junit.Assert;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import com.google.gson.JsonParser;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

/** Integration tests for the HiveMetaTool program. */
public class TestHiveMetaTool {
  private static final String DB_NAME = "TestHiveMetaToolDB";
  private static final String TABLE_NAME = "simpleTbl";
  private static final String LOCATION = "hdfs://nn.example.com/";
  private static final String NEW_LOCATION = "hdfs://nn-ha-uri/";
  private static final String PATH = "warehouse/hive/ab.avsc";
  private static final String AVRO_URI = LOCATION + PATH;
  private static final String NEW_AVRO_URI = NEW_LOCATION + PATH;

  private HiveMetaStoreClient client;
  private OutputStream os;
  protected Driver d;
  protected TxnStore txnHandler;
  private static HiveConf hiveConf;
  private static final String TEST_DATA_DIR = new File(System.getProperty("java.io.tmpdir") +
          File.separator + TestHiveMetaTool.class.getCanonicalName() + "-" + System.currentTimeMillis()
  ).getPath().replaceAll("\\\\", "/");
  @Before
  public void setUp() throws Exception {


    try {
      os = new ByteArrayOutputStream();
      System.setOut(new PrintStream(os));

      hiveConf = new HiveConf(HiveMetaTool.class);
      client = new HiveMetaStoreClient(hiveConf);

      createDatabase();
      createTable();

      client.close();
      Path workDir = new Path(System.getProperty("test.tmp.dir",
              "target" + File.separator + "test" + File.separator + "tmp"));
      hiveConf.set("mapred.local.dir", workDir + File.separator + this.getClass().getSimpleName()
              + File.separator + "mapred" + File.separator + "local");
      hiveConf.set("mapred.system.dir", workDir + File.separator + this.getClass().getSimpleName()
              + File.separator + "mapred" + File.separator + "system");
      hiveConf.set("mapreduce.jobtracker.staging.root.dir", workDir + File.separator + this.getClass().getSimpleName()
              + File.separator + "mapred" + File.separator + "staging");
      hiveConf.set("mapred.temp.dir", workDir + File.separator + this.getClass().getSimpleName()
              + File.separator + "mapred" + File.separator + "temp");
      hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
      hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
      hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, getWarehouseDir());
      hiveConf.setVar(HiveConf.ConfVars.HIVEINPUTFORMAT, HiveInputFormat.class.getName());
      hiveConf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
                      "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
      hiveConf.setBoolVar(HiveConf.ConfVars.MERGE_CARDINALITY_VIOLATION_CHECK, true);
      HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.MERGE_SPLIT_UPDATE, true);
      hiveConf.setBoolVar(HiveConf.ConfVars.HIVESTATSCOLAUTOGATHER, false);
      hiveConf.setBoolean("mapred.input.dir.recursive", true);
      TxnDbUtil.setConfValues(hiveConf);
      txnHandler = TxnUtils.getTxnStore(hiveConf);
      TxnDbUtil.prepDb(hiveConf);
      File f = new File(getWarehouseDir());
      if (f.exists()) {
        FileUtil.fullyDelete(f);
      }
      if (!(new File(getWarehouseDir()).mkdirs())) {
        throw new RuntimeException("Could not create " + getWarehouseDir());
      }
      SessionState ss = SessionState.start(hiveConf);
      ss.applyAuthorizationPolicy();
      d = new Driver(new QueryState.Builder().withHiveConf(hiveConf).nonIsolated().build());
      d.setMaxRows(10000);
    } catch (Exception e) {
      System.err.println("Unable to setup the hive metatool test");
      System.err.println(StringUtils.stringifyException(e));
      throw new Exception(e);
    }
  }
  protected String getWarehouseDir() {
    return getTestDataDir() + "/warehouse";
  }

  private String getTestDataDir() {
    return TEST_DATA_DIR;
  }

  private void createDatabase() throws Exception {
    if (client.getAllDatabases().contains(DB_NAME)) {
      client.dropDatabase(DB_NAME);
    }

    Database db = new Database();
    db.setName(DB_NAME);
    client.createDatabase(db);
  }

  private void createTable() throws Exception {
    Table tbl = new Table();
    tbl.setDbName(DB_NAME);
    tbl.setTableName(TABLE_NAME);

    Map<String, String> parameters = new HashMap<>();
    parameters.put(AvroTableProperties.SCHEMA_URL.getPropName(), AVRO_URI);
    tbl.setParameters(parameters);

    List<FieldSchema> fields = new ArrayList<FieldSchema>(2);
    fields.add(new FieldSchema("name", serdeConstants.STRING_TYPE_NAME, ""));
    fields.add(new FieldSchema("income", serdeConstants.INT_TYPE_NAME, ""));

    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(fields);
    sd.setParameters(new HashMap<String, String>());
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    sd.getParameters().put(AvroTableProperties.SCHEMA_URL.getPropName(), AVRO_URI);
    tbl.setSd(sd);

    client.createTable(tbl);
  }

  @Test
  public void testListFSRoot() throws Exception {
    HiveMetaTool.main(new String[] {"-listFSRoot"});
    String out = os.toString();
    assertTrue(out + " doesn't contain " + client.getDatabase(DB_NAME).getLocationUri(),
        out.contains(client.getDatabase(DB_NAME).getLocationUri()));
  }

  @Test
  public void testExecuteJDOQL() throws Exception {
    HiveMetaTool.main(
        new String[] {"-executeJDOQL", "select locationUri from org.apache.hadoop.hive.metastore.model.MDatabase"});
    String out = os.toString();
    assertTrue(out + " doesn't contain " + client.getDatabase(DB_NAME).getLocationUri(),
        out.contains(client.getDatabase(DB_NAME).getLocationUri()));
  }

  @Test
  public void testUpdateFSRootLocation() throws Exception {
    checkAvroSchemaURLProps(AVRO_URI);

    HiveMetaTool.main(new String[] {"-updateLocation", NEW_LOCATION, LOCATION, "-tablePropKey", "avro.schema.url"});
    checkAvroSchemaURLProps(NEW_AVRO_URI);

    HiveMetaTool.main(new String[] {"-updateLocation", LOCATION, NEW_LOCATION, "-tablePropKey", "avro.schema.url"});
    checkAvroSchemaURLProps(AVRO_URI);
  }

  /*
   * Tests -listExtTblLocs option on various input combinations.
   */
  @Test
  public void testListExtTblLocs() throws Exception {
    String extTblLocation = getTestDataDir() + "/ext";
    String outLocation = getTestDataDir() + "/extTblOutput/";

    //Case 1: Multiple unpartitioned external tables, expected o/p: 1 location
    runStatementOnDriver("create external table ext1(a int) location '" + extTblLocation + "/t1'");
    runStatementOnDriver("create external table ext2(a int) location '" + extTblLocation + "/t2'" );
    runStatementOnDriver("create external table ext3(a int) location '" + extTblLocation + "/t3'" );
    JSONObject outJS = getListExtTblLocs("default", outLocation);
    Set<String> outLocationSet = outJS.keySet();
    Assert.assertTrue(outLocationSet.contains(getAbsolutePath(extTblLocation)));
    Assert.assertEquals(outLocationSet.size(), 1);
    runStatementOnDriver("drop table ext1");
    runStatementOnDriver("drop table ext2");
    runStatementOnDriver("drop table ext3");

    //Case 2: 1 table containing all partitions in table location, expected o/p : 1 location with "*"
    runStatementOnDriver("create external table ext(a int) partitioned by (b int) location '" + extTblLocation +"'");
    runStatementOnDriver("alter table ext add partition(b = 1)");
    runStatementOnDriver("alter table ext add partition(b = 2)");
    runStatementOnDriver("alter table ext add partition(b = 3)");
    outJS = getListExtTblLocs("default", outLocation);
    String expectedOutLoc = getAbsolutePath(extTblLocation);
    outLocationSet = outJS.keySet();
    Assert.assertTrue(outLocationSet.contains(expectedOutLoc));
    Assert.assertEquals(outLocationSet.size(), 1);
    JSONArray outArr = outJS.getJSONArray(expectedOutLoc);
    Assert.assertEquals(outArr.length(), 1);
    Assert.assertTrue(outArr.getString(0).equalsIgnoreCase("default.ext.*"));

    //Case 3 : Table contains no partitions, 3 partitions outside table.
    // inputs    ../ext/t1- table location, partitions locations:
    //           ../ext/b1
    //           ../ext/b2
    //           ../ext/b3
    // expected output : ../ext containing 4 elements
    runStatementOnDriver("drop table ext");
    runStatementOnDriver("create external table ext(a int) partitioned by (b int) " +
            "location '" + getTestDataDir() + "/ext/t1'");
    runStatementOnDriver("alter table ext add partition(b = 1) location '" + getTestDataDir() + "/ext/b1'" );
    runStatementOnDriver("alter table ext add partition(b = 2) location '" + getTestDataDir() + "/ext/b2'" );
    runStatementOnDriver("alter table ext add partition(b = 3) location '" + getTestDataDir() + "/ext/b3'" );
    outJS = getListExtTblLocs("default", outLocation);
    expectedOutLoc = getAbsolutePath(extTblLocation);
    outLocationSet = outJS.keySet();
    Assert.assertTrue(outLocationSet.contains(expectedOutLoc));
    Assert.assertEquals(outLocationSet.size(), 1);
    outArr = outJS.getJSONArray(expectedOutLoc);
    Assert.assertEquals(outArr.length(), 4);
    Assert.assertTrue(outArr.getString(0).equalsIgnoreCase("default.ext p(0/3)"));
    Assert.assertTrue(outArr.getString(1).equalsIgnoreCase("default.ext.b=1"));
    Assert.assertTrue(outArr.getString(2).equalsIgnoreCase("default.ext.b=2"));
    Assert.assertTrue(outArr.getString(3).equalsIgnoreCase("default.ext.b=3"));


    //Case 4 : Partitions at multiple depths
    // inputs   ../ext/b0 - contains tbl-loc(t1)
    //          ../ext/p=0 - contains 1 partition
    //          ../ext/b1/b2/b3 - contains 3 partitions of table (p1, p2, p3)
    // expected output : [../ext/b1/b2/b3 containing 3 elements, t1, p0]
    runStatementOnDriver("drop table ext");
    runStatementOnDriver("create external table ext(a int) partitioned by (p int) " +
            "location '" + getTestDataDir() + "/ext/b0'");
    runStatementOnDriver("alter table ext add partition(p = 0) location '" + getTestDataDir() + "/ext'" );
    runStatementOnDriver("alter table ext add partition(p = 1) location '" + getTestDataDir() + "/ext/b1/b2/b3'" );
    runStatementOnDriver("alter table ext add partition(p = 2) location '" + getTestDataDir() + "/ext/b1/b2/b3'" );
    runStatementOnDriver("alter table ext add partition(p = 3) location '" + getTestDataDir() + "/ext/b1/b2/b3'" );
    outJS = getListExtTblLocs("default", outLocation);
    String expectedOutLoc1 = getAbsolutePath(extTblLocation + "/b0");
    String expectedOutLoc2 = getAbsolutePath(extTblLocation + "/p=0");
    String expectedOutLoc3 = getAbsolutePath(extTblLocation + "/b1/b2/b3");
    outLocationSet = outJS.keySet();
    Assert.assertTrue(outLocationSet.contains(expectedOutLoc1));
    Assert.assertTrue(outLocationSet.contains(expectedOutLoc2));
    Assert.assertTrue(outLocationSet.contains(expectedOutLoc3));
    Assert.assertEquals(outLocationSet.size(), 3);
    outArr = outJS.getJSONArray(expectedOutLoc1);
    Assert.assertEquals(outArr.length(), 1);
    Assert.assertTrue(outArr.getString(0).equalsIgnoreCase("default.ext p(0/4)"));
    outArr = outJS.getJSONArray(expectedOutLoc2);
    Assert.assertTrue(outArr.getString(0).equalsIgnoreCase("default.ext.p=0"));
    outArr = outJS.getJSONArray(expectedOutLoc3);
    Assert.assertTrue(outArr.getString(0).equalsIgnoreCase("default.ext.p=1"));
    Assert.assertTrue(outArr.getString(1).equalsIgnoreCase("default.ext.p=2"));
    Assert.assertTrue(outArr.getString(2).equalsIgnoreCase("default.ext.p=3"));


    // Case 5 : Root location contains a lot of leaves
    // inputs   ../ext/b0 - contains tbllocation and 2 partitions (p0,p1) (3 elements total)
    //          ../ext/b1 - contains 2 partitions of table (p2, p3)
    //          ../ext    - contains 6 partitions (p4,..p9)
    // expected output : in this case, we take the root (ext) itself because it covers more than half the locations
    //                   exp o/p: [/ext containing 11 elements]
    runStatementOnDriver("drop table ext");
    runStatementOnDriver("create external table ext(a int) partitioned by (p int) " +
            "location '" + getTestDataDir() + "/ext/b0'");
    runStatementOnDriver("alter table ext add partition(p = 0) location '" + getTestDataDir() + "/ext/b0'" );
    runStatementOnDriver("alter table ext add partition(p = 1) location '" + getTestDataDir() + "/ext/b0'" );
    runStatementOnDriver("alter table ext add partition(p = 2) location '" + getTestDataDir() + "/ext/b1'" );
    runStatementOnDriver("alter table ext add partition(p = 3) location '" + getTestDataDir() + "/ext/b1'" );
    runStatementOnDriver("alter table ext add partition(p = 4) location '" + getTestDataDir() + "/ext'" );
    runStatementOnDriver("alter table ext add partition(p = 5) location '" + getTestDataDir() + "/ext'" );
    runStatementOnDriver("alter table ext add partition(p = 6) location '" + getTestDataDir() + "/ext'" );
    runStatementOnDriver("alter table ext add partition(p = 7) location '" + getTestDataDir() + "/ext'" );
    runStatementOnDriver("alter table ext add partition(p = 8) location '" + getTestDataDir() + "/ext'" );
    runStatementOnDriver("alter table ext add partition(p = 9) location '" + getTestDataDir() + "/ext'" );
    outJS = getListExtTblLocs("default", outLocation);
    expectedOutLoc1 = getAbsolutePath(extTblLocation);
    outLocationSet = outJS.keySet();
    Assert.assertTrue(outLocationSet.contains(expectedOutLoc1));
    Assert.assertEquals(outLocationSet.size(), 1);
    outArr = outJS.getJSONArray(expectedOutLoc1);
    Assert.assertEquals(outArr.length(), 11);
    Assert.assertTrue(outArr.getString(0).equalsIgnoreCase("default.ext p(0/10)"));
    Assert.assertTrue(outArr.getString(1).equalsIgnoreCase("default.ext.p=0"));
    Assert.assertTrue(outArr.getString(2).equalsIgnoreCase("default.ext.p=1"));
    Assert.assertTrue(outArr.getString(3).equalsIgnoreCase("default.ext.p=2"));
    Assert.assertTrue(outArr.getString(4).equalsIgnoreCase("default.ext.p=3"));
    Assert.assertTrue(outArr.getString(5).equalsIgnoreCase("default.ext.p=4"));
    Assert.assertTrue(outArr.getString(6).equalsIgnoreCase("default.ext.p=5"));
    Assert.assertTrue(outArr.getString(7).equalsIgnoreCase("default.ext.p=6"));
    Assert.assertTrue(outArr.getString(8).equalsIgnoreCase("default.ext.p=7"));
    Assert.assertTrue(outArr.getString(9).equalsIgnoreCase("default.ext.p=8"));
    Assert.assertTrue(outArr.getString(10).equalsIgnoreCase("default.ext.p=9"));


    // Case 6 : Check count of partitions contained in tbl-loc
    // inputs   ../ext/b0 - table1 location containing 3/5 partitions
    //          ../ext/b1 - table2 location containining 2/4 partitions
    //          ../ext/b2/b3/b4 - 2 partitions of table1, 1 partition of table2, table loc of table3
    //          ../ext    - partitions of table3 which is less in number than all above combined
    // expected output : [../ext/b0, ../ext/b1, ../ext/b2,b3,b4, table3 partitions individually]
    runStatementOnDriver("drop table ext");
    runStatementOnDriver("create external table ext(a int) partitioned by (p int) " +
            "location '" + getTestDataDir() + "/ext/b0'");
    runStatementOnDriver("create external table ext2(a int) partitioned by (p int, p1 int) " +
            "location '" + getTestDataDir() + "/ext/b1'");
    runStatementOnDriver("create external table ext3(a int) partitioned by (p int) "
            + "location '" + getTestDataDir() + "/ext/b2/b3/b4'");
    runStatementOnDriver("alter table ext add partition(p = 0)" );
    runStatementOnDriver("alter table ext add partition(p = 1)" );
    runStatementOnDriver("alter table ext add partition(p = 2)" );
    runStatementOnDriver("alter table ext2 add partition(p = 0, p1 = 0)");
    runStatementOnDriver("alter table ext2 add partition(p = 0, p1 = 1)");
    runStatementOnDriver("alter table ext3 add partition(p = 0) location '"
            + getTestDataDir() + "/ext'" );
    runStatementOnDriver("alter table ext3 add partition(p = 1) location '"
            + getTestDataDir() + "/ext'" );
    runStatementOnDriver("alter table ext3 add partition(p = 2) location '"
            + getTestDataDir() + "/ext'" );
    runStatementOnDriver("alter table ext add partition(p = 3) location '"
            + getTestDataDir() + "/ext/b2/b3/b4'" );
    runStatementOnDriver("alter table ext add partition(p = 4) location '"
            + getTestDataDir() + "/ext/b2/b3/b4'" );
    runStatementOnDriver("alter table ext2 add partition(p = 0, p1 = 2) location '"
            + getTestDataDir() + "/ext/b2/b3/b4'");
    runStatementOnDriver("alter table ext2 add partition(p = 0, p1 = 3) location '"
            + getTestDataDir() + "/ext/b2/b3/b4'");


    outJS = getListExtTblLocs("default", outLocation);
    expectedOutLoc1 = getAbsolutePath(extTblLocation + "/b0");
    expectedOutLoc2 = getAbsolutePath(extTblLocation + "/b1");
    expectedOutLoc3 = getAbsolutePath(extTblLocation + "/b2/b3/b4");
    String expectedOutLoc4 = getAbsolutePath(extTblLocation + "/p=0");
    String expectedOutLoc5 = getAbsolutePath(extTblLocation + "/p=1");
    String expectedOutLoc6 = getAbsolutePath(extTblLocation + "/p=2");

    outLocationSet = outJS.keySet();
    Assert.assertEquals(outLocationSet.size(), 6);
    Assert.assertTrue(outLocationSet.contains(expectedOutLoc1));
    outArr = outJS.getJSONArray(expectedOutLoc1);
    Assert.assertEquals(outArr.length(), 1);
    Assert.assertTrue(outArr.getString(0).equalsIgnoreCase("default.ext p(3/5)"));
    Assert.assertTrue(outLocationSet.contains(expectedOutLoc2));
    outArr = outJS.getJSONArray(expectedOutLoc2);
    Assert.assertEquals(outArr.length(), 1);
    Assert.assertTrue(outArr.getString(0).equalsIgnoreCase("default.ext2 p(2/4)"));
    Assert.assertTrue(outLocationSet.contains(expectedOutLoc3));
    outArr = outJS.getJSONArray(expectedOutLoc3);
    Assert.assertEquals(outArr.length(), 5);
    Assert.assertTrue(outArr.getString(0).equalsIgnoreCase("default.ext.p=3"));
    Assert.assertTrue(outArr.getString(1).equalsIgnoreCase("default.ext.p=4"));
    Assert.assertTrue(outArr.getString(2).equalsIgnoreCase("default.ext2.p=0/p1=2"));
    Assert.assertTrue(outArr.getString(3).equalsIgnoreCase("default.ext2.p=0/p1=3"));
    Assert.assertTrue(outArr.getString(4).equalsIgnoreCase("default.ext3 p(0/3)"));
    Assert.assertTrue(outLocationSet.contains(expectedOutLoc4));
    outArr = outJS.getJSONArray(expectedOutLoc4);
    Assert.assertEquals(outArr.length(), 1);
    Assert.assertTrue(outArr.getString(0).equalsIgnoreCase("default.ext3.p=0"));
    Assert.assertTrue(outLocationSet.contains(expectedOutLoc5));
    outArr = outJS.getJSONArray(expectedOutLoc5);
    Assert.assertEquals(outArr.length(), 1);
    Assert.assertTrue(outArr.getString(0).equalsIgnoreCase("default.ext3.p=1"));
    Assert.assertTrue(outLocationSet.contains(expectedOutLoc6));
    outArr = outJS.getJSONArray(expectedOutLoc6);
    Assert.assertEquals(outArr.length(), 1);
    Assert.assertTrue(outArr.getString(0).equalsIgnoreCase("default.ext3.p=2"));
  }

  private String getAbsolutePath(String extTblLocation) {
    return "file:" + extTblLocation;
  }

  private JSONObject getListExtTblLocs(String dbName, String outLocation) throws IOException {
    File f = new File(outLocation);
    if (f.exists()) {
      FileUtil.fullyDelete(f);
    }
    if (!(new File(outLocation).mkdirs())) {
      throw new RuntimeException("Could not create " + outLocation);
    }
    HiveMetaTool.main(new String[] {"-listExtTblLocs", dbName, outLocation});
    for (File outFile : f.listFiles()) {
      String contents = new String(Files.readAllBytes(Paths.get(outFile.getAbsolutePath())));
      return new JSONObject(contents);
    }
    return null;
  }

  private void checkAvroSchemaURLProps(String expectedUri) throws TException {
    Table table = client.getTable(DB_NAME, TABLE_NAME);
    assertEquals(expectedUri, table.getParameters().get(AvroTableProperties.SCHEMA_URL.getPropName()));
    assertEquals(expectedUri, table.getSd().getParameters().get(AvroTableProperties.SCHEMA_URL.getPropName()));
  }

  protected List<String> runStatementOnDriver(String stmt) throws Exception {
    try {
      d.run(stmt);
    } catch (CommandProcessorException e) {
      throw new RuntimeException(stmt + " failed: " + e);
    }
    List<String> rs = new ArrayList<>();
    d.getResults(rs);
    return rs;
  }

  @After
  public void tearDown() throws Exception {
    try {
      client.dropTable(DB_NAME, TABLE_NAME);
      client.dropDatabase(DB_NAME);
      try {
        if (d != null) {
          d.close();
          d.destroy();
          d = null;
        }
      } finally {
        TxnDbUtil.cleanDb(hiveConf);
        FileUtils.deleteDirectory(new File(getTestDataDir()));
      }

      client.close();
    } catch (Throwable e) {
      System.err.println("Unable to close metastore");
      System.err.println(StringUtils.stringifyException(e));
      throw new Exception(e);
    }
  }
}
