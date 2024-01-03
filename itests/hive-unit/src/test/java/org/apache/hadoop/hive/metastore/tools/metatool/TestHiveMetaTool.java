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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
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
      hiveConf.set(HiveConf.ConfVars.PRE_EXEC_HOOKS.varname, "");
      hiveConf.set(HiveConf.ConfVars.POST_EXEC_HOOKS.varname, "");
      hiveConf.set(HiveConf.ConfVars.METASTORE_WAREHOUSE.varname, getWarehouseDir());
      hiveConf.setVar(HiveConf.ConfVars.HIVE_INPUT_FORMAT, HiveInputFormat.class.getName());
      hiveConf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
                      "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
      hiveConf.setBoolVar(HiveConf.ConfVars.MERGE_CARDINALITY_VIOLATION_CHECK, true);
      HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.SPLIT_UPDATE, true);
      hiveConf.setBoolVar(HiveConf.ConfVars.HIVESTATSCOLAUTOGATHER, false);
      hiveConf.setBoolean("mapred.input.dir.recursive", true);
      TestTxnDbUtil.setConfValues(hiveConf);
      txnHandler = TxnUtils.getTxnStore(hiveConf);
      TestTxnDbUtil.prepDb(hiveConf);
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
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.WAREHOUSE_EXTERNAL, getWarehouseDir());
    MetaToolTaskListExtTblLocs.msConf = conf;

    // Case 1 : Check default locations
    // Inputs : db1, db2 in default locations, db3 in custom location
    // Expected outputs: default locations for db1, db2 and custom location for db3 after aggregation
    runStatementOnDriver("create database db1");
    runStatementOnDriver("create database db2");
    runStatementOnDriver("create database db3");
    runStatementOnDriver("create external table db1.ext(a int) partitioned by (p int)");
    runStatementOnDriver("create external table db2.ext(a int) partitioned by (p int)");
    runStatementOnDriver("create external table db3.ext(a int) partitioned by (p int) " +
            "location '" + getTestDataDir() + "/ext/tblLoc'");
    runStatementOnDriver("alter table db3.ext add partition(p = 0) location '" + getTestDataDir() + "/part'" );
    runStatementOnDriver("alter table db3.ext add partition(p = 1) location '" + getTestDataDir() + "/part'" );
    JSONObject outJS = getListExtTblLocs("db*", outLocation);
    //confirm default locations
    Set<String> outLocationSet = outJS.keySet();
    String expectedOutLoc1 = getAbsolutePath(getWarehouseDir() + "/db1.db");
    Assert.assertTrue(outLocationSet.contains(expectedOutLoc1));
    Assert.assertEquals(outLocationSet.size(), 4);
    JSONArray outArr = outJS.getJSONArray(expectedOutLoc1);
    Assert.assertEquals(outArr.length(), 1);
    Assert.assertTrue(outArr.getString(0).equals("db1.ext"));
    String expectedOutLoc2 = getAbsolutePath(getWarehouseDir() + "/db2.db");
    Assert.assertTrue(outLocationSet.contains(expectedOutLoc2));
    outArr = outJS.getJSONArray(expectedOutLoc2);
    Assert.assertEquals(outArr.length(), 1);
    Assert.assertTrue(outArr.getString(0).equals("db2.ext"));
    String expectedOutLoc3 = getAbsolutePath(getTestDataDir() + "/part");
    Assert.assertTrue(outLocationSet.contains(expectedOutLoc3));
    outArr = outJS.getJSONArray(expectedOutLoc3);
    Assert.assertEquals(outArr.length(), 2);
    Assert.assertTrue(outArr.getString(0).equals("db3.ext.p=0"));
    Assert.assertTrue(outArr.getString(1).equals("db3.ext.p=1"));
    String expectedOutLoc4 = getAbsolutePath(getTestDataDir() + "/ext/tblLoc");
    Assert.assertTrue(outLocationSet.contains(expectedOutLoc4));
    outArr = outJS.getJSONArray(expectedOutLoc4);
    Assert.assertEquals(outArr.length(), 1);
    Assert.assertTrue(outArr.getString(0).equals("db3.ext p(0/2)"));


    // Case 2 : Check with special chars in partition-names : including quotes, timestamp formats, spaces, backslash etc.
    // Also checks count of partitions in tbl-location.
    // inputs   (default database)
    //          ../ext/t1 - table1 location containing 3/5 partitions
    //          ../ext/t2 - table2 location containining 2/4 partitions
    //          ../ext/dir1/dir2/dir3 - 2 partitions of table1, 1 partition of table2, table loc of table3 with 0 partitions.
    //          ../ext    - partitions of table3
    // expected output : [../ext/t1, ../ext/t2, ../ext/dir1/dir2/dir3/t1_parts (2 partitions), ../ext/dir1/dir2/dir3/t2_parts(2 partitions),
    //                     .../ext/dir1/dir2/dir3/t3 (0 parittions), ../ext/t3_parts (1 partition) ]
    //                   Doesn't contain default database location as there are no entities in default location in this case,
    //                   all data is under some custom location (../ext)
    runStatementOnDriver("drop table ext");
    runStatementOnDriver("create external table ext(a int) partitioned by (p varchar(3)) " +
            "location '" + getTestDataDir() + "/ext/t1'");
    runStatementOnDriver("create external table ext2(a int) partitioned by (flt string, dbl string) " +
            "location '" + getTestDataDir() + "/ext/t2'");
    runStatementOnDriver("create external table ext3(a int) partitioned by (dt string, timeSt string) "
            + "location '" + getTestDataDir() + "/ext/dir1/dir2/dir3/t3'");
    runStatementOnDriver("alter table ext add partition(p = 'A')");
    runStatementOnDriver("alter table ext add partition(p = 'B')");
    runStatementOnDriver("alter table ext add partition(p = 'UK')" );
    runStatementOnDriver("alter table ext2 add partition(flt = '0.0', dbl = '0')");
    runStatementOnDriver("alter table ext2 add partition(flt = '0.1', dbl = '1.1')");
    runStatementOnDriver("alter table ext3 add partition(dt = '2020-12-01', timeSt = '23:23:23') location '"
            + getTestDataDir() + "/ext/t3_parts'" );
    runStatementOnDriver("alter table ext3 add partition(dt = '2020-12-02', timeSt = '22:22:22') location '"
            + getTestDataDir() + "/ext/t3_parts'" );
    runStatementOnDriver("alter table ext3 add partition(dt = '2020-12-03', timeSt = '21:21:21.1234') location '"
            + getTestDataDir() + "/ext/t3_parts'" );
    runStatementOnDriver("alter table ext add partition(p = \'A\\\\\') location '"
            + getTestDataDir() + "/ext/dir1/dir2/dir3/t1_parts'" );
    runStatementOnDriver("alter table ext add partition(p = \' A\"\') location '"
            + getTestDataDir() + "/ext/dir1/dir2/dir3/t1_parts'" );
    runStatementOnDriver("alter table ext2 add partition(flt = '0.1', dbl='3.22') location '"
            + getTestDataDir() + "/ext/dir1/dir2/dir3/t2_parts'");
    runStatementOnDriver("alter table ext2 add partition(flt = '0.22', dbl = '2.22') location '"
            + getTestDataDir() + "/ext/dir1/dir2/dir3/t2_parts'");


    outJS = getListExtTblLocs("default", outLocation);
    expectedOutLoc1 = getAbsolutePath(extTblLocation + "/t1");
    expectedOutLoc2 = getAbsolutePath(extTblLocation + "/t2");
    expectedOutLoc3 = getAbsolutePath(extTblLocation + "/dir1/dir2/dir3/t1_parts");
    expectedOutLoc4 = getAbsolutePath(extTblLocation + "/dir1/dir2/dir3/t2_parts");
    String expectedOutLoc5 = getAbsolutePath(extTblLocation + "/dir1/dir2/dir3/t3");
    String expectedOutLoc6 = getAbsolutePath(extTblLocation + "/t3_parts");

    outLocationSet = outJS.keySet();
    Assert.assertEquals(outLocationSet.size(), 6);
    Assert.assertTrue(outLocationSet.contains(expectedOutLoc1));
    outArr = outJS.getJSONArray(expectedOutLoc1); //t1
    Assert.assertEquals(outArr.length(), 1);
    Assert.assertTrue(outArr.getString(0).equals("default.ext p(3/5)"));
    Assert.assertTrue(outLocationSet.contains(expectedOutLoc2));
    outArr = outJS.getJSONArray(expectedOutLoc2); //t2
    Assert.assertEquals(outArr.length(), 1);
    Assert.assertTrue(outArr.getString(0).equals("default.ext2 p(2/4)"));
    Assert.assertTrue(outLocationSet.contains(expectedOutLoc3)); //t1_parts
    outArr = outJS.getJSONArray(expectedOutLoc3);
    Assert.assertEquals(outArr.length(), 2);
    Assert.assertTrue(outArr.getString(0).equals("default.ext.p= A%22"));  //spaces, quotes
    Assert.assertTrue(outArr.getString(1).equals("default.ext.p=A%5C")); //backslash
    Assert.assertTrue(outLocationSet.contains(expectedOutLoc4)); //t2_parts
    outArr = outJS.getJSONArray(expectedOutLoc4);
    Assert.assertEquals(outArr.length(), 2);
    Assert.assertTrue(outArr.getString(0).equals("default.ext2.flt=0.1/dbl=3.22")); //periods, slash
    Assert.assertTrue(outArr.getString(1).equals("default.ext2.flt=0.22/dbl=2.22"));
    Assert.assertTrue(outLocationSet.contains(expectedOutLoc5)); //t3
    outArr = outJS.getJSONArray(expectedOutLoc5);
    Assert.assertEquals(outArr.length(), 1);
    Assert.assertTrue(outArr.getString(0).equals("default.ext3 p(0/3)"));
    Assert.assertTrue(outLocationSet.contains(expectedOutLoc6)); //t3_parts
    outArr = outJS.getJSONArray(expectedOutLoc6);
    Assert.assertEquals(outArr.length(), 3);
    Assert.assertTrue(outArr.getString(0).equals("default.ext3.dt=2020-12-01/timest=23%3A23%3A23")); //date, timestamp formats
    Assert.assertTrue(outArr.getString(1).equals("default.ext3.dt=2020-12-02/timest=22%3A22%3A22"));
    Assert.assertTrue(outArr.getString(2).equals("default.ext3.dt=2020-12-03/timest=21%3A21%3A21.1234"));
  }

  /*
   * Tests -diffExtTblLocs option on various input combinations.
   */
  @Test
  public void testDiffExtTblLocs() throws Exception {
    String extTblLocation = getTestDataDir() + "/ext";
    String outLocation = getTestDataDir() + "/extTblOutput";
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.WAREHOUSE_EXTERNAL, getWarehouseDir());
    MetaToolTaskListExtTblLocs.msConf = conf;

    //create first file using -listExtTblLocs
    runStatementOnDriver("create database diffDb");
    runStatementOnDriver("create external table diffDb.ext1(a int) partitioned by (p int)");
    runStatementOnDriver("create external table diffDb.ext2(a int) partitioned by (p int)");
    runStatementOnDriver("create external table diffDb.ext3(a int) partitioned by (p int) " +
            "location '" + getTestDataDir() + "/ext/tblLoc'");
    runStatementOnDriver("alter table diffDb.ext1 add partition(p = 0) location '" + getTestDataDir() + "/part'" );
    runStatementOnDriver("alter table diffDb.ext1 add partition(p = 1) location '" + getTestDataDir() + "/part'" );
    String outLocation1 = outLocation + "1";
    getListExtTblLocs("diffDb", outLocation1);

    //create second file using -listExtTblLocs after dropping a table, dropping a partition and adding a different partition
    runStatementOnDriver("drop table diffDb.ext2");
    runStatementOnDriver("alter table diffDb.ext1 drop partition(p = 0)" );
    runStatementOnDriver("alter table diffDb.ext1 add partition(p = 3) location '" + getTestDataDir() + "/part'" );
    String outLocation2 = outLocation + "2";
    getListExtTblLocs("diffDb", outLocation2);

    //run diff on the above two files
    JSONObject outJS = getDiffExtTblLocs(outLocation1, outLocation2, outLocation);
    Set<String> outLocationSet = outJS.keySet();
    String defaultDbLoc = getAbsolutePath(getWarehouseDir() + "/diffdb.db");
    Assert.assertEquals(outLocationSet.size(), 2);
    Assert.assertTrue(outLocationSet.contains(defaultDbLoc));
    JSONArray outArr = outJS.getJSONArray(defaultDbLoc);
    Assert.assertEquals(outArr.length(), 1);
    Assert.assertTrue(outArr.getString(0).equals("- diffdb.ext2")); // dropped ext2 from default location
    String partLoc = getAbsolutePath(getTestDataDir() + "/part");
    Assert.assertTrue(outLocationSet.contains(partLoc));
    outArr = outJS.getJSONArray(partLoc);
    Assert.assertEquals(outArr.length(), 2); //two entries - 1 for added partition and 1 for dropped partition
    Assert.assertTrue(outArr.getString(0).equals("+ diffdb.ext1.p=3"));
    Assert.assertTrue(outArr.getString(1).equals("- diffdb.ext1.p=0"));
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

  private JSONObject getDiffExtTblLocs(String fileLoc1, String fileLoc2, String outLocation) throws IOException {
    File f = new File(outLocation);
    if (f.exists()) {
      FileUtil.fullyDelete(f);
    }
    if (!(new File(outLocation).mkdirs())) {
      throw new RuntimeException("Could not create " + outLocation);
    }
    File f1 = new File(fileLoc1);
    File f2 = new File(fileLoc2);
    for (File outFile1 : f1.listFiles()) {
      for (File outFile2 : f2.listFiles()) {
        HiveMetaTool.main(new String[] {"-diffExtTblLocs", outFile1.getAbsolutePath(), outFile2.getAbsolutePath(), outLocation});
        for(File outFile : f.listFiles()) {
          String contents = new String(Files.readAllBytes(Paths.get(outFile.getAbsolutePath())));
          return new JSONObject(contents);
        }
      }
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
        TestTxnDbUtil.cleanDb(hiveConf);
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
