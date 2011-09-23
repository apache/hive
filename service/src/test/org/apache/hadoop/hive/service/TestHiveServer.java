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
package org.apache.hadoop.hive.service;

import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ServerUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.dynamic_type.DynamicSerDe;
import org.apache.hadoop.io.BytesWritable;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * TestHiveServer.
 *
 */
public class TestHiveServer extends TestCase {

  private HiveInterface client;
  private static final String host = "localhost";
  private static final int port = 10000;
  private final Path dataFilePath;

  private static String tableName = "testhivedrivertable";
  private final HiveConf conf;
  private boolean standAloneServer = false;
  private TTransport transport;
  private final String invalidPath;

  public TestHiveServer(String name) {
    super(name);
    conf = new HiveConf(TestHiveServer.class);
    String dataFileDir = conf.get("test.data.files").replace('\\', '/')
        .replace("c:", "");
    invalidPath = dataFileDir+"/invalidpath/";
    dataFilePath = new Path(dataFileDir, "kv1.txt");
    // See data/conf/hive-site.xml
    String paramStr = System.getProperty("test.service.standalone.server");
    if (paramStr != null && paramStr.equals("true")) {
      standAloneServer = true;
    }
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();

    if (standAloneServer) {
      try {
        transport = new TSocket(host, port);
        TProtocol protocol = new TBinaryProtocol(transport);
        client = new HiveClient(protocol);
        transport.open();
      } catch (Throwable e) {
        e.printStackTrace();
      }
    } else {
      client = new HiveServer.HiveServerHandler();
    }
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    if (standAloneServer) {
      try {
        client.clean();
      } catch (Exception e) {
        e.printStackTrace();
      }
      transport.close();
    }
  }

  public void testExecute() throws Exception {
    try {
      client.execute("set hive.support.concurrency = false");
      client.execute("drop table " + tableName);
    } catch (Exception ex) {
    }

    try {
      client.execute("create table " + tableName + " (num int)");
      client.execute("load data local inpath '" + dataFilePath.toString()
          + "' into table " + tableName);
      client.execute("select count(1) as cnt from " + tableName);
      String row = client.fetchOne();
      assertEquals(row, "500");

      Schema hiveSchema = client.getSchema();
      List<FieldSchema> listFields = hiveSchema.getFieldSchemas();
      assertEquals(listFields.size(), 1);
      assertEquals(listFields.get(0).getName(), "cnt");
      assertEquals(listFields.get(0).getType(), "bigint");

      Schema thriftSchema = client.getThriftSchema();
      List<FieldSchema> listThriftFields = thriftSchema.getFieldSchemas();
      assertEquals(listThriftFields.size(), 1);
      assertEquals(listThriftFields.get(0).getName(), "cnt");
      assertEquals(listThriftFields.get(0).getType(), "i64");

      client.execute("drop table " + tableName);
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  public void notestExecute() throws Exception {
    try {
      client.execute("set hive.support.concurrency = false");
      client.execute("drop table " + tableName);
    } catch (Exception ex) {
    }

    client.execute("create table " + tableName + " (num int)");
    client.execute("load data local inpath '" + dataFilePath.toString()
        + "' into table " + tableName);
    client.execute("select count(1) from " + tableName);
    String row = client.fetchOne();
    assertEquals(row, "500");
    client.execute("drop table " + tableName);
    transport.close();
  }

  public void testNonHiveCommand() throws Exception {
    try {
      client.execute("set hive.support.concurrency = false");
      client.execute("drop table " + tableName);
    } catch (Exception ex) {
    }

    client.execute("create table " + tableName + " (num int)");
    client.execute("load data local inpath '" + dataFilePath.toString()
        + "' into table " + tableName);

    // Command not part of HiveQL - verify no results
    client.execute("SET hive.mapred.mode = nonstrict");

    Schema schema = client.getSchema();
    assertEquals(schema.getFieldSchemasSize(), 0);
    assertEquals(schema.getPropertiesSize(), 0);

    Schema thriftschema = client.getThriftSchema();
    assertEquals(thriftschema.getFieldSchemasSize(), 0);
    assertEquals(thriftschema.getPropertiesSize(), 0);

    try {
      String ret = client.fetchOne();
      assertTrue(false);
    } catch (HiveServerException e) {
      assertEquals(e.getErrorCode(), 0);
    }
    assertEquals(client.fetchN(10).size(), 0);
    assertEquals(client.fetchAll().size(), 0);

    // Execute Hive query and fetch
    client.execute("select * from " + tableName + " limit 10");
    client.fetchOne();

    // Re-execute command not part of HiveQL - verify still no results
    client.execute("SET hive.mapred.mode = nonstrict");

    schema = client.getSchema();
    assertEquals(schema.getFieldSchemasSize(), 0);
    assertEquals(schema.getPropertiesSize(), 0);

    thriftschema = client.getThriftSchema();
    assertEquals(thriftschema.getFieldSchemasSize(), 0);
    assertEquals(thriftschema.getPropertiesSize(), 0);

    try {
      String ret = client.fetchOne();
      assertTrue(false);
    } catch (HiveServerException e) {
      assertEquals(e.getErrorCode(), 0);
    }
    assertEquals(client.fetchN(10).size(), 0);
    assertEquals(client.fetchAll().size(), 0);

    // Cleanup
    client.execute("drop table " + tableName);
  }

  /**
   * Test metastore call.
   */
  public void testMetastore() throws Exception {
    try {
      client.execute("set hive.support.concurrency = false");
      client.execute("drop table " + tableName);
    } catch (Exception ex) {
    }

    client.execute("create table " + tableName + " (num int)");
    List<String> tabs = client.get_tables("default", tableName);
    assertEquals(tabs.get(0), tableName);
    client.execute("drop table " + tableName);
  }

  /**
   * Test cluster status retrieval.
   */
  public void testGetClusterStatus() throws Exception {
    HiveClusterStatus clusterStatus = client.getClusterStatus();
    assertNotNull(clusterStatus);
    assertTrue(clusterStatus.getTaskTrackers() >= 0);
    assertTrue(clusterStatus.getMapTasks() >= 0);
    assertTrue(clusterStatus.getReduceTasks() >= 0);
    assertTrue(clusterStatus.getMaxMapTasks() >= 0);
    assertTrue(clusterStatus.getMaxReduceTasks() >= 0);
    assertTrue(clusterStatus.getState() == JobTrackerState.INITIALIZING
        || clusterStatus.getState() == JobTrackerState.RUNNING);
  }

  /**
   *
   */
  public void testFetch() throws Exception {
    // create and populate a table with 500 rows.
    try {
      client.execute("set hive.support.concurrency = false");
      client.execute("drop table " + tableName);
    } catch (Exception ex) {
    }
    client.execute("create table " + tableName + " (key int, value string)");
    client.execute("load data local inpath '" + dataFilePath.toString()
        + "' into table " + tableName);

    try {
      // fetchAll test
      client.execute("select key, value from " + tableName);
      assertEquals(client.fetchAll().size(), 500);
      assertEquals(client.fetchAll().size(), 0);

      // fetchOne test
      client.execute("select key, value from " + tableName);
      for (int i = 0; i < 500; i++) {
        try {
          String str = client.fetchOne();
        } catch (HiveServerException e) {
          assertTrue(false);
        }
      }
      try {
        client.fetchOne();
      } catch (HiveServerException e) {
        assertEquals(e.getErrorCode(), 0);
      }

      // fetchN test
      client.execute("select key, value from " + tableName);
      assertEquals(client.fetchN(499).size(), 499);
      assertEquals(client.fetchN(499).size(), 1);
      assertEquals(client.fetchN(499).size(), 0);
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }

  public void testDynamicSerde() throws Exception {
    try {
      client.execute("set hive.support.concurrency = false");
      client.execute("drop table " + tableName);
    } catch (Exception ex) {
    }

    client.execute("create table " + tableName + " (key int, value string)");
    client.execute("load data local inpath '" + dataFilePath.toString()
        + "' into table " + tableName);
    // client.execute("select key, count(1) from " + tableName +
    // " where key > 10 group by key");
    String sql = "select key, value from " + tableName + " where key > 10";
    client.execute(sql);

    // Instantiate DynamicSerDe
    DynamicSerDe ds = new DynamicSerDe();
    Properties dsp = new Properties();
    dsp.setProperty(Constants.SERIALIZATION_FORMAT,
        org.apache.hadoop.hive.serde2.thrift.TCTLSeparatedProtocol.class
            .getName());
    dsp.setProperty(
        org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_NAME,
        "result");
    String serDDL = new String("struct result { ");
    List<FieldSchema> schema = client.getThriftSchema().getFieldSchemas();
    for (int pos = 0; pos < schema.size(); pos++) {
      if (pos != 0) {
        serDDL = serDDL.concat(",");
      }
      serDDL = serDDL.concat(schema.get(pos).getType());
      serDDL = serDDL.concat(" ");
      serDDL = serDDL.concat(schema.get(pos).getName());
    }
    serDDL = serDDL.concat("}");

    dsp.setProperty(Constants.SERIALIZATION_DDL, serDDL);
    dsp.setProperty(Constants.SERIALIZATION_LIB, ds.getClass().toString());
    dsp.setProperty(Constants.FIELD_DELIM, "9");
    ds.initialize(new Configuration(), dsp);

    String row = client.fetchOne();
    Object o = ds.deserialize(new BytesWritable(row.getBytes()));

    assertEquals(o.getClass().toString(), "class java.util.ArrayList");
    List<?> lst = (List<?>) o;
    assertEquals(lst.get(0), 238);

    // TODO: serde doesn't like underscore -- struct result { string _c0}
    sql = "select count(1) as c from " + tableName;
    client.execute(sql);
    row = client.fetchOne();

    serDDL = new String("struct result { ");
    schema = client.getThriftSchema().getFieldSchemas();
    for (int pos = 0; pos < schema.size(); pos++) {
      if (pos != 0) {
        serDDL = serDDL.concat(",");
      }
      serDDL = serDDL.concat(schema.get(pos).getType());
      serDDL = serDDL.concat(" ");
      serDDL = serDDL.concat(schema.get(pos).getName());
    }
    serDDL = serDDL.concat("}");

    dsp.setProperty(Constants.SERIALIZATION_DDL, serDDL);
    // Need a new DynamicSerDe instance - re-initialization is not supported.
    ds = new DynamicSerDe();
    ds.initialize(new Configuration(), dsp);
    o = ds.deserialize(new BytesWritable(row.getBytes()));
  }

  public void testAddJarShouldFailIfJarNotExist() throws Exception {
    boolean queryExecutionFailed = false;
    try {
      client.execute("add jar " + invalidPath + "sample.jar");
    } catch (Exception e) {
      queryExecutionFailed = true;
    }
    if (!queryExecutionFailed) {
      fail("It should throw exception since jar does not exist");
    }
  }

  public void testAddFileShouldFailIfFileNotExist() throws Exception {
    boolean queryExecutionFailed = false;
    try {
      client.execute("add file " + invalidPath + "sample.txt");
    } catch (Exception e) {
      queryExecutionFailed = true;
    }
    if (!queryExecutionFailed) {
      fail("It should throw exception since file does not exist");
    }
  }

  public void testAddArchiveShouldFailIfFileNotExist() throws Exception {
    boolean queryExecutionFailed = false;
    try {
      client.execute("add archive " + invalidPath + "sample.zip");
    } catch (Exception e) {
      queryExecutionFailed = true;
    }
    if (!queryExecutionFailed) {
      fail("It should trow exception since archive does not exist");
    }
  }

  public void testScratchDirShouldNotClearWhileStartup() throws Exception {
    FileSystem fs = FileSystem.get(conf);
    Path scratchDirPath = new Path(HiveConf.getVar(conf,
        HiveConf.ConfVars.SCRATCHDIR));
    boolean fileExists = fs.exists(scratchDirPath);
    if (!fileExists) {
      fileExists = fs.mkdirs(scratchDirPath);
    }
    ServerUtils.cleanUpScratchDir(conf);
    assertTrue("Scratch dir is not available after startup", fs.exists(scratchDirPath));
  }

  public void testScratchDirShouldClearWhileStartup() throws Exception {
    FileSystem fs = FileSystem.get(conf);
    Path scratchDirPath = new Path(HiveConf.getVar(conf,
        HiveConf.ConfVars.SCRATCHDIR));
    boolean fileExists = fs.exists(scratchDirPath);
    if (!fileExists) {
      fileExists = fs.mkdirs(scratchDirPath);
    }
    try {
      conf.setBoolVar(HiveConf.ConfVars.HIVE_START_CLEANUP_SCRATCHDIR, true);
      ServerUtils.cleanUpScratchDir(conf);
    } finally {
      conf.setBoolVar(HiveConf.ConfVars.HIVE_START_CLEANUP_SCRATCHDIR, false);
    }
    assertFalse("Scratch dir is available after startup", fs.exists(scratchDirPath));
  }

}
