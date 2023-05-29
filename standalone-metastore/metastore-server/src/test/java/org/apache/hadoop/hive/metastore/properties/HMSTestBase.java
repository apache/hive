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
package org.apache.hadoop.hive.metastore.properties;

import org.apache.commons.io.IOUtils;
import org.apache.commons.jexl3.JxltEngine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HMSHandler;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.TestObjectStore;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import static org.apache.hadoop.hive.metastore.properties.HMSPropertyManager.MaintenanceOpStatus;
import static org.apache.hadoop.hive.metastore.properties.HMSPropertyManager.MaintenanceOpType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.apache.hadoop.hive.metastore.properties.HMSPropertyManager.JEXL;
import static org.apache.hadoop.hive.metastore.properties.HMSPropertyManager.MAINTENANCE_OPERATION;
import static org.apache.hadoop.hive.metastore.properties.HMSPropertyManager.MAINTENANCE_STATUS;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.BOOLEAN;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.DATETIME;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.DOUBLE;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.INTEGER;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.JSON;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.STRING;

public abstract class HMSTestBase {
  /**
   * Abstract the property client access on a given namespace.
   */
  interface PropertyClient {
    boolean setProperties(Map<String, String> properties);
    Map<String, Map<String, String>> getProperties(String mapPrefix, String mapPredicate, String... selection) throws IOException;
  }

  interface HttpPropertyClient extends PropertyClient {
    default Map<String, String> getProperties(List<String> selection) throws IOException {
      throw new UnsupportedOperationException("not implemented in " + this.getClass());
    }
  }

  protected ObjectStore objectStore = null;
  protected Warehouse warehouse = null;
  protected Configuration conf = null;

  protected static final Logger LOG = LoggerFactory.getLogger(TestObjectStore.class.getName());
  static Random RND = new Random(20230424);
  protected String NS;// = "hms" + RND.nextInt(100);
  protected String DB;// = "dbtest" + RND.nextInt(100);
  protected PropertyClient client;
  protected int port = -1;

  boolean createStore(Configuration conf) {
    try {
      MetaStoreTestUtils.setConfForStandloneMode(conf);
      objectStore = new ObjectStore();
      objectStore.setConf(conf);
      //TestObjectStore.dropAllStoreObjects(objectStore);
      warehouse = new Warehouse(conf);
      HMSHandler.createDefaultCatalog(objectStore, warehouse);
      // configure object store
      objectStore.createDatabase(new DatabaseBuilder()
          .setCatalogName("hive")
          .setName(DB)
          .setDescription("description")
          .setLocation("locationurl")
          .build(conf));
    } catch (InvalidObjectException | MetaException | InvalidOperationException xmeta) {
      throw new PropertyException("unable to initialize store", xmeta);
    }
    return true;
  }

  @Before
  public void setUp() throws Exception {
    NS = "hms" + RND.nextInt(100);
    DB = "dbtest" + RND.nextInt(100);
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.PROPERTIES_SERVLET_PORT, 0);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    // Events that get cleaned happen in batches of 1 to exercise batching code
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.EVENT_CLEAN_MAX_EVENTS, 1L);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_METASTORE_AUTHENTICATION_JWT_JWKS_URL, "jwt");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    // The server
    port = createServer(conf);
    System.out.println("Starting MetaStore Server on port " + port);
    // The store
    if (objectStore == null) {
      // The store
      boolean inited = createStore(conf);
      LOG.info("MetaStore store initialization " + (inited ? "successful" : "failed"));
    }
    // The manager decl
    PropertyManager.declare(NS, HMSPropertyManager.class);
    // The client
    client = createClient(conf, port);
    Assert.assertNotNull("Unable to connect to the MetaStore server", client);
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (objectStore != null) {
        objectStore.flushCache();
        objectStore.dropDatabase("hive", DB);
      }
      if (port >= 0) {
        stopServer(port);
        port = -1;
      }
      // Clear the SSL system properties before each test.
      System.clearProperty(ObjectStore.TRUSTSTORE_PATH_KEY);
      System.clearProperty(ObjectStore.TRUSTSTORE_PASSWORD_KEY);
      System.clearProperty(ObjectStore.TRUSTSTORE_TYPE_KEY);
      //
    } finally {
      objectStore = null;
      client = null;
      conf = null;
    }
  }

  /**
   * Creates and starts the server.
   * @param conf
   * @return the server port
   * @throws Exception
   */
  protected int createServer(Configuration conf) throws Exception {
    return 0;
  }

  /**
   * Stops the server.
   * @param port the server port
   * @throws Exception
   */
  protected void stopServer(int port) throws Exception {
    // nothing
  }

  /**
   * Creates a client.
   * @return the client instance
   * @throws Exception
   */
  protected abstract PropertyClient createClient(Configuration conf, int port) throws Exception;


  public void runOtherProperties0(PropertyClient client) throws Exception {
    Map<String, String> ptyMap = createProperties0();
    boolean commit = client.setProperties(ptyMap);
    Assert.assertTrue(commit);
    // select tables whose policy table name starts with table0
    Map<String, Map<String, String>> maps = client.getProperties("db0.table", "policy.'Table-name' =^ 'table0'");
    Assert.assertNotNull(maps);
    Assert.assertEquals(8, maps.size());

    // select
    Map<String, Map<String, String>> project = client.getProperties("db0.tabl", "fillFactor > 92",
        "fillFactor",
        "{ 'policy' : { 'Compaction' : { 'target-size' : policy.Compaction.'target-size' } } }");
    Assert.assertNotNull(project);
    Assert.assertEquals(2, project.size());
  }

  static Map<String, String> createProperties0() {
    // configure hms
    HMSPropertyManager.declareTableProperty("fillFactor", DOUBLE, 0.75d);
    HMSPropertyManager.declareTableProperty("policy", JSON, null);
    HMSPropertyManager.declareTableProperty("maint_status", MAINTENANCE_STATUS, null);
    HMSPropertyManager.declareTableProperty("maint_operation", MAINTENANCE_OPERATION, null);
    // link store and manager
    try {
      String json = IOUtils.toString(
          HMSDirectTest.class.getResourceAsStream("payload.json"),
          "UTF-8"
      );
      JxltEngine JXLT = JEXL.createJxltEngine();
      JxltEngine.Template jsonjexl = JXLT.createTemplate(json, "table", "delta", "g");
      Assert.assertNotNull(json);
      Map<String, String> ptyMap = new TreeMap<>();
      for (int i = 0; i < 16; ++i) {
        String tname = "table" + String.format("%1$02o", i);
        String tb = "db0." + tname + ".";
        ptyMap.put(tb + "fillFactor", Integer.toString(100 - (5 * i)));

        StringWriter strw = new StringWriter();
        jsonjexl.evaluate(null, strw, tname, i * i % 100, (i + 1) % 7);
        ptyMap.put(tb + "policy", strw.toString());

        HMSPropertyManager.MaintenanceOpStatus status = HMSPropertyManager.findOpStatus( i % MaintenanceOpStatus.values().length);
        Assert.assertNotNull(status);
        ptyMap.put(tb + "maint_status", status.toString());

        HMSPropertyManager.MaintenanceOpType type = HMSPropertyManager.findOpType(i % MaintenanceOpType.values().length);
        Assert.assertNotNull(type);
        ptyMap.put(tb + "maint_operation", type.toString());
      }
      return ptyMap;
    } catch (IOException xio) {
      return null;
    }
  }

  public void runOtherProperties1(PropertyClient client) throws Exception {
    Map<String, String> ptyMap = createProperties1();
    boolean commit = client.setProperties(ptyMap);
    Assert.assertTrue(commit);
    // go get some
    Map<String, Map<String, String>> maps = client.getProperties("db1.table", "someb && fillFactor < 95");
    Assert.assertNotNull(maps);
    Assert.assertEquals(5, maps.size());

    if (client instanceof HttpPropertyClient) {
      HttpPropertyClient httpClient = (HttpPropertyClient) client;
      // get fillfactors using getProperties, create args array from previous result
      List<String> keys = new ArrayList<>(maps.keySet());
      for (int k = 0; k < keys.size(); ++k) {
        keys.set(k, keys.get(k) + ".fillFactor");
      }
      Object values = httpClient.getProperties(keys);
      Assert.assertTrue(values instanceof Map);
      Map<String, String> getm = (Map<String, String>) values;
      for (Map.Entry<String, Map<String, String>> entry : maps.entrySet()) {
        Map<String, String> map0v = entry.getValue();
        Assert.assertEquals(map0v.get("fillFactor"), getm.get(entry.getKey() + ".fillFactor"));
      }
    }

    maps = client.getProperties("db1.table", "fillFactor > 92", "fillFactor");
    Assert.assertNotNull(maps);
    Assert.assertEquals(8, maps.size());
  }

  static Map<String, String> createProperties1() {
    // configure hms
    HMSPropertyManager.declareTableProperty("id", INTEGER, null);
    HMSPropertyManager.declareTableProperty("name", STRING, null);
    HMSPropertyManager.declareTableProperty("uuid", STRING, null);
    HMSPropertyManager.declareTableProperty("fillFactor", DOUBLE, 0.75d);
    HMSPropertyManager.declareTableProperty("someb", BOOLEAN, Boolean.FALSE);
    HMSPropertyManager.declareTableProperty("creation date", DATETIME, "2023-01-06T12:16:00");
    HMSPropertyManager.declareTableProperty("project", STRING, "Hive");
    HMSPropertyManager.declareTableProperty("policy", JSON, null);
    HMSPropertyManager.declareTableProperty("maint_status", MAINTENANCE_STATUS, null);
    HMSPropertyManager.declareTableProperty("maint_operation", MAINTENANCE_OPERATION, null);
    // use properties to init
    Map<String, String> ptys = new TreeMap<>();
    for (int i = 0; i < 16; ++i) {
      String tb = "db1.table" + String.format("%1$02o", i) + ".";
      ptys.put(tb + "id", Integer.toString(1000 + i));
      ptys.put(tb + "name", "TABLE_" + i);
      ptys.put(tb + "fillFactor", Integer.toString(100 - i));
      ptys.put(tb + "someb", (i % 2) == 0 ? "true" : "false");
    }
    return ptys;
  }

}
