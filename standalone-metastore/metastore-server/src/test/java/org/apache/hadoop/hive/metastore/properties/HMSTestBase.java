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
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.TestObjectStore;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MaintenanceOpStatus;
import org.apache.hadoop.hive.metastore.api.MaintenanceOpType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
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

  protected ObjectStore objectStore = null;
  protected Warehouse warehouse = null;
  protected Configuration conf = null;

  protected static final Logger LOG = LoggerFactory.getLogger(TestObjectStore.class.getName());
  static Random RND = new Random(20230424);
  protected final String NS = "hms" + RND.nextInt(100);
  protected final String DB = "dbtest" + RND.nextInt(100);
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
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    // Events that get cleaned happen in batches of 1 to exercise batching code
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.EVENT_CLEAN_MAX_EVENTS, 1L);
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    if (port > 0) {
      Assert.assertNotNull("Unable to connect to the MetaStore server", client);
      return;
    }
    // The server
    port = createServer(conf);
    System.out.println("Starting MetaStore Server on port " + port);

    // The store
    boolean inited = createStore(conf);
    LOG.info("MetaStore store initialization " + (inited ? "successful" : "failed"));
    // The client
    client = createClient(conf, port);
    Assert.assertNotNull("Unable to connect to the MetaStore server", client);
  }

  @After
  public void tearDown() throws Exception {
    if (objectStore != null) {
      objectStore.flushCache();
      objectStore.dropDatabase("hive", DB);
      objectStore = null;
    }
    if (port >= 0) {
      MetaStoreTestUtils.close(port);
    }
    // Clear the SSL system properties before each test.
    System.clearProperty(ObjectStore.TRUSTSTORE_PATH_KEY);
    System.clearProperty(ObjectStore.TRUSTSTORE_PASSWORD_KEY);
    System.clearProperty(ObjectStore.TRUSTSTORE_TYPE_KEY);
  }

  /**
   * A Thrift based property client.
   */
  static class ThriftPropertyClient implements PropertyClient {
    private final String namespace;
    private final HiveMetaStoreClient client;
    ThriftPropertyClient(String ns, HiveMetaStoreClient c) {
      this.namespace = ns;
      this.client = c;
    }

    @Override
    public boolean setProperties(Map<String, String> properties) {
      try {
        return client.setProperties(namespace, properties);
      } catch(TException tex) {
        return false;
      }
    }

    @Override
    public Map<String, Map<String, String>> getProperties(String mapPrefix, String mapPredicate, String... selection) throws IOException {
      try {
        return client.getProperties(namespace, mapPrefix, mapPredicate, selection);
      } catch(TException tex) {
        return null;
      }
    }
  }

  /**
   * Creates and starts the server.
   * @param conf
   * @return the server port
   * @throws Exception
   */
  protected int createServer(Configuration conf) throws Exception {
    return MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(), conf);
  }

  /**
   * Creates a client.
   * @return the client instance
   * @throws Exception
   */
  protected PropertyClient createClient(Configuration conf, int port) throws Exception {
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_URIS, "http://localhost:" + port);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.EXECUTE_SET_UGI, false);
    HiveMetaStoreClient client = new HiveMetaStoreClient(conf);
    return new ThriftPropertyClient(NS, client);
  }


  public void runOtherProperties0(PropertyClient client) throws Exception {
    Map<String, String> ptyMap = createProperties0(NS);
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

  static Map<String, String> createProperties0(String ns) {
    PropertyManager.declare(ns, HMSPropertyManager.class);
    // configure hms
    HMSPropertyManager.declareTableProperty("fillFactor", DOUBLE, 0.75d);
    HMSPropertyManager.declareTableProperty("policy", JSON, null);
    HMSPropertyManager.declareTableProperty("maint_status", MAINTENANCE_STATUS, null);
    HMSPropertyManager.declareTableProperty("maint_operation", MAINTENANCE_OPERATION, null);
    // link store and manager
    try {
      String json = IOUtils.toString(
          HMSDirectTest.class.getResourceAsStream("pol0.json"),
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

        MaintenanceOpStatus status = MaintenanceOpStatus.findByValue(1 + i % MaintenanceOpStatus.values().length);
        Assert.assertNotNull(status);
        ptyMap.put(tb + "maint_status", status.toString());

        MaintenanceOpType type = MaintenanceOpType.findByValue(1 + i % MaintenanceOpType.values().length);
        Assert.assertNotNull(type);
        ptyMap.put(tb + "maint_operation", type.toString());
      }
      return ptyMap;
    } catch (IOException xio) {
      return null;
    }
  }

  public void runOtherProperties1(PropertyClient client) throws Exception {
    Map<String, String> ptyMap = createProperties1(NS);
    boolean commit = client.setProperties(ptyMap);
    Assert.assertTrue(commit);
    // go get some
    Map<String, Map<String, String>> maps = client.getProperties("db0.table", "someb && fillFactor < 95");
    Assert.assertNotNull(maps);
  }

  static Map<String, String> createProperties1(String ns) {
    PropertyManager.declare(ns, HMSPropertyManager.class);
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
      String tb = "db0.table" + Integer.toHexString(i) + ".";
      ptys.put(tb + "id", Integer.toString(1000 + i));
      ptys.put(tb + "name", "TABLE_" + i);
      ptys.put(tb + "fillFactor", Integer.toString(100 - i));
      ptys.put(tb + "someb", (i % 2) == 0 ? "true" : "false");
    }
    return ptys;
  }

}
