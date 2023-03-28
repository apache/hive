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
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.JdoPropertyStore;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.TestObjectStore;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MaintenanceOpStatus;
import org.apache.hadoop.hive.metastore.api.MaintenanceOpType;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

import static org.apache.hadoop.hive.metastore.properties.HMSPropertyManager.JEXL;
import static org.apache.hadoop.hive.metastore.properties.HMSPropertyManager.MAINTENANCE_OPERATION;
import static org.apache.hadoop.hive.metastore.properties.HMSPropertyManager.MAINTENANCE_STATUS;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.BOOLEAN;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.DATETIME;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.DOUBLE;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.INTEGER;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.JSON;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.STRING;

public class HMSPropertyStoreTest {
  protected Warehouse warehouse = null;
  protected ObjectStore objectStore = null;
  protected Configuration conf = null;

  protected static final String DB1 = "dbtest1";
  protected static final Logger LOG = LoggerFactory.getLogger(TestObjectStore.class.getName());

  @Before
  public void setUp() throws Exception {
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    // Events that get cleaned happen in batches of 1 to exercise batching code
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.EVENT_CLEAN_MAX_EVENTS, 1L);
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    objectStore = new ObjectStore();
    objectStore.setConf(conf);
    warehouse = new Warehouse(conf);
    HMSHandler.createDefaultCatalog(objectStore, warehouse);
    // configure object store
    objectStore.createDatabase(new DatabaseBuilder()
        .setCatalogName("hive")
        .setName(DB1)
        .setDescription("description")
        .setLocation("locationurl")
        .build(conf));
  }

  @After
  public void tearDown() throws Exception {
    objectStore.dropDatabase("hive", DB1);
    objectStore.flushCache();
    // Clear the SSL system properties before each test.
    System.clearProperty(ObjectStore.TRUSTSTORE_PATH_KEY);
    System.clearProperty(ObjectStore.TRUSTSTORE_PASSWORD_KEY);
    System.clearProperty(ObjectStore.TRUSTSTORE_TYPE_KEY);
  }

  @Test
  public void testHMSProperties() throws Exception {
    // configure hms
    HMSPropertyManager.declareClusterProperty("clstrp0", STRING, "Spark");
    HMSPropertyManager.declareDatabaseProperty("store", STRING, "ORC");
    HMSPropertyManager.declareTableProperty("id", INTEGER, null);
    HMSPropertyManager.declareTableProperty("name", STRING, null);
    HMSPropertyManager.declareTableProperty("uuid", STRING, null);
    HMSPropertyManager.declareTableProperty("fillfactor", DOUBLE, 0.75d);
    HMSPropertyManager.declareTableProperty("creation date", DATETIME, "2023-01-06T12:16:00");
    HMSPropertyManager.declareTableProperty("project", STRING, "Hive");
    // link store and manager
    CachingPropertyStore store = new CachingPropertyStore(new JdoPropertyStore(objectStore));
    HMSPropertyManager hms = new HMSPropertyManager(store);

    hms.setProperty("ser.der.id", 42);
    hms.setProperty("ser.der.name", "serder");
    hms.setProperty("ser.der.project", "Metastore");
    hms.commit();
    store.clearCache();
    Assert.assertEquals(42, hms.getPropertyValue("ser.der.id"));
    Assert.assertEquals("serder", hms.getPropertyValue("ser.der.name"));
    Assert.assertEquals("Metastore", hms.getPropertyValue("ser.der.project"));
  }

  @Test
  public void testOtherProperties() throws Exception {
    // configure hms
    HMSPropertyManager.declareTableProperty("fillFactor", DOUBLE, 0.75d);
    HMSPropertyManager.declareTableProperty("policy", JSON, null);
    HMSPropertyManager.declareTableProperty("maint_status", MAINTENANCE_STATUS, null);
    HMSPropertyManager.declareTableProperty("maint_operation", MAINTENANCE_OPERATION, null);
    // link store and manager
    PropertyStore store = new CachingPropertyStore(new JdoPropertyStore(objectStore));

    String json = IOUtils.toString(
        this.getClass().getResourceAsStream("pol0.json"),
        StandardCharsets.UTF_8
    );
    JxltEngine JXLT = JEXL.createJxltEngine();
    JxltEngine.Template jsonjexl = JXLT.createTemplate(json, "table", "delta", "g");
    Assert.assertNotNull(json);
    HMSPropertyManager hms = new HMSPropertyManager(store);
    for (int i = 0; i < 16; ++i) {
      String tname = "table" + String.format("%1$02o", i);
      String tb = "db0." + tname + ".";
      hms.setProperty(tb + "fillFactor", 100 - (5 * i));
      StringWriter strw = new StringWriter();
      jsonjexl.evaluate(null, strw, tname, i * i % 100, (i + 1) % 7);

      hms.setProperty(tb + "policy", strw.toString());
      hms.setProperty(tb + "maint_status", MaintenanceOpStatus.findByValue(i % MaintenanceOpStatus.values().length));
      hms.setProperty(tb + "maint_operation", MaintenanceOpType.findByValue(i % MaintenanceOpType.values().length));
    }
    hms.commit();
    // select tables whose policy table name starts with table0
    Map<String, PropertyMap> maps = hms.selectProperties("db0.table", "policy.'Table-name' =^ 'table0'");
    Assert.assertNotNull(maps);
    Assert.assertEquals(8, maps.size());

    // select
    Map<String, PropertyMap> project = hms.selectProperties(
      "db0.tabl",
"fillFactor > 92",
"fillFactor",
        "{ 'policy' : { 'Compaction' : { 'target-size' : policy.Compaction.'target-size' } } }");
    Assert.assertNotNull(project);
    Assert.assertEquals(2, project.size());
  }

  @Test
  public void testPropertiesScript0() throws Exception {
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
    // link store and manager
    PropertyStore store = new CachingPropertyStore(new JdoPropertyStore(objectStore));
    // use properties to init
    Properties ptys = new Properties();
    for (int i = 0; i < 16; ++i) {
      String tb = "db0.table" + Integer.toHexString(i) + ".";
      ptys.put(tb + "id", 1000 + i);
      ptys.put(tb + "name", "TABLE_" + i);
      ptys.put(tb + "fillFactor", 100 - i);
      ptys.put(tb + "someb", (i % 2) == 0);
    }
    HMSPropertyManager hms = new HMSPropertyManager(store);
    hms.setProperties(ptys);
    hms.commit();
    Map<String, PropertyMap> maps = hms.selectProperties("db0.table","someb && fillFactor < 95");
    Assert.assertNotNull(maps);
  }

}
