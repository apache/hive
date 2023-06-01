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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HMSHandler;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.apache.hadoop.hive.metastore.properties.PropertyType.DATETIME;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.DOUBLE;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.INTEGER;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.STRING;

/**
 * In-process property manager test.
 */
public class HMSDirectTest extends HMSTestBase {
  protected ObjectStore objectStore = null;
  static Random RND = new Random(20230424);
  protected String DB;// = "dbtest" + RND.nextInt(100);
  @Override protected int createServer(Configuration conf) {
    // The store
    if (objectStore == null) {
      // The store
      try {
        DB = "dbtest" + RND.nextInt(100);
        objectStore = new ObjectStore();
        objectStore.setConf(conf);
        Warehouse warehouse = new Warehouse(conf);
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
    }
    return 0;
  }

  @Override public void tearDown() throws Exception {
    super.tearDown();
    if (objectStore != null) {
      objectStore.flushCache();
      objectStore.dropDatabase("hive", DB);
    }
  }

  /**
   * An embedded property client.
   */
  static class DirectPropertyClient implements HttpPropertyClient {
    private final HMSPropertyManager hms;
    DirectPropertyClient(HMSPropertyManager hms) {
      this.hms = hms;
    }

    @Override
    public boolean setProperties(Map<String, String> properties) {
      try {
        hms.setProperties(properties);
        hms.commit();
        return true;
      } catch(Exception tex) {
        hms.rollback();
        return false;
      }
    }

    @Override
    public Map<String, Map<String, String>> getProperties(String mapPrefix, String mapPredicate, String... selection) throws IOException {
      try {
        List<String> project = selection == null || selection.length == 0? null : Arrays.asList(selection);
        Map<String, PropertyMap> selected  = hms.selectProperties(mapPrefix, mapPredicate, project);
        Map<String, Map<String, String>> returned = new TreeMap<>();
        selected.forEach((k, v)-> returned.put(k, v.export(project == null)));
        hms.commit();
        return returned;
      } catch(Exception tex) {
        hms.rollback();
        return null;
      }
    }

    @Override
    public Map<String, String> getProperties(List<String> keys) throws IOException {
      Map<String, String> returned = new TreeMap<>();
      try {
        for(String key : keys) {
          String value  = hms.exportPropertyValue(key);
          if (value != null) {
            returned.put(key, value);
          }
        }
        hms.commit();
        return returned;
      } catch(Exception tex) {
        hms.rollback();
        return null;
      }
    }
  }

  @Override protected PropertyClient createClient(Configuration conf, int port) throws Exception {
    HMSPropertyManager mgr = new HMSPropertyManager(objectStore.getPropertyStore());
    return new DirectPropertyClient(mgr);
  }

  @Test
  public void testDirectProperties() {
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
    HMSPropertyManager hms = new HMSPropertyManager(objectStore.getPropertyStore());

    hms.setProperty("ser.der.id", 42);
    hms.setProperty("ser.der.name", "serder");
    hms.setProperty("ser.der.project", "Metastore");
    hms.commit();
    PropertyStore store = objectStore.getPropertyStore();
    if (store instanceof CachingPropertyStore) {
      ((CachingPropertyStore) store).clearCache();
    }
    Assert.assertEquals(42, hms.getPropertyValue("ser.der.id"));
    Assert.assertEquals("serder", hms.getPropertyValue("ser.der.name"));
    Assert.assertEquals("Metastore", hms.getPropertyValue("ser.der.project"));
  }

  @Test
  public void testDirectProperties0() throws Exception {
    runOtherProperties0(client);
  }

  @Test
  public void testDirectProperties1() throws Exception {
    runOtherProperties1(client);
  }

}
