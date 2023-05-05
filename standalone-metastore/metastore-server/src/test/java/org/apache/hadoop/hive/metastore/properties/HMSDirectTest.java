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
import org.apache.hadoop.hive.metastore.RawStore;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.hadoop.hive.metastore.properties.PropertyType.DATETIME;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.DOUBLE;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.INTEGER;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.STRING;

public class HMSDirectTest extends HMSTestBase {
  /**
   * An embedded property client.
   */
  static class DirectPropertyClient implements PropertyClient {
    private final HMSPropertyManager hms;
    DirectPropertyClient(HMSPropertyManager hms) {
      this.hms = hms;
    }

    @Override
    public boolean setProperties(Map<String, String> properties) {
      try {
        hms.setProperties(properties);
        hms.commit();;
        return true;
      } catch(Exception tex) {
        hms.rollback();
        return false;
      }
    }

    @Override
    public Map<String, Map<String, String>> getProperties(String mapPrefix, String mapPredicate, String... selection) throws IOException {
      try {
        Map<String, PropertyMap> selected  = hms.selectProperties(mapPrefix, mapPredicate, selection);
        Map<String, Map<String, String>> returned = new TreeMap<>();
        selected.forEach((k, v)->{
          returned.put(k, v.export());
        });
        hms.commit();
        return returned;
      } catch(Exception tex) {
        hms.rollback();
        return null;
      }
    }
  }

  @Override protected PropertyClient createClient(Configuration conf, int port) throws Exception {
    RawStore store = objectStore;
    HMSPropertyManager mgr = new HMSPropertyManager(store.getPropertyStore());
    return new DirectPropertyClient(mgr);
  }

  @Test
  public void testDirectProperties() throws Exception {
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
