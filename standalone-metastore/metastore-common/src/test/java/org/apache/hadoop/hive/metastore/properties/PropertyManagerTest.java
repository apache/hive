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

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;
import java.util.function.BooleanSupplier;

import static org.apache.hadoop.hive.metastore.properties.PropertyType.DATETIME;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.DOUBLE;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.INTEGER;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.STRING;

public class PropertyManagerTest {
  private final PropertyStore store;
  private final PropertyManager manager;

  public PropertyManagerTest() {
    JavaTestManager.declareDomainProperty("framework", STRING, "Spark");

    JavaTestManager.declarePackageProperty("store", STRING, "ORC");

    JavaTestManager.declareClazzProperty("id", INTEGER, null);
    JavaTestManager.declareClazzProperty("name", STRING, null);
    JavaTestManager.declareClazzProperty("uuid", STRING, null);
    JavaTestManager.declareClazzProperty("fillfactor", DOUBLE, 0.75d);
    JavaTestManager.declareClazzProperty("creation date", DATETIME, "2023-01-06T12:16:00");
    JavaTestManager.declareClazzProperty("project", STRING, "Hive");


    store = new TransientPropertyStore();
    manager = new JavaTestManager(store);

    Properties jutilp = new Properties();
    jutilp.setProperty("domain.framework", "Tez");
    jutilp.setProperty("package.store", "Iceberg");
    manager.importDefaultValues(jutilp);
  }

  @Test
  public void testSerDerScript() {
    runSerDer(() -> {
      return (boolean) manager.runScript(
          "setProperty('framework', 'llap');" +
              "setProperty('ser.store', 'Parquet');" +
              "setProperty('ser.der.id', 42);" +
              "setProperty('ser.der.name', 'serder');" +
              "setProperty('ser.der.project', 'Metastore');" +
              "true;");
    });
  }
  @Test
  public void testSelectDirty() {
    Object result = manager.runScript(
          "setProperty('framework', 'llap');" +
              "setProperty('ser.store', 'Parquet');" +
              "setProperty('ser.der.id', 42);" +
              "setProperty('ser.der.name', 'serder');" +
              "setProperty('ser.der.project', 'Metastore');" +
              "selectProperties('ser.der', ()->true);");
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof Map<?,?>);
    Assert.assertEquals(1, ((Map<?,?>) result).size());
    manager.commit();
  }


  @Test
  public void testSerDer() {
    runSerDer(()->{
      // update main (no commit yet)
      manager.setProperty("framework", "llap");
      manager.setProperty("ser.store", "Parquet");
      manager.setProperty("ser.der.id", 42);
      manager.setProperty("ser.der.name", "serder");
      manager.setProperty("ser.der.project", "Metastore");
      return true;
    });
  }
  private void runSerDer(BooleanSupplier update) {
    // create a second manager to check isolation
    JavaTestManager hms2 = new JavaTestManager(store);

    Assert.assertTrue(update.getAsBoolean());

    Assert.assertEquals("llap", manager.getPropertyValue("framework"));
    Assert.assertEquals("Parquet", manager.getPropertyValue("ser.store"));
    Assert.assertEquals(42, manager.getPropertyValue("ser.der.id"));
    Assert.assertEquals("serder", manager.getPropertyValue("ser.der.name"));
    Assert.assertEquals("Metastore", manager.getPropertyValue("ser.der.project"));

    // second manager unaffected
    Assert.assertEquals("Tez", hms2.getPropertyValue("framework"));
    Assert.assertEquals("Iceberg", hms2.getPropertyValue("ser.store"));
    Assert.assertNull(hms2.getPropertyValue("ser.der.id"));
    Assert.assertEquals(null, hms2.getPropertyValue("ser.der.name"));
    Assert.assertEquals("Hive", hms2.getPropertyValue("ser.der.project"));

    // commit main
    manager.commit();

    // main manager
    Assert.assertEquals(42, manager.getPropertyValue("ser.der.id"));
    Assert.assertEquals("serder", manager.getPropertyValue("ser.der.name"));
    Assert.assertEquals("Metastore", manager.getPropertyValue("ser.der.project"));
    Assert.assertEquals("llap", manager.getPropertyValue("framework"));
    Assert.assertEquals("Parquet", manager.getPropertyValue("ser.store"));

    // second manager agrees
    Assert.assertEquals(42, hms2.getPropertyValue("ser.der.id"));
    Assert.assertEquals("serder", hms2.getPropertyValue("ser.der.name"));
    Assert.assertEquals("Metastore", hms2.getPropertyValue("ser.der.project"));
    Assert.assertEquals("llap", hms2.getPropertyValue("framework"));
    Assert.assertEquals("Parquet", hms2.getPropertyValue("ser.store"));
  }

}
