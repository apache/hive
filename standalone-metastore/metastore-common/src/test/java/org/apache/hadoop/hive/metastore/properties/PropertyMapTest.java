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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.apache.hadoop.hive.metastore.properties.PropertyType.BOOLEAN;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.INTEGER;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.JSON;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.STRING;

public class PropertyMapTest {
  private final PropertyStore store;
  private final PropertySchema table;

  public PropertyMapTest() {
    table = new PropertySchema("table");
    table.declareProperty("id", INTEGER);
    table.declareProperty("name", STRING);
    table.declareProperty("uuid", STRING);
    table.declareProperty("project", STRING, "Hive");
    table.declareProperty("policy", JSON);
    store = new TransientPropertyStore();
  }

  private static PropertyMap fetchProperties(PropertyStore store, String name, Function<String, PropertySchema> getSchema) {
    PropertyMap map = store.fetchProperties(name, getSchema);
    return map != null? map : new PropertyMap(getSchema.apply(name));
  }
  private static Map<String,PropertyMap> selectProperties(PropertyStore store, String mapPrefix, Predicate<String> nameFilter, Function<String, PropertySchema> getSchema) {
    return store.selectProperties(mapPrefix, nameFilter, getSchema);
  }
  private PropertySchema fetchSchema(String name) {
    return table;
  }

  @Test public void testBasics() {
    PropertyMap map = new PropertyMap(table);
    map.setClean();
    try {
      map.putProperty("nosuchproperty", 32);
      Assert.fail("schema does not describe nosuchproperty");
    } catch(IllegalArgumentException xill) {
      Assert.assertTrue(xill.getMessage().contains("nosuchproperty"));
    }
    Assert.assertFalse(map.isDirty());

    try {
      map.putProperty("id", "notavalidvalue");
      Assert.fail("schema expects an int");
    } catch(IllegalArgumentException xill) {
      Assert.assertTrue(xill.getMessage().contains("notavalidvalue"));
    }
    Assert.assertFalse(map.isDirty());

    try {
      table.declareProperty(null, BOOLEAN);
      Assert.fail("null name!");
    } catch(IllegalArgumentException xill) {
      Assert.assertTrue(xill.getMessage().contains("name"));
    }
    Assert.assertFalse(map.isDirty());

    try {
      table.declareProperty("whatever", null);
      Assert.fail("null type!");
    } catch(IllegalArgumentException xill) {
      Assert.assertTrue(xill.getMessage().contains("type"));
    }
    Assert.assertFalse(map.isDirty());
  }

  @Test
  public void testMap0() {
    runMap0(store);
  }

  @Test
  public void testMap1() {
    PropertyStore caching = new CachingPropertyStore(store);
    runMap0(caching);
  }

  private void runMap0(PropertyStore store) {
    PropertyMap map = fetchProperties(store, "p0", this::fetchSchema);
    Assert.assertEquals(INTEGER, map.getTypeOf("id"));
    Assert.assertNull(map.putProperty("id", 42));
    Assert.assertEquals(STRING, map.getTypeOf("name"));
    Assert.assertNull(map.putProperty("name", "table0"));

    UUID digest0 = map.getDigest();
    Assert.assertNotNull(digest0);
    Assert.assertEquals("table0", map.putProperty("name", "TABLE0"));
    UUID digest1 = map.getDigest();
    Assert.assertNotNull(digest1);
    Assert.assertNotEquals(digest0, digest1);
    Assert.assertEquals("TABLE0", map.putProperty("name", "table0"));
    UUID digest2 = map.getDigest();
    Assert.assertNotNull(digest2);
    Assert.assertEquals(digest0, digest2);
    Assert.assertTrue(map.isDirty());

    Assert.assertEquals("table0", map.getPropertyValue("name"));
    Assert.assertEquals("Hive", map.getPropertyValue("project"));
  }

  @Test
  public void testSelect0() throws IOException, ClassNotFoundException {
    runSelect0(store);
  }
  @Test
  public void testSelect1() throws IOException, ClassNotFoundException {
    PropertyStore caching = new CachingPropertyStore(store);
    runSelect0(caching);
  }
  private void runSelect0(PropertyStore store) {
    for(int i = 0; i < 16; ++i) {
      String name = "table" + Integer.toOctalString(i);
      PropertyMap map = fetchProperties(store, name, this::fetchSchema);
      Assert.assertEquals(INTEGER, map.getTypeOf("id"));
      Assert.assertNull(map.putProperty("id", 42 + i));
      Assert.assertEquals(STRING, map.getTypeOf("name"));
      Assert.assertNull(map.putProperty("name", "relation" + Integer.toOctalString(i)));
      store.saveProperties(name, map);
    }
    Map<String, PropertyMap> selected = selectProperties(store, "table", null, this::fetchSchema);
    Assert.assertNotNull(selected);
  }

  @Test
  public void testSerDer0() throws IOException, ClassNotFoundException {
    runSerDer(store);
  }

  @Test
  public void testSerDer1() throws IOException, ClassNotFoundException {
    PropertyStore caching = new CachingPropertyStore(store);
    runSerDer(caching);
  }

  private void runSerDer(PropertyStore store) throws IOException {
    PropertyMap map = fetchProperties(store, "serder", this::fetchSchema);
    map.putProperty("id", 42);
    map.putProperty("name", "serder");
    map.putProperty("project", "Metastore");

    // do serialize/deserialize through instance
    ByteArrayOutputStream baos = new ByteArrayOutputStream(128);
    ObjectOutputStream out = new ObjectOutputStream(baos);
    out.writeObject(map);
    byte[] data = baos.toByteArray();
    ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(data));
    Function<String, PropertySchema> getSchema  = this::fetchSchema;
    PropertyMap copy = SerializationProxy.read(in,  getSchema);
    Assert.assertEquals(42, copy.getProperty("id"));
    Assert.assertEquals("serder", copy.getProperty("name"));
    Assert.assertEquals(map.getProperty("id"), copy.getProperty("id"));
    Assert.assertEquals(map.getPropertyValue("name"), copy.getPropertyValue("name"));

    // do serialize/derserialize through store
    Assert.assertTrue(map.isDirty());
    store.saveProperties("serder", map);
    map = fetchProperties(store, "serder", this::fetchSchema);
    Assert.assertFalse(map.isDirty());
    Assert.assertEquals(42, map.getPropertyValue("id"));
    Assert.assertEquals("serder", map.getPropertyValue("name"));
    Assert.assertEquals("Metastore", map.getPropertyValue("project"));

    // resetting
    Assert.assertEquals("Metastore", map.removeProperty("project"));
    Assert.assertTrue(map.isDirty());
    Assert.assertEquals("Hive", map.getPropertyValue("project"));
    store.saveProperties("serder", map);
    Assert.assertFalse(map.isDirty());
    Assert.assertEquals("Hive", map.getPropertyValue("project"));
  }

  @Test
  public void testImportExport() {
    Properties ptys = new Properties();
    ptys.setProperty("id", "42");
    ptys.setProperty("name", "serder");
    ptys.setProperty("project", "Metastore");
    // create map from properties
    PropertyMap map = new PropertyMap(table);
    map.importFromProperties(ptys);
    // create control map
    PropertyMap ctl = new PropertyMap(table);
    ctl.putProperty("id", 42);
    ctl.putProperty("name", "serder");
    ctl.putProperty("project", "Metastore");
    // ensure property based vs ctl are equivalent
    Assert.assertEquals(map.hashCode(), ctl.hashCode());
    Assert.assertEquals(map.getDigest(), ctl.getDigest());
    Assert.assertEquals(map, ctl);
    // and that back to properties lead to same result
    Properties ctlp = new Properties();
    ctl.exportToProperties(ctlp);
    Assert.assertEquals(ctlp, ptys);
  }

}
