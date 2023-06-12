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

import com.google.common.base.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HMSHandler;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.TestObjectStore;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.model.MMetastoreDBProperties;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class PropertyStoreTest {
  private ObjectStore objectStore = null;
  private Configuration conf;

  private static final String DB1 = "testdb1";
  private static final Logger LOG = LoggerFactory.getLogger(TestObjectStore.class.getName());

  private static final class LongSupplier implements Supplier<Long> {
    public long value = 0;

    @Override
    public Long get() {
      return value;
    }
  }

  @Before
  public void setUp() throws Exception {
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    // Events that get cleaned happen in batches of 1 to exercise batching code
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.EVENT_CLEAN_MAX_EVENTS, 1L);
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    objectStore = new ObjectStore();
    objectStore.setConf(conf);
    //dropAllStoreObjects(objectStore);
    HMSHandler.createDefaultCatalog(objectStore, new Warehouse(conf));
  }

  @After
  public void tearDown() throws Exception {
    // Clear the SSL system properties before each test.
    System.clearProperty(ObjectStore.TRUSTSTORE_PATH_KEY);
    System.clearProperty(ObjectStore.TRUSTSTORE_PASSWORD_KEY);
    System.clearProperty(ObjectStore.TRUSTSTORE_TYPE_KEY);
  }

  @Test
  public void testProperties() throws Exception {
    objectStore.createDatabase(new DatabaseBuilder()
        .setName(DB1)
        .setDescription("description")
        .setLocation("locationurl")
        .build(conf));

    MMetastoreDBProperties p0 = objectStore.putProperties("key", "value", null, "content".getBytes(StandardCharsets.UTF_8));
    Assert.assertNotNull(p0);
    Assert.assertNotNull(p0.getPropertyContent());
    MMetastoreDBProperties p1 = objectStore.fetchProperties("key", (p)->{ p.getPropertyContent(); return p; });
    Assert.assertNotNull(p1);
    Assert.assertEquals(p0.getPropertykey(), p1.getPropertykey());
    Assert.assertEquals(p0.getPropertyValue(), p1.getPropertyValue());
    Assert.assertNotNull(p1.getPropertyContent());
    Assert.assertTrue(p0.getPropertyContent() != p1.getPropertyContent());
    String cp0 = new String(p0.getPropertyContent(), StandardCharsets.UTF_8);
    String cp1 = new String(p1.getPropertyContent(), StandardCharsets.UTF_8);
    Assert.assertEquals(cp0, cp1);

    Assert.assertFalse(objectStore.renameProperties("yek", "KEY"));
    Assert.assertFalse(objectStore.renameProperties("yek", "key"));
    boolean b = objectStore.renameProperties("key", "KEY");
    Assert.assertTrue(b);
    p1 = objectStore.fetchProperties("key", (p)->{ p.getPropertyContent(); return p; });
    Assert.assertNull(p1);
    p1 = objectStore.fetchProperties("KEY", (p)->{ p.getPropertyContent(); return p; });
    Assert.assertNotNull(p1);
    Assert.assertTrue(p0.getPropertyContent() != p1.getPropertyContent());
    cp1 = new String(p1.getPropertyContent(), StandardCharsets.UTF_8);
    Assert.assertEquals(cp0, cp1);
  }

}
