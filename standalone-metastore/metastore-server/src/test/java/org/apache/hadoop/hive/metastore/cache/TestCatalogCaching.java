/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.cache;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HMSHandler;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.client.builder.CatalogBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Comparator;
import java.util.List;

/**
 * Tests that catalogs are properly cached.
 */
@Category(MetastoreCheckinTest.class)
public class TestCatalogCaching {
  private static final String CAT1_NAME = "cat1";
  private static final String CAT2_NAME = "cat2";

  private ObjectStore objectStore;
  private Configuration conf;
  private CachedStore cachedStore;

  @Before
  public void createObjectStore() throws MetaException, InvalidOperationException {
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    objectStore = new ObjectStore();
    objectStore.setConf(conf);

    // Create three catalogs
    HMSHandler.createDefaultCatalog(objectStore, new Warehouse(conf));

    Catalog cat1 = new CatalogBuilder()
        .setName(CAT1_NAME)
        .setLocation("/tmp/cat1")
        .build();
    objectStore.createCatalog(cat1);
    Catalog cat2 = new CatalogBuilder()
        .setName(CAT2_NAME)
        .setLocation("/tmp/cat2")
        .build();
    objectStore.createCatalog(cat2);
  }

  @After
  public void clearCatalogCache() throws MetaException, NoSuchObjectException {
    List<String> catalogs = objectStore.getCatalogs();
    for (String catalog : catalogs) objectStore.dropCatalog(catalog);
  }

  @Test
  public void defaultHiveOnly() throws Exception {
    // By default just the Hive catalog should be cached.
    cachedStore = new CachedStore();
    cachedStore.setConf(conf);
    CachedStore.stopCacheUpdateService(1);
    cachedStore.resetCatalogCache();

    CachedStore.prewarm(objectStore);

    // Only the hive catalog should be cached
    List<String> cachedCatalogs = cachedStore.getCatalogs();
    Assert.assertEquals(1, cachedCatalogs.size());
    Assert.assertEquals(Warehouse.DEFAULT_CATALOG_NAME, cachedCatalogs.get(0));
  }

  @Test
  public void cacheAll() throws Exception {
    // Set the config value to empty string, which should result in all catalogs being cached.
    Configuration newConf = new Configuration(conf);
    MetastoreConf.setVar(newConf, MetastoreConf.ConfVars.CATALOGS_TO_CACHE, "");
    cachedStore = new CachedStore();
    cachedStore.setConf(newConf);
    CachedStore.stopCacheUpdateService(1);
    objectStore.setConf(newConf); // have to override it with the new conf since this is where
                                  // prewarm gets the conf object
    cachedStore.resetCatalogCache();

    CachedStore.prewarm(objectStore);

    // All the catalogs should be cached
    List<String> cachedCatalogs = cachedStore.getCatalogs();
    Assert.assertEquals(3, cachedCatalogs.size());
    cachedCatalogs.sort(Comparator.naturalOrder());
    Assert.assertEquals(CAT1_NAME, cachedCatalogs.get(0));
    Assert.assertEquals(CAT2_NAME, cachedCatalogs.get(1));
    Assert.assertEquals(Warehouse.DEFAULT_CATALOG_NAME, cachedCatalogs.get(2));
  }

  @Test
  public void cacheSome() throws Exception {
    // Set the config value to 2 catalogs other than hive
    Configuration newConf = new Configuration(conf);
    MetastoreConf.setVar(newConf, MetastoreConf.ConfVars.CATALOGS_TO_CACHE, CAT1_NAME + "," + CAT2_NAME);
    cachedStore = new CachedStore();
    cachedStore.setConf(newConf);
    CachedStore.stopCacheUpdateService(1);
    objectStore.setConf(newConf); // have to override it with the new conf since this is where
                                  // prewarm gets the conf object
    cachedStore.resetCatalogCache();

    CachedStore.prewarm(objectStore);

    // All the catalogs should be cached
    List<String> cachedCatalogs = cachedStore.getCatalogs();
    Assert.assertEquals(2, cachedCatalogs.size());
    cachedCatalogs.sort(Comparator.naturalOrder());
    Assert.assertEquals(CAT1_NAME, cachedCatalogs.get(0));
    Assert.assertEquals(CAT2_NAME, cachedCatalogs.get(1));
  }
}
