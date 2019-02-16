package org.apache.hadoop.hive.metastore.client;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.client.builder.CatalogBuilder;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;

/**
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
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestCatalogs extends MetaStoreClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestCatalogs.class);
  private final AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;

  public TestCatalogs(String name, AbstractMetaStoreService metaStore) throws Exception {
    this.metaStore = metaStore;
  }

  @Before
  public void setUp() throws Exception {
    // Get new client
    client = metaStore.getClient();

  }

  @After
  public void tearDown() throws Exception {
    // Drop any left over catalogs
    List<String> catalogs = client.getCatalogs();
    for (String catName : catalogs) {
      if (!catName.equalsIgnoreCase(Warehouse.DEFAULT_CATALOG_NAME)) {
        // First drop any databases in catalog
        List<String> databases = client.getAllDatabases(catName);
        for (String db : databases) {
          client.dropDatabase(catName, db, true, false, true);
        }
        client.dropCatalog(catName);
      } else {
        List<String> databases = client.getAllDatabases(catName);
        for (String db : databases) {
          if (!db.equalsIgnoreCase(DEFAULT_DATABASE_NAME)) {
            client.dropDatabase(catName, db, true, false, true);
          }
        }

      }
    }
    try {
      if (client != null) {
        try {
          client.close();
        } catch (Exception e) {
          // HIVE-19729: Shallow the exceptions based on the discussion in the Jira
        }
      }
    } finally {
      client = null;
    }
  }

  @Test
  public void catalogOperations() throws TException {
    String[] catNames = {"cat1", "cat2", "ADifferentName"};
    String[] description = {"a description", "super descriptive", null};
    String[] location = {MetaStoreTestUtils.getTestWarehouseDir("cat1"),
                         MetaStoreTestUtils.getTestWarehouseDir("cat2"),
                         MetaStoreTestUtils.getTestWarehouseDir("different")};

    for (int i = 0; i < catNames.length; i++) {
      Catalog cat = new CatalogBuilder()
          .setName(catNames[i])
          .setLocation(location[i])
          .setDescription(description[i])
          .build();
      client.createCatalog(cat);
      File dir = new File(cat.getLocationUri());
      Assert.assertTrue(dir.exists() && dir.isDirectory());
    }

    for (int i = 0; i < catNames.length; i++) {
      Catalog cat = client.getCatalog(catNames[i]);
      Assert.assertTrue(catNames[i].equalsIgnoreCase(cat.getName()));
      Assert.assertEquals(description[i], cat.getDescription());
      Assert.assertEquals(location[i], cat.getLocationUri());
      Assert.assertTrue("Create time of catalog should be set", cat.isSetCreateTime());
      Assert.assertTrue("Create time of catalog should be non-zero", cat.getCreateTime() > 0);
      File dir = new File(cat.getLocationUri());
      Assert.assertTrue(dir.exists() && dir.isDirectory());

      // Make sure there's a default database associated with each catalog
      Database db = client.getDatabase(catNames[i], DEFAULT_DATABASE_NAME);
      Assert.assertEquals("file:" + cat.getLocationUri(), db.getLocationUri());
    }

    List<String> catalogs = client.getCatalogs();
    Assert.assertEquals(4, catalogs.size());
    catalogs.sort(Comparator.naturalOrder());
    List<String> expected = new ArrayList<>(catNames.length + 1);
    expected.add(Warehouse.DEFAULT_CATALOG_NAME);
    expected.addAll(Arrays.asList(catNames));
    expected.sort(Comparator.naturalOrder());
    for (int i = 0; i < catalogs.size(); i++) {
      Assert.assertTrue("Expected " + expected.get(i) + " actual " + catalogs.get(i),
          catalogs.get(i).equalsIgnoreCase(expected.get(i)));
    }


    // Update catalogs
    // Update location
    Catalog newCat = new Catalog(client.getCatalog(catNames[0]));
    String newLocation = MetaStoreTestUtils.getTestWarehouseDir("a_different_location");
    newCat.setLocationUri(newLocation);
    client.alterCatalog(catNames[0], newCat);
    Catalog fetchedNewCat = client.getCatalog(catNames[0]);
    Assert.assertEquals(newLocation, fetchedNewCat.getLocationUri());
    Assert.assertEquals(description[0], fetchedNewCat.getDescription());

    // Update description
    newCat = new Catalog(client.getCatalog(catNames[1]));
    String newDescription = "an even more descriptive description";
    newCat.setDescription(newDescription);
    client.alterCatalog(catNames[1], newCat);
    fetchedNewCat = client.getCatalog(catNames[1]);
    Assert.assertEquals(location[1], fetchedNewCat.getLocationUri());
    Assert.assertEquals(newDescription, fetchedNewCat.getDescription());

    for (int i = 0; i < catNames.length; i++) {
      client.dropCatalog(catNames[i]);
      File dir = new File(location[i]);
      Assert.assertFalse(dir.exists());
    }

    catalogs = client.getCatalogs();
    Assert.assertEquals(1, catalogs.size());
    Assert.assertTrue(catalogs.get(0).equalsIgnoreCase(Warehouse.DEFAULT_CATALOG_NAME));
  }

  @Test(expected = NoSuchObjectException.class)
  public void getNonExistentCatalog() throws TException {
    client.getCatalog("noSuchCatalog");
  }

  @Test(expected = MetaException.class)
  @Ignore // TODO This test passes fine locally but fails on Linux, not sure why
  public void createCatalogWithBadLocation() throws TException {
    Catalog cat = new CatalogBuilder()
        .setName("goodluck")
        .setLocation("/nosuch/nosuch")
        .build();
    client.createCatalog(cat);
  }

  @Test(expected = NoSuchObjectException.class)
  public void dropNonExistentCatalog() throws TException {
    client.dropCatalog("noSuchCatalog");
  }

  @Test(expected = MetaException.class)
  public void dropHiveCatalog() throws TException {
    client.dropCatalog(Warehouse.DEFAULT_CATALOG_NAME);
  }

  @Test(expected = InvalidOperationException.class)
  public void dropNonEmptyCatalog() throws TException {
    String catName = "toBeDropped";
    Catalog cat = new CatalogBuilder()
        .setName(catName)
        .setLocation(MetaStoreTestUtils.getTestWarehouseDir(catName))
        .build();
    client.createCatalog(cat);

    String dbName = "dontDropMe";
    new DatabaseBuilder()
        .setName(dbName)
        .setCatalogName(catName)
        .create(client, metaStore.getConf());

    client.dropCatalog(catName);
  }

  @Test(expected = InvalidOperationException.class)
  public void dropCatalogWithNonEmptyDefaultDb() throws TException {
    String catName = "toBeDropped2";
    new CatalogBuilder()
        .setName(catName)
        .setLocation(MetaStoreTestUtils.getTestWarehouseDir(catName))
        .create(client);

    new TableBuilder()
        .setTableName("not_droppable")
        .setCatName(catName)
        .addCol("cola1", "bigint")
        .create(client, metaStore.getConf());

    client.dropCatalog(catName);
  }

  @Test(expected = NoSuchObjectException.class)
  public void alterNonExistentCatalog() throws TException {
    String catName = "alter_no_such_catalog";
    Catalog cat = new CatalogBuilder()
        .setName(catName)
        .setLocation(MetaStoreTestUtils.getTestWarehouseDir(catName))
        .build();

    client.alterCatalog(catName, cat);
  }

  @Test(expected = InvalidOperationException.class)
  public void alterChangeName() throws TException {
    String catName = "alter_change_name";
    String location = MetaStoreTestUtils.getTestWarehouseDir(catName);
    String description = "I have a bad feeling about this";
    new CatalogBuilder()
        .setName(catName)
        .setLocation(location)
        .setDescription(description)
        .create(client);

    Catalog newCat = client.getCatalog(catName);
    newCat.setName("you_may_call_me_tim");
    client.alterCatalog(catName, newCat);
  }
}
