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
package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.client.builder.CatalogBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.thrift.TException;
import org.junit.After;

/**
 * This tests metastore client calls that do not specify a catalog but with the config on the
 * client set to go to a non-default catalog.
 */
public class TestCatalogNonDefaultClient extends NonCatCallsWithCatalog {

  final private String catName = "non_default_catalog";
  private String catLocation;

  @After
  public void dropCatalog() throws TException {
    MetaStoreTestUtils.dropCatalogCascade(client, catName);
  }

  @Override
  protected IMetaStoreClient getClient() throws Exception {

    Configuration svrConf = new Configuration(conf);
    int port = MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(),
        svrConf, false, false, false, false, false);
    // Only set the default catalog on the client.
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_URIS, "thrift://localhost:" + port);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CATALOG_DEFAULT, catName);
    IMetaStoreClient client = new HiveMetaStoreClient(conf);
    assert !client.isLocalMetaStore();
    // Don't make any calls but catalog calls until the catalog has been created, as we just told
    // the client to direct all calls to a catalog that does not yet exist.
    catLocation = MetaStoreTestUtils.getTestWarehouseDir(catName);
    Catalog cat = new CatalogBuilder()
        .setName(catName)
        .setLocation(catLocation)
        .build();
    client.createCatalog(cat);
    return client;
  }

  @Override
  protected String expectedCatalog() {
    return catName;
  }

  @Override
  protected String expectedBaseDir() throws MetaException {
    return catLocation;
  }

  @Override
  protected String expectedExtBaseDir() throws MetaException {
    return catLocation;
  }
}
