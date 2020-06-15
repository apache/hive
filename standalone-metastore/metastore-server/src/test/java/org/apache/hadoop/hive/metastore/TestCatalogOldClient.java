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

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;

import org.apache.hadoop.hive.metastore.api.MetaException;

/**
 * This tests calls with an older client, to make sure that if the client supplies no catalog
 * information the server still does the right thing.  I assumes the default catalog
 */
public class TestCatalogOldClient extends NonCatCallsWithCatalog {

  @Override
  protected IMetaStoreClient getClient() throws MetaException {
    return new HiveMetaStoreClientPreCatalog(conf);
  }

  @Override
  protected String expectedCatalog() {
    return DEFAULT_CATALOG_NAME;
  }

  @Override
  protected String expectedBaseDir() throws MetaException {
    return new Warehouse(conf).getWhRoot().toUri().getPath();
  }

  @Override
  protected String expectedExtBaseDir() throws MetaException {
    return new Warehouse(conf).getWhRootExternal().toUri().getPath();
  }
}
