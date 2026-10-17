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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.security.authorization.plugin.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.ql.security.authorization.plugin.fallback.FallbackHiveAuthorizerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestHiveMetaStoreAuthorizerFilterDatabases {

  private Configuration conf;
  private int metastorePort = -1;
  private HiveMetaStoreClient client;

  @Before
  public void setUp() throws Exception {
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, ConfVars.HIVE_AUTHORIZATION_MANAGER, FallbackHiveAuthorizerFactory.class.getName());
    MetastoreConf.setVar(conf, ConfVars.FILTER_HOOK, HiveMetaStoreAuthorizer.class.getName());
    MetastoreConf.setBoolVar(conf, ConfVars.METASTORE_CLIENT_FILTER_ENABLED, true);
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    metastorePort = MetaStoreTestUtils.startMetaStoreWithRetry(conf);
    client = new HiveMetaStoreClient(conf);
  }

  @After
  public void tearDown() {
    if (client != null) {
      client.close();
    }
    if (metastorePort > 0) {
      MetaStoreTestUtils.close(metastorePort);
    }
  }

  @Test
  public void testGetAllDatabasesReturnsNamesWithFilterHook() throws Exception {
    List<String> databases = client.getAllDatabases();
    assertFalse("getAllDatabases returned empty list", databases.isEmpty());
    for (String dbName : databases) {
      assertNotNull("getAllDatabases returned null DB name", dbName);
    }
    assertTrue("default database not present in DB list", databases.contains("default"));
  }
}
