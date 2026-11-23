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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.fallback.FallbackHiveAuthorizerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
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

  public static class MockRangerAuthorizerFactory implements HiveAuthorizerFactory {
    @Override
    public HiveAuthorizer createHiveAuthorizer(HiveMetastoreClientFactory metastoreClientFactory,
        HiveConf conf, HiveAuthenticationProvider authenticator, HiveAuthzSessionContext ctx) {
      HiveAuthorizer authorizer = Mockito.mock(HiveAuthorizer.class);
      try {
        Mockito.when(authorizer.filterListCmdObjects(Mockito.anyList(), Mockito.any()))
            .thenAnswer(invocation -> {
              List<HivePrivilegeObject> databases = invocation.getArgument(0);
              for (HivePrivilegeObject database : databases) {
                // Simulating Ranger resolving each database's owner when not already supplied which 
                // causes a StackOverflowError due to re-entering the filter hook through filterDatabase.
                if (database.getOwnerName() == null) {
                  Database fetchedDB = new Database(database.getDbname(), null, null, null);
                  fetchedDB.setOwnerName("test_user");
                  fetchedDB.setOwnerType(PrincipalType.ROLE);
                  // Simulating filterDatabase call by the client side filter on the fetched DB from the metastore
                  new HiveMetaStoreAuthorizer(conf).filterDatabase(fetchedDB);
                }
              }
              return databases;
            });
      } catch (Exception e) {
        throw new RuntimeException("Failed to set up Mock Ranger Authorizer", e);
      }
      return authorizer;
    }
  }

  @Test(timeout = 60000)
  public void testFilterDatabasesDoesNotStackOverflowWithRanger() throws Exception {
    Configuration authzConf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(authzConf, ConfVars.HIVE_AUTHORIZATION_MANAGER, MockRangerAuthorizerFactory.class.getName());
    
    HiveMetaStoreAuthorizer authorizer = new HiveMetaStoreAuthorizer(authzConf);
    List<String> result = authorizer.filterDatabases("hive", Collections.singletonList("test_db"));
    assertNotNull("filterDatabases returned null", result);
    assertTrue("expected test_db in filtered databases", result.contains("test_db"));
  }
}
