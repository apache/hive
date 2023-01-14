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

package org.apache.hadoop.hive.metastore.client;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

/**
 * Test class for IMetaStoreClient API. Testing the Table related functions for metadata
 * manipulation, like creating, dropping and altering tables.
 */
@RunWith(Parameterized.class)
public class TestTablesCreateDropAlterTruncate {
  private static final Logger LOG = LoggerFactory.getLogger(TestTablesCreateDropAlterTruncate.class);
  // Needed until there is no junit release with @BeforeParam, @AfterParam (junit 4.13)
  // https://github.com/junit-team/junit4/commit/1bf8438b65858565dbb64736bfe13aae9cfc1b5a
  // Then we should remove our own copy
  private static Set<AbstractMetaStoreService> metaStoreServices = null;
  private static final String DEFAULT_DATABASE = "default";
  private final AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;
  private Table[] testTables = new Table[6];

  @Parameterized.Parameters(name = "{0}")
  public static List<Object[]> getMetaStoreToTest() throws Exception {
    List<Object[]> result = MetaStoreFactoryForTests.getMetaStores();
    metaStoreServices = result.stream()
            .map(test -> (AbstractMetaStoreService) test[1])
            .collect(Collectors.toSet());
    return result;
  }

  public TestTablesCreateDropAlterTruncate(String name, AbstractMetaStoreService metaStore) throws Exception {
    this.metaStore = metaStore;
    this.metaStore.start();
  }

  // Needed until there is no junit release with @BeforeParam, @AfterParam (junit 4.13)
  // https://github.com/junit-team/junit4/commit/1bf8438b65858565dbb64736bfe13aae9cfc1b5a
  // Then we should move this to @AfterParam
  @AfterClass
  public static void stopMetaStores() throws Exception {
    for (AbstractMetaStoreService metaStoreService : metaStoreServices) {
      try {
        metaStoreService.stop();
      } catch (Exception e) {
        // Catch the exceptions, so every other metastore could be stopped as well
        // Log it, so at least there is a slight possibility we find out about this :)
        LOG.error("Error stopping MetaStoreService", e);
      }
    }
  }

  @Before
  public void setUp() throws Exception {
    // Get new client
    client = metaStore.getClient();

    // Drop every table in the default database
    for (String tableName : client.getAllTables(DEFAULT_DATABASE)) {
      client.dropTable(DEFAULT_DATABASE, tableName, true, true, true);
    }

    // Clean up trash
    metaStore.cleanWarehouseDirs();

    testTables[0] =
            new TableBuilder()
                    .setDbName(DEFAULT_DATABASE)
                    .setTableName("test_table")
                    .addCol("test_col", "int")
                    .build();

    client.createTable(testTables[0]);
    testTables[0] = client.getTable(testTables[0].getDbName(), testTables[0].getTableName());
  }

  @After
  public void tearDown() throws Exception {
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
  public void testAlterTableExpectedPropertyMatch() throws Exception {
    Table originalTable = testTables[0];

    EnvironmentContext context = new EnvironmentContext();
    context.putToProperties(hive_metastoreConstants.EXPECTED_PARAMETER_KEY, "transient_lastDdlTime");
    context.putToProperties(hive_metastoreConstants.EXPECTED_PARAMETER_VALUE,
            originalTable.getParameters().get("transient_lastDdlTime"));

    client.alter_table_with_environmentContext(originalTable.getDbName(), originalTable.getTableName(),
            originalTable, context);
  }

  @Test(expected = MetaException.class)
  public void testAlterTableExpectedPropertyDifferent() throws Exception {
    Table originalTable = testTables[0];

    EnvironmentContext context = new EnvironmentContext();
    context.putToProperties(hive_metastoreConstants.EXPECTED_PARAMETER_KEY, "transient_lastDdlTime");
    context.putToProperties(hive_metastoreConstants.EXPECTED_PARAMETER_VALUE, "alma");

    client.alter_table_with_environmentContext(originalTable.getDbName(), originalTable.getTableName(),
            originalTable, context);
  }

  /**
   * This tests ensures that concurrent Iceberg commits will fail. Acceptable as a first sanity check.
   * <p>
   * I have not found a good way to check that HMS side database commits are parallel in the
   * {@link org.apache.hadoop.hive.metastore.HiveAlterHandler#alterTable(RawStore, Warehouse, String, String, Table, EnvironmentContext)}
   * call, but this test could be used to manually ensure that using breakpoints.
   */
  @Test
  public void testAlterTableExpectedPropertyConcurrent() throws Exception {
    Table originalTable = testTables[0];

    originalTable.getParameters().put("snapshot", "0");
    client.alter_table_with_environmentContext(originalTable.getDbName(), originalTable.getTableName(),
            originalTable, null);

    ExecutorService threads = null;
    try {
      threads = Executors.newFixedThreadPool(2);
      for (int i = 0; i < 3; i++) {
        EnvironmentContext context = new EnvironmentContext();
        context.putToProperties(hive_metastoreConstants.EXPECTED_PARAMETER_KEY, "snapshot");
        context.putToProperties(hive_metastoreConstants.EXPECTED_PARAMETER_VALUE, String.valueOf(i));

        Table newTable = originalTable.deepCopy();
        newTable.getParameters().put("snapshot", String.valueOf(i + 1));

        IMetaStoreClient client1 = metaStore.getClient();
        IMetaStoreClient client2 = metaStore.getClient();

        Collection<Callable<Boolean>> concurrentTasks = new ArrayList<>(2);
        concurrentTasks.add(alterTask(client1, newTable, context));
        concurrentTasks.add(alterTask(client2, newTable, context));

        Collection<Future<Boolean>> results = threads.invokeAll(concurrentTasks);

        boolean foundSuccess = false;
        boolean foundFailure = false;

        for (Future<Boolean> result : results) {
          if (result.get()) {
            foundSuccess = true;
          } else {
            foundFailure = true;
          }
        }

        assertTrue("At least one success is expected", foundSuccess);
        assertTrue("At least one failure is expected", foundFailure);
      }
    } finally {
      if (threads != null) {
        threads.shutdown();
      }
    }
  }

  private Callable<Boolean> alterTask(IMetaStoreClient hmsClient, Table newTable, EnvironmentContext context) {
    return () -> {
      try {
        hmsClient.alter_table_with_environmentContext(newTable.getDbName(), newTable.getTableName(),
                newTable, context);
      } catch (Throwable e) {
        return false;
      }
      return true;
    };
  }
}