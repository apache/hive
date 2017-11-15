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
package org.apache.hadoop.hive.metastore;


import com.codahale.metrics.Counter;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.metastore.ObjectStore.RetryingExecutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jdo.Query;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TestObjectStore {
  private ObjectStore objectStore = null;

  private static final String DB1 = "testobjectstoredb1";
  private static final String DB2 = "testobjectstoredb2";
  private static final String TABLE1 = "testobjectstoretable1";
  private static final String KEY1 = "testobjectstorekey1";
  private static final String KEY2 = "testobjectstorekey2";
  private static final String OWNER = "testobjectstoreowner";
  private static final String USER1 = "testobjectstoreuser1";
  private static final String ROLE1 = "testobjectstorerole1";
  private static final String ROLE2 = "testobjectstorerole2";
  private static final Logger LOG = LoggerFactory.getLogger(TestObjectStore.class.getName());

  private static final class LongSupplier implements Supplier<Long> {
    public long value = 0;

    @Override
    public Long get() {
      return value;
    }
  }

  public static class MockPartitionExpressionProxy implements PartitionExpressionProxy {
    @Override
    public String convertExprToFilter(byte[] expr) throws MetaException {
      return null;
    }

    @Override
    public boolean filterPartitionsByExpr(List<FieldSchema> partColumns,
                                          byte[] expr, String defaultPartitionName,
                                          List<String> partitionNames)
        throws MetaException {
      return false;
    }

    @Override
    public FileMetadataExprType getMetadataType(String inputFormat) {
      return null;
    }

    @Override
    public SearchArgument createSarg(byte[] expr) {
      return null;
    }

    @Override
    public FileFormatProxy getFileFormatProxy(FileMetadataExprType type) {
      return null;
    }
  }

  @Before
  public void setUp() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.EXPRESSION_PROXY_CLASS,
        MockPartitionExpressionProxy.class.getName());

    objectStore = new ObjectStore();
    objectStore.setConf(conf);
    dropAllStoreObjects(objectStore);
  }

  /**
   * Test database operations
   */
  @Test
  public void testDatabaseOps() throws MetaException, InvalidObjectException,
      NoSuchObjectException {
    Database db1 = new Database(DB1, "description", "locationurl", null);
    Database db2 = new Database(DB2, "description", "locationurl", null);
    objectStore.createDatabase(db1);
    objectStore.createDatabase(db2);

    List<String> databases = objectStore.getAllDatabases();
    LOG.info("databases: " + databases);
    Assert.assertEquals(2, databases.size());
    Assert.assertEquals(DB1, databases.get(0));
    Assert.assertEquals(DB2, databases.get(1));

    objectStore.dropDatabase(DB1);
    databases = objectStore.getAllDatabases();
    Assert.assertEquals(1, databases.size());
    Assert.assertEquals(DB2, databases.get(0));

    objectStore.dropDatabase(DB2);
  }

  /**
   * Test table operations
   */
  @Test
  public void testTableOps() throws MetaException, InvalidObjectException, NoSuchObjectException,
      InvalidInputException {
    Database db1 = new Database(DB1, "description", "locationurl", null);
    objectStore.createDatabase(db1);
    StorageDescriptor sd1 =
        new StorageDescriptor(ImmutableList.of(new FieldSchema("pk_col", "double", null)),
            "location", null, null, false, 0, new SerDeInfo("SerDeName", "serializationLib", null),
            null, null, null);
    HashMap<String, String> params = new HashMap<>();
    params.put("EXTERNAL", "false");
    Table tbl1 =
        new Table(TABLE1, DB1, "owner", 1, 2, 3, sd1, null, params, null, null, "MANAGED_TABLE");
    objectStore.createTable(tbl1);

    List<String> tables = objectStore.getAllTables(DB1);
    Assert.assertEquals(1, tables.size());
    Assert.assertEquals(TABLE1, tables.get(0));

    StorageDescriptor sd2 =
        new StorageDescriptor(ImmutableList.of(new FieldSchema("fk_col", "double", null)),
            "location", null, null, false, 0, new SerDeInfo("SerDeName", "serializationLib", null),
            null, null, null);
    Table newTbl1 = new Table("new" + TABLE1, DB1, "owner", 1, 2, 3, sd2, null, params, null, null,
        "MANAGED_TABLE");
    objectStore.alterTable(DB1, TABLE1, newTbl1);
    tables = objectStore.getTables(DB1, "new*");
    Assert.assertEquals(1, tables.size());
    Assert.assertEquals("new" + TABLE1, tables.get(0));

    objectStore.createTable(tbl1);
    tables = objectStore.getAllTables(DB1);
    Assert.assertEquals(2, tables.size());

    List<SQLForeignKey> foreignKeys = objectStore.getForeignKeys(DB1, TABLE1, null, null);
    Assert.assertEquals(0, foreignKeys.size());

    SQLPrimaryKey pk = new SQLPrimaryKey(DB1, TABLE1, "pk_col", 1,
        "pk_const_1", false, false, false);
    objectStore.addPrimaryKeys(ImmutableList.of(pk));
    SQLForeignKey fk = new SQLForeignKey(DB1, TABLE1, "pk_col",
        DB1, "new" + TABLE1, "fk_col", 1,
        0, 0, "fk_const_1", "pk_const_1", false, false, false);
    objectStore.addForeignKeys(ImmutableList.of(fk));

    // Retrieve from PK side
    foreignKeys = objectStore.getForeignKeys(null, null, DB1, "new" + TABLE1);
    Assert.assertEquals(1, foreignKeys.size());

    List<SQLForeignKey> fks = objectStore.getForeignKeys(null, null, DB1, "new" + TABLE1);
    if (fks != null) {
      for (SQLForeignKey fkcol : fks) {
        objectStore.dropConstraint(fkcol.getFktable_db(), fkcol.getFktable_name(),
            fkcol.getFk_name());
      }
    }
    // Retrieve from FK side
    foreignKeys = objectStore.getForeignKeys(DB1, TABLE1, null, null);
    Assert.assertEquals(0, foreignKeys.size());
    // Retrieve from PK side
    foreignKeys = objectStore.getForeignKeys(null, null, DB1, "new" + TABLE1);
    Assert.assertEquals(0, foreignKeys.size());

    objectStore.dropTable(DB1, TABLE1);
    tables = objectStore.getAllTables(DB1);
    Assert.assertEquals(1, tables.size());

    objectStore.dropTable(DB1, "new" + TABLE1);
    tables = objectStore.getAllTables(DB1);
    Assert.assertEquals(0, tables.size());

    objectStore.dropDatabase(DB1);
  }

  private StorageDescriptor createFakeSd(String location) {
    return new StorageDescriptor(null, location, null, null, false, 0,
        new SerDeInfo("SerDeName", "serializationLib", null), null, null, null);
  }


  /**
   * Tests partition operations
   */
  @Test
  public void testPartitionOps() throws MetaException, InvalidObjectException,
      NoSuchObjectException, InvalidInputException {
    Database db1 = new Database(DB1, "description", "locationurl", null);
    objectStore.createDatabase(db1);
    StorageDescriptor sd = createFakeSd("location");
    HashMap<String, String> tableParams = new HashMap<>();
    tableParams.put("EXTERNAL", "false");
    FieldSchema partitionKey1 = new FieldSchema("Country", ColumnType.STRING_TYPE_NAME, "");
    FieldSchema partitionKey2 = new FieldSchema("State", ColumnType.STRING_TYPE_NAME, "");
    Table tbl1 =
        new Table(TABLE1, DB1, "owner", 1, 2, 3, sd, Arrays.asList(partitionKey1, partitionKey2),
            tableParams, null, null, "MANAGED_TABLE");
    objectStore.createTable(tbl1);
    HashMap<String, String> partitionParams = new HashMap<>();
    partitionParams.put("PARTITION_LEVEL_PRIVILEGE", "true");
    List<String> value1 = Arrays.asList("US", "CA");
    Partition part1 = new Partition(value1, DB1, TABLE1, 111, 111, sd, partitionParams);
    objectStore.addPartition(part1);
    List<String> value2 = Arrays.asList("US", "MA");
    Partition part2 = new Partition(value2, DB1, TABLE1, 222, 222, sd, partitionParams);
    objectStore.addPartition(part2);

    Deadline.startTimer("getPartition");
    List<Partition> partitions = objectStore.getPartitions(DB1, TABLE1, 10);
    Assert.assertEquals(2, partitions.size());
    Assert.assertEquals(111, partitions.get(0).getCreateTime());
    Assert.assertEquals(222, partitions.get(1).getCreateTime());

    int numPartitions = objectStore.getNumPartitionsByFilter(DB1, TABLE1, "");
    Assert.assertEquals(partitions.size(), numPartitions);

    numPartitions = objectStore.getNumPartitionsByFilter(DB1, TABLE1, "country = \"US\"");
    Assert.assertEquals(2, numPartitions);

    objectStore.dropPartition(DB1, TABLE1, value1);
    partitions = objectStore.getPartitions(DB1, TABLE1, 10);
    Assert.assertEquals(1, partitions.size());
    Assert.assertEquals(222, partitions.get(0).getCreateTime());

    objectStore.dropPartition(DB1, TABLE1, value2);
    objectStore.dropTable(DB1, TABLE1);
    objectStore.dropDatabase(DB1);
  }

  /**
   * Test master keys operation
   */
  @Test
  public void testMasterKeyOps() throws MetaException, NoSuchObjectException {
    int id1 = objectStore.addMasterKey(KEY1);
    int id2 = objectStore.addMasterKey(KEY2);

    String[] keys = objectStore.getMasterKeys();
    Assert.assertEquals(2, keys.length);
    Assert.assertEquals(KEY1, keys[0]);
    Assert.assertEquals(KEY2, keys[1]);

    objectStore.updateMasterKey(id1, "new" + KEY1);
    objectStore.updateMasterKey(id2, "new" + KEY2);
    keys = objectStore.getMasterKeys();
    Assert.assertEquals(2, keys.length);
    Assert.assertEquals("new" + KEY1, keys[0]);
    Assert.assertEquals("new" + KEY2, keys[1]);

    objectStore.removeMasterKey(id1);
    keys = objectStore.getMasterKeys();
    Assert.assertEquals(1, keys.length);
    Assert.assertEquals("new" + KEY2, keys[0]);

    objectStore.removeMasterKey(id2);
  }

  /**
   * Test role operation
   */
  @Test
  public void testRoleOps() throws InvalidObjectException, MetaException, NoSuchObjectException {
    objectStore.addRole(ROLE1, OWNER);
    objectStore.addRole(ROLE2, OWNER);
    List<String> roles = objectStore.listRoleNames();
    Assert.assertEquals(2, roles.size());
    Assert.assertEquals(ROLE2, roles.get(1));
    Role role1 = objectStore.getRole(ROLE1);
    Assert.assertEquals(OWNER, role1.getOwnerName());
    objectStore.grantRole(role1, USER1, PrincipalType.USER, OWNER, PrincipalType.ROLE, true);
    objectStore.revokeRole(role1, USER1, PrincipalType.USER, false);
    objectStore.removeRole(ROLE1);
  }

  @Test
  public void testDirectSqlErrorMetrics() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.METRICS_ENABLED, true);
    Metrics.initialize(conf);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.HIVE_CODAHALE_METRICS_REPORTER_CLASSES,
        "org.apache.hadoop.hive.common.metrics.metrics2.JsonFileMetricsReporter, " +
            "org.apache.hadoop.hive.common.metrics.metrics2.JmxMetricsReporter"
    );

    // recall setup so that we get an object store with the metrics initalized
    setUp();
    Counter directSqlErrors =
        Metrics.getRegistry().getCounters().get(MetricsConstants.DIRECTSQL_ERRORS);

    objectStore.new GetDbHelper("foo", true, true) {
      @Override
      protected Database getSqlResult(ObjectStore.GetHelper<Database> ctx) throws MetaException {
        return null;
      }

      @Override
      protected Database getJdoResult(ObjectStore.GetHelper<Database> ctx) throws MetaException,
          NoSuchObjectException {
        return null;
      }
    }.run(false);

    Assert.assertEquals(0, directSqlErrors.getCount());

    objectStore.new GetDbHelper("foo", true, true) {
      @Override
      protected Database getSqlResult(ObjectStore.GetHelper<Database> ctx) throws MetaException {
        throw new RuntimeException();
      }

      @Override
      protected Database getJdoResult(ObjectStore.GetHelper<Database> ctx) throws MetaException,
          NoSuchObjectException {
        return null;
      }
    }.run(false);

    Assert.assertEquals(1, directSqlErrors.getCount());
  }

  private static void dropAllStoreObjects(RawStore store)
      throws MetaException, InvalidObjectException, InvalidInputException {
    try {
      Deadline.registerIfNot(100000);
      List<Function> functions = store.getAllFunctions();
      for (Function func : functions) {
        store.dropFunction(func.getDbName(), func.getFunctionName());
      }
      List<String> dbs = store.getAllDatabases();
      for (String db : dbs) {
        List<String> tbls = store.getAllTables(db);
        for (String tbl : tbls) {
          List<Index> indexes = store.getIndexes(db, tbl, 100);
          for (Index index : indexes) {
            store.dropIndex(db, tbl, index.getIndexName());
          }
        }
        for (String tbl : tbls) {
          Deadline.startTimer("getPartition");
          List<Partition> parts = store.getPartitions(db, tbl, 100);
          for (Partition part : parts) {
            store.dropPartition(db, tbl, part.getValues());
          }
          // Find any constraints and drop them
          Set<String> constraints = new HashSet<>();
          List<SQLPrimaryKey> pk = store.getPrimaryKeys(db, tbl);
          if (pk != null) {
            for (SQLPrimaryKey pkcol : pk) {
              constraints.add(pkcol.getPk_name());
            }
          }
          List<SQLForeignKey> fks = store.getForeignKeys(null, null, db, tbl);
          if (fks != null) {
            for (SQLForeignKey fkcol : fks) {
              constraints.add(fkcol.getFk_name());
            }
          }
          for (String constraint : constraints) {
            store.dropConstraint(db, tbl, constraint);
          }
          store.dropTable(db, tbl);
        }
        store.dropDatabase(db);
      }
      List<String> roles = store.listRoleNames();
      for (String role : roles) {
        store.removeRole(role);
      }
    } catch (NoSuchObjectException e) {
    }
  }

  @Test
  public void testQueryCloseOnError() throws Exception {
    ObjectStore spy = Mockito.spy(objectStore);
    spy.getAllDatabases();
    spy.getAllFunctions();
    spy.getAllTables(DB1);
    spy.getPartitionCount();
    Mockito.verify(spy, Mockito.times(2))
        .rollbackAndCleanup(Mockito.anyBoolean(), Mockito.<Query>anyObject());
  }

  @Test
  public void testRetryingExecutorSleep() throws Exception {
    RetryingExecutor re = new ObjectStore.RetryingExecutor(MetastoreConf.newMetastoreConf(), null);
    Assert.assertTrue("invalid sleep value", re.getSleepInterval() >= 0);
  }

  @Ignore // See comment in ObjectStore.getDataSourceProps
  @Test
  public void testNonConfDatanucleusValueSet() {
    String key = "datanucleus.no.such.key";
    String value = "test_value";
    String key1 = "blabla.no.such.key";
    String value1 = "another_value";
    Assume.assumeTrue(System.getProperty(key) == null);
    Configuration localConf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(localConf, MetastoreConf.ConfVars.EXPRESSION_PROXY_CLASS,
        MockPartitionExpressionProxy.class.getName());
    localConf.set(key, value);
    localConf.set(key1, value1);
    objectStore = new ObjectStore();
    objectStore.setConf(localConf);
    Assert.assertEquals(value, objectStore.getProp().getProperty(key));
    Assert.assertNull(objectStore.getProp().getProperty(key1));
  }
}

