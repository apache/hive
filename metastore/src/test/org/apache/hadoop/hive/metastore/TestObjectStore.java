/**
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

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.metrics2.CodahaleMetrics;
import org.apache.hadoop.hive.common.metrics.metrics2.MetricsReporting;
import org.apache.hadoop.hive.common.metrics.MetricsTestUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
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
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.model.MTableWrite;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hive.common.util.MockFileSystem;
import org.apache.hive.common.util.MockFileSystem.MockFile;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;

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
    public boolean filterPartitionsByExpr(List<String> partColumnNames,
        List<PrimitiveTypeInfo> partColumnTypeInfos, byte[] expr,
        String defaultPartitionName, List<String> partitionNames)
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
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.METASTORE_EXPRESSION_PROXY_CLASS, MockPartitionExpressionProxy.class.getName());

    objectStore = new ObjectStore();
    objectStore.setConf(conf);
    dropAllStoreObjects(objectStore);
  }

  @After
  public void tearDown() {
  }

  /**
   * Test database operations
   */
  @Test
  public void testDatabaseOps() throws MetaException, InvalidObjectException, NoSuchObjectException {
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
  public void testTableOps() throws MetaException, InvalidObjectException, NoSuchObjectException, InvalidInputException {
    Database db1 = new Database(DB1, "description", "locationurl", null);
    objectStore.createDatabase(db1);
    StorageDescriptor sd = createFakeSd("location");
    HashMap<String,String> params = new HashMap<String,String>();
    params.put("EXTERNAL", "false");
    Table tbl1 = new Table(TABLE1, DB1, "owner", 1, 2, 3, sd, null, params, "viewOriginalText", "viewExpandedText", "MANAGED_TABLE");
    objectStore.createTable(tbl1);

    List<String> tables = objectStore.getAllTables(DB1);
    Assert.assertEquals(1, tables.size());
    Assert.assertEquals(TABLE1, tables.get(0));

    Table newTbl1 = new Table("new" + TABLE1, DB1, "owner", 1, 2, 3, sd, null, params, "viewOriginalText", "viewExpandedText", "MANAGED_TABLE");
    objectStore.alterTable(DB1, TABLE1, newTbl1);
    tables = objectStore.getTables(DB1, "new*");
    Assert.assertEquals(1, tables.size());
    Assert.assertEquals("new" + TABLE1, tables.get(0));

    objectStore.dropTable(DB1, "new" + TABLE1);
    tables = objectStore.getAllTables(DB1);
    Assert.assertEquals(0, tables.size());

    objectStore.dropDatabase(DB1);
  }
  

  /**
   * Test table operations
   */
  @Test
  public void testMmCleaner() throws Exception {
    HiveConf conf = new HiveConf();
    conf.set(ConfVars.HIVE_METASTORE_MM_HEARTBEAT_TIMEOUT.varname, "3ms");
    conf.set(ConfVars.HIVE_METASTORE_MM_ABSOLUTE_TIMEOUT.varname, "20ms");
    conf.set(ConfVars.HIVE_METASTORE_MM_ABORTED_GRACE_PERIOD.varname, "5ms");
    conf.set("fs.mock.impl", MockFileSystem.class.getName());

    MockFileSystem mfs = (MockFileSystem)(new Path("mock:///").getFileSystem(conf));
    mfs.clear();
    mfs.allowDelete = true;
    // Don't add the files just yet...
    MockFile[] files = new MockFile[9];
    for (int i = 0; i < files.length; ++i) {
      files[i] = new MockFile("mock:/foo/mm_" + i + "/1", 0, new byte[0]);
    }

    LongSupplier time = new LongSupplier();

    MmCleanerThread mct = new MmCleanerThread(0);
    mct.setHiveConf(conf);
    mct.overrideTime(time);

    Database db1 = new Database(DB1, "description", "locationurl", null);
    objectStore.createDatabase(db1);
    StorageDescriptor sd = createFakeSd("mock:/foo");
    HashMap<String,String> params = new HashMap<String,String>();
    params.put("EXTERNAL", "false");
    params.put(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, "true");
    params.put(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES, "insert_only");
    Table tbl = new Table(TABLE1, DB1, "owner", 1, 2, 3, sd,
        null, params, null, null, "MANAGED_TABLE");
    objectStore.createTable(tbl);

    // Add write #0 so the watermark wouldn't advance; skip write #1, add #2 at 0, skip #3
    createCompleteTableWrite(mfs, files, 0, time, tbl, HiveMetaStore.MM_WRITE_OPEN);
    mfs.addFile(files[1]);
    createCompleteTableWrite(mfs, files, 2, time, tbl, HiveMetaStore.MM_WRITE_OPEN);
    mfs.addFile(files[3]);
    tbl.setMmNextWriteId(4);
    objectStore.alterTable(DB1, TABLE1, tbl);

    mct.runOneIteration(objectStore);
    List<Long> writes = getAbortedWrites();
    assertEquals(0, writes.size()); // Missing write is not aborted before timeout.
    time.value = 4; // Advance time.
    mct.runOneIteration(objectStore);
    writes = getAbortedWrites();
    assertEquals(1, writes.size()); // Missing write is aborted after timeout.
    assertEquals(1L, writes.get(0).longValue());
    checkDeletedSet(files, 1);
    // However, write #3 was not aborted as we cannot determine when it will time out.
    createCompleteTableWrite(mfs, files, 4, time, tbl, HiveMetaStore.MM_WRITE_OPEN);
    time.value = 8;
    // It will now be aborted, since we have a following write.
    mct.runOneIteration(objectStore);
    writes = getAbortedWrites();
    assertEquals(2, writes.size());
    assertTrue(writes.contains(Long.valueOf(3)));
    checkDeletedSet(files, 1, 3);

    // Commit #0 and #2 and confirm that the watermark advances.
    // It will only advance over #1, since #3 was aborted at 8 and grace period has not passed.
    time.value = 10;
    MTableWrite tw = objectStore.getTableWrite(DB1, TABLE1, 0);
    tw.setState(String.valueOf(HiveMetaStore.MM_WRITE_COMMITTED));
    objectStore.updateTableWrite(tw);
    tw = objectStore.getTableWrite(DB1, TABLE1, 2);
    tw.setState(String.valueOf(HiveMetaStore.MM_WRITE_COMMITTED));
    objectStore.updateTableWrite(tw);
    mct.runOneIteration(objectStore);
    writes = getAbortedWrites();
    assertEquals(1, writes.size());
    assertEquals(3L, writes.get(0).longValue());
    tbl = objectStore.getTable(DB1, TABLE1);
    assertEquals(2L, tbl.getMmWatermarkWriteId());

    // Now advance the time and see that watermark also advances over #3.
    time.value = 16;
    mct.runOneIteration(objectStore);
    writes = getAbortedWrites();
    assertEquals(0, writes.size());
    tbl = objectStore.getTable(DB1, TABLE1);
    assertEquals(3L, tbl.getMmWatermarkWriteId());

    // Check that the open write gets aborted after some time; then the watermark advances.
    time.value = 25;
    mct.runOneIteration(objectStore);
    writes = getAbortedWrites();
    assertEquals(1, writes.size());
    assertEquals(4L, writes.get(0).longValue());
    time.value = 31;
    mct.runOneIteration(objectStore);
    tbl = objectStore.getTable(DB1, TABLE1);
    assertEquals(4L, tbl.getMmWatermarkWriteId());
    checkDeletedSet(files, 1, 3, 4); // The other two should still be deleted.

    // Finally check that we cannot advance watermark if cleanup fails for some file.
    createCompleteTableWrite(mfs, files, 5, time, tbl, HiveMetaStore.MM_WRITE_ABORTED);
    createCompleteTableWrite(mfs, files, 6, time, tbl, HiveMetaStore.MM_WRITE_ABORTED);
    createCompleteTableWrite(mfs, files, 7, time, tbl, HiveMetaStore.MM_WRITE_COMMITTED);
    createCompleteTableWrite(mfs, files, 8, time, tbl, HiveMetaStore.MM_WRITE_ABORTED);
    time.value = 37; // Skip the grace period.
    files[6].cannotDelete = true;
    mct.runOneIteration(objectStore);
    checkDeletedSet(files, 1, 3, 4, 5, 8); // The other two should still be deleted.
    tbl = objectStore.getTable(DB1, TABLE1);
    assertEquals(5L, tbl.getMmWatermarkWriteId()); // Watermark only goes up to 5.
    files[6].cannotDelete = false;
    mct.runOneIteration(objectStore);
    checkDeletedSet(files, 1, 3, 4, 5, 6, 8);
    tbl = objectStore.getTable(DB1, TABLE1);
    assertEquals(8L, tbl.getMmWatermarkWriteId()); // Now it advances all the way.

    objectStore.dropTable(DB1, TABLE1);
    objectStore.dropDatabase(DB1);
  }

  private void createCompleteTableWrite(MockFileSystem mfs, MockFile[] files,
      int id, LongSupplier time, Table tbl, char state) throws MetaException, InvalidObjectException {
    objectStore.createTableWrite(tbl, id, state, time.value);
    mfs.addFile(files[id]);
    tbl.setMmNextWriteId(id + 1);
    objectStore.alterTable(DB1, TABLE1, tbl);
  }

  private void checkDeletedSet(MockFile[] files, int... deleted) {
    for (int id : deleted) {
      assertTrue("File " + id + " not deleted", files[id].isDeleted);
    }
    int count = 0;
    for (MockFile file : files) {
      if (file.isDeleted) ++count;
    }
    assertEquals(deleted.length, count); // Make sure nothing else is deleted.
  }

  private List<Long> getAbortedWrites() throws MetaException {
    return objectStore.getTableWriteIds(DB1, TABLE1, -1, 10, HiveMetaStore.MM_WRITE_ABORTED);
  }

  private StorageDescriptor createFakeSd(String location) {
    return new StorageDescriptor(null, location, null, null, false, 0,
        new SerDeInfo("SerDeName", "serializationLib", null), null, null, null);
  }


  /**
   * Tests partition operations
   */
  @Test
  public void testPartitionOps() throws MetaException, InvalidObjectException, NoSuchObjectException, InvalidInputException {
    Database db1 = new Database(DB1, "description", "locationurl", null);
    objectStore.createDatabase(db1);
    StorageDescriptor sd = createFakeSd("location");
    HashMap<String,String> tableParams = new HashMap<String,String>();
    tableParams.put("EXTERNAL", "false");
    FieldSchema partitionKey1 = new FieldSchema("Country", serdeConstants.STRING_TYPE_NAME, "");
    FieldSchema partitionKey2 = new FieldSchema("State", serdeConstants.STRING_TYPE_NAME, "");
    Table tbl1 = new Table(TABLE1, DB1, "owner", 1, 2, 3, sd, Arrays.asList(partitionKey1, partitionKey2), tableParams, "viewOriginalText", "viewExpandedText", "MANAGED_TABLE");
    objectStore.createTable(tbl1);
    HashMap<String, String> partitionParams = new HashMap<String, String>();
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

    int numPartitions  = objectStore.getNumPartitionsByFilter(DB1, TABLE1, "");
    Assert.assertEquals(partitions.size(), numPartitions);

    numPartitions  = objectStore.getNumPartitionsByFilter(DB1, TABLE1, "country = \"US\"");
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
    HiveConf conf = new HiveConf();
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_METRICS_ENABLED, true);
    conf.setVar(HiveConf.ConfVars.HIVE_METRICS_REPORTER, MetricsReporting.JSON_FILE.name()
        + "," + MetricsReporting.JMX.name());

    MetricsFactory.init(conf);
    CodahaleMetrics metrics = (CodahaleMetrics) MetricsFactory.getInstance();

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

    String json = metrics.dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.COUNTER,
        MetricsConstant.DIRECTSQL_ERRORS, "");

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

    json = metrics.dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.COUNTER,
        MetricsConstant.DIRECTSQL_ERRORS, 1);
  }

  public static void dropAllStoreObjects(RawStore store) throws MetaException, InvalidObjectException, InvalidInputException {
    try {
      Deadline.registerIfNot(100000);
      List<Function> funcs = store.getAllFunctions();
      for (Function func : funcs) {
        store.dropFunction(func.getDbName(), func.getFunctionName());
      }
      List<String> dbs = store.getAllDatabases();
      for (int i = 0; i < dbs.size(); i++) {
        String db = dbs.get(i);
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
}
