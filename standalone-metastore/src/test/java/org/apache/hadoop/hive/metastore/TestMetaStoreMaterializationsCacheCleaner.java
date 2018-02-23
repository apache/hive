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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.BasicTxnInfo;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.Materialization;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link org.apache.hadoop.hive.metastore.MaterializationsInvalidationCache}.
 * The tests focus on arrival of notifications (possibly out of order) and the logic
 * to clean up the materializations cache. Tests need to be executed in a certain order
 * to avoid interactions among them, as the invalidation cache is a singleton.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestMetaStoreMaterializationsCacheCleaner {

  private static final String DB_NAME = "hive3252";
  private static final String TBL_NAME_1 = "tmptbl1";
  private static final String TBL_NAME_2 = "tmptbl2";
  private static final String TBL_NAME_3 = "tmptbl3";
  private static final String MV_NAME_1 = "mv1";
  private static final String MV_NAME_2 = "mv2";


  @Test
  public void testCleanerScenario1() throws Exception {
    // create mock raw store
    Configuration conf = new Configuration();
    conf.set("metastore.materializations.invalidation.impl", "DISABLE");
    // create mock handler
    final IHMSHandler handler = mock(IHMSHandler.class);
    // initialize invalidation cache (set conf to disable)
    MaterializationsInvalidationCache.get().init(conf, handler);

    // This is a dummy test, invalidation cache is not supposed to
    // record any information.
    MaterializationsInvalidationCache.get().notifyTableModification(
        DB_NAME, TBL_NAME_1, 1, 1);
    int id = 2;
    BasicTxnInfo txn2 = createTxnInfo(DB_NAME, TBL_NAME_1, id);
    MaterializationsInvalidationCache.get().notifyTableModification(
        DB_NAME, TBL_NAME_1, id, id);
    // Create tbl2 (nothing to do)
    id = 3;
    BasicTxnInfo txn3 = createTxnInfo(DB_NAME, TBL_NAME_1, id);
    MaterializationsInvalidationCache.get().notifyTableModification(
        DB_NAME, TBL_NAME_2, id, id);
    // Cleanup (current = 4, duration = 4) -> Does nothing
    long removed = MaterializationsInvalidationCache.get().cleanup(0L);
    Assert.assertEquals(0L, removed);
    // Create mv1
    Table mv1 = mock(Table.class);
    when(mv1.getDbName()).thenReturn(DB_NAME);
    when(mv1.getTableName()).thenReturn(MV_NAME_1);
    CreationMetadata mockCM1 = new CreationMetadata(
        DB_NAME, MV_NAME_1,
        ImmutableSet.of(
            DB_NAME + "." + TBL_NAME_1,
            DB_NAME + "." + TBL_NAME_2));
    // Create txn list (highWatermark=4;minOpenTxn=Long.MAX_VALUE)
    mockCM1.setValidTxnList("3:" + Long.MAX_VALUE + "::");
    when(mv1.getCreationMetadata()).thenReturn(mockCM1);
    MaterializationsInvalidationCache.get().createMaterializedView(mockCM1.getDbName(), mockCM1.getTblName(),
        mockCM1.getTablesUsed(), mockCM1.getValidTxnList());
    Map<String, Materialization> invalidationInfos =
        MaterializationsInvalidationCache.get().getMaterializationInvalidationInfo(
            DB_NAME, ImmutableList.of(MV_NAME_1));
    Assert.assertTrue(invalidationInfos.isEmpty());
    id = 10;
    BasicTxnInfo txn10 = createTxnInfo(DB_NAME, TBL_NAME_2, id);
    MaterializationsInvalidationCache.get().notifyTableModification(
        DB_NAME, TBL_NAME_2, id, id);
    id = 9;
    BasicTxnInfo txn9 = createTxnInfo(DB_NAME, TBL_NAME_1, id);
    MaterializationsInvalidationCache.get().notifyTableModification(
        DB_NAME, TBL_NAME_1, id, id);
    // Cleanup (current = 12, duration = 4) -> Removes txn1, txn2, txn3
    removed = MaterializationsInvalidationCache.get().cleanup(8L);
    Assert.assertEquals(0L, removed);
    invalidationInfos =
        MaterializationsInvalidationCache.get().getMaterializationInvalidationInfo(
            DB_NAME, ImmutableList.of(MV_NAME_1));
    Assert.assertTrue(invalidationInfos.isEmpty());
    // Create mv2
    Table mv2 = mock(Table.class);
    when(mv2.getDbName()).thenReturn(DB_NAME);
    when(mv2.getTableName()).thenReturn(MV_NAME_2);
    CreationMetadata mockCM2 = new CreationMetadata(
        DB_NAME, MV_NAME_2,
        ImmutableSet.of(
            DB_NAME + "." + TBL_NAME_1,
            DB_NAME + "." + TBL_NAME_2));
    // Create txn list (highWatermark=10;minOpenTxn=Long.MAX_VALUE)
    mockCM2.setValidTxnList("10:" + Long.MAX_VALUE + "::");
    when(mv2.getCreationMetadata()).thenReturn(mockCM2);
    MaterializationsInvalidationCache.get().createMaterializedView(mockCM2.getDbName(), mockCM2.getTblName(),
        mockCM2.getTablesUsed(), mockCM2.getValidTxnList());
    when(mv2.getCreationMetadata()).thenReturn(mockCM2);
    invalidationInfos =
        MaterializationsInvalidationCache.get().getMaterializationInvalidationInfo(
            DB_NAME, ImmutableList.of(MV_NAME_1, MV_NAME_2));
    Assert.assertTrue(invalidationInfos.isEmpty());
    // Create tbl3 (nothing to do)
    MaterializationsInvalidationCache.get().notifyTableModification(
        DB_NAME, TBL_NAME_3, 11, 11);
    MaterializationsInvalidationCache.get().notifyTableModification(
        DB_NAME, TBL_NAME_3, 18, 18);
    MaterializationsInvalidationCache.get().notifyTableModification(
        DB_NAME, TBL_NAME_1, 14, 14);
    MaterializationsInvalidationCache.get().notifyTableModification(
        DB_NAME, TBL_NAME_1, 17, 17);
    MaterializationsInvalidationCache.get().notifyTableModification(
        DB_NAME, TBL_NAME_2, 16, 16);
    // Cleanup (current = 20, duration = 4) -> Removes txn10, txn11
    removed = MaterializationsInvalidationCache.get().cleanup(16L);
    Assert.assertEquals(0L, removed);
    invalidationInfos =
        MaterializationsInvalidationCache.get().getMaterializationInvalidationInfo(
            DB_NAME, ImmutableList.of(MV_NAME_1, MV_NAME_2));
    Assert.assertTrue(invalidationInfos.isEmpty());
    MaterializationsInvalidationCache.get().notifyTableModification(
        DB_NAME, TBL_NAME_1, 12, 12);
    MaterializationsInvalidationCache.get().notifyTableModification(
        DB_NAME, TBL_NAME_2, 15, 15);
    MaterializationsInvalidationCache.get().notifyTableModification(
        DB_NAME, TBL_NAME_2, 7, 7);
    invalidationInfos =
        MaterializationsInvalidationCache.get().getMaterializationInvalidationInfo(
            DB_NAME, ImmutableList.of(MV_NAME_1, MV_NAME_2));
    Assert.assertTrue(invalidationInfos.isEmpty());
    // Cleanup (current = 24, duration = 4) -> Removes txn9, txn14, txn15, txn16, txn17, txn18
    removed = MaterializationsInvalidationCache.get().cleanup(20L);
    Assert.assertEquals(0L, removed);
    invalidationInfos =
        MaterializationsInvalidationCache.get().getMaterializationInvalidationInfo(
            DB_NAME, ImmutableList.of(MV_NAME_1, MV_NAME_2));
    Assert.assertTrue(invalidationInfos.isEmpty());
    // Cleanup (current = 28, duration = 4) -> Removes txn9
    removed = MaterializationsInvalidationCache.get().cleanup(24L);
    Assert.assertEquals(0L, removed);
  }

  @Test
  public void testCleanerScenario2() throws Exception {
    // create mock raw store
    Configuration conf = new Configuration();
    conf.set("metastore.materializations.invalidation.impl", "DEFAULT");
    // create mock handler
    final IHMSHandler handler = mock(IHMSHandler.class);
    // initialize invalidation cache (set conf to default)
    MaterializationsInvalidationCache.get().init(conf, handler);

    // Scenario consists of the following steps:
    // Create tbl1
    // (t = 1) Insert row in tbl1
    // (t = 2) Insert row in tbl1
    // Create tbl2
    // (t = 3) Insert row in tbl2
    // Cleanup (current = 4, duration = 4) -> Does nothing
    // Create mv1
    // (t = 10) Insert row in tbl2
    // (t = 9) Insert row in tbl1 (out of order)
    // Cleanup (current = 12, duration = 4) -> Removes txn1, txn2, txn3
    // Create mv2
    // Create tbl3
    // (t = 11) Insert row in tbl3
    // (t = 18) Insert row in tbl3
    // (t = 14) Insert row in tbl1
    // (t = 17) Insert row in tbl1
    // (t = 16) Insert row in tbl2
    // Cleanup (current = 20, duration = 4) -> Removes txn10, txn11
    // (t = 12) Insert row in tbl1
    // (t = 15) Insert row in tbl2
    // (t = 7) Insert row in tbl2
    // Cleanup (current = 24, duration = 4) -> Removes txn9, txn14, txn15, txn16, txn17, txn18
    // Create tbl1 (nothing to do)
    MaterializationsInvalidationCache.get().notifyTableModification(
        DB_NAME, TBL_NAME_1, 1, 1);
    int id = 2;
    BasicTxnInfo txn2 = createTxnInfo(DB_NAME, TBL_NAME_1, id);
    MaterializationsInvalidationCache.get().notifyTableModification(
        DB_NAME, TBL_NAME_1, id, id);
    // Create tbl2 (nothing to do)
    id = 3;
    BasicTxnInfo txn3 = createTxnInfo(DB_NAME, TBL_NAME_1, id);
    MaterializationsInvalidationCache.get().notifyTableModification(
        DB_NAME, TBL_NAME_2, id, id);
    // Cleanup (current = 4, duration = 4) -> Does nothing
    long removed = MaterializationsInvalidationCache.get().cleanup(0L);
    Assert.assertEquals(0L, removed);
    // Create mv1
    Table mv1 = mock(Table.class);
    when(mv1.getDbName()).thenReturn(DB_NAME);
    when(mv1.getTableName()).thenReturn(MV_NAME_1);
    CreationMetadata mockCM1 = new CreationMetadata(
        DB_NAME, MV_NAME_1,
        ImmutableSet.of(
            DB_NAME + "." + TBL_NAME_1,
            DB_NAME + "." + TBL_NAME_2));
    // Create txn list (highWatermark=4;minOpenTxn=Long.MAX_VALUE)
    mockCM1.setValidTxnList("3:" + Long.MAX_VALUE + "::");
    when(mv1.getCreationMetadata()).thenReturn(mockCM1);
    MaterializationsInvalidationCache.get().createMaterializedView(mockCM1.getDbName(), mockCM1.getTblName(),
        mockCM1.getTablesUsed(), mockCM1.getValidTxnList());
    Map<String, Materialization> invalidationInfos =
        MaterializationsInvalidationCache.get().getMaterializationInvalidationInfo(
            DB_NAME, ImmutableList.of(MV_NAME_1));
    Assert.assertEquals(0L, invalidationInfos.get(MV_NAME_1).getInvalidationTime());
    id = 10;
    BasicTxnInfo txn10 = createTxnInfo(DB_NAME, TBL_NAME_2, id);
    MaterializationsInvalidationCache.get().notifyTableModification(
        DB_NAME, TBL_NAME_2, id, id);
    id = 9;
    BasicTxnInfo txn9 = createTxnInfo(DB_NAME, TBL_NAME_1, id);
    MaterializationsInvalidationCache.get().notifyTableModification(
        DB_NAME, TBL_NAME_1, id, id);
    // Cleanup (current = 12, duration = 4) -> Removes txn1, txn2, txn3
    removed = MaterializationsInvalidationCache.get().cleanup(8L);
    Assert.assertEquals(3L, removed);
    invalidationInfos =
        MaterializationsInvalidationCache.get().getMaterializationInvalidationInfo(
            DB_NAME, ImmutableList.of(MV_NAME_1));
    Assert.assertEquals(9L, invalidationInfos.get(MV_NAME_1).getInvalidationTime());
    // Create mv2
    Table mv2 = mock(Table.class);
    when(mv2.getDbName()).thenReturn(DB_NAME);
    when(mv2.getTableName()).thenReturn(MV_NAME_2);
    CreationMetadata mockCM2 = new CreationMetadata(
        DB_NAME, MV_NAME_2,
        ImmutableSet.of(
            DB_NAME + "." + TBL_NAME_1,
            DB_NAME + "." + TBL_NAME_2));
    // Create txn list (highWatermark=10;minOpenTxn=Long.MAX_VALUE)
    mockCM2.setValidTxnList("10:" + Long.MAX_VALUE + "::");
    when(mv2.getCreationMetadata()).thenReturn(mockCM2);
    MaterializationsInvalidationCache.get().createMaterializedView(mockCM2.getDbName(), mockCM2.getTblName(),
        mockCM2.getTablesUsed(), mockCM2.getValidTxnList());
    when(mv2.getCreationMetadata()).thenReturn(mockCM2);
    invalidationInfos =
        MaterializationsInvalidationCache.get().getMaterializationInvalidationInfo(
            DB_NAME, ImmutableList.of(MV_NAME_1, MV_NAME_2));
    Assert.assertEquals(9L, invalidationInfos.get(MV_NAME_1).getInvalidationTime());
    Assert.assertEquals(0L, invalidationInfos.get(MV_NAME_2).getInvalidationTime());
    // Create tbl3 (nothing to do)
    MaterializationsInvalidationCache.get().notifyTableModification(
        DB_NAME, TBL_NAME_3, 11, 11);
    MaterializationsInvalidationCache.get().notifyTableModification(
        DB_NAME, TBL_NAME_3, 18, 18);
    MaterializationsInvalidationCache.get().notifyTableModification(
        DB_NAME, TBL_NAME_1, 14, 14);
    MaterializationsInvalidationCache.get().notifyTableModification(
        DB_NAME, TBL_NAME_1, 17, 17);
    MaterializationsInvalidationCache.get().notifyTableModification(
        DB_NAME, TBL_NAME_2, 16, 16);
    // Cleanup (current = 20, duration = 4) -> Removes txn10, txn11
    removed = MaterializationsInvalidationCache.get().cleanup(16L);
    Assert.assertEquals(2L, removed);
    invalidationInfos =
        MaterializationsInvalidationCache.get().getMaterializationInvalidationInfo(
            DB_NAME, ImmutableList.of(MV_NAME_1, MV_NAME_2));
    Assert.assertEquals(9L, invalidationInfos.get(MV_NAME_1).getInvalidationTime());
    Assert.assertEquals(14L, invalidationInfos.get(MV_NAME_2).getInvalidationTime());
    MaterializationsInvalidationCache.get().notifyTableModification(
        DB_NAME, TBL_NAME_1, 12, 12);
    MaterializationsInvalidationCache.get().notifyTableModification(
        DB_NAME, TBL_NAME_2, 15, 15);
    MaterializationsInvalidationCache.get().notifyTableModification(
        DB_NAME, TBL_NAME_2, 7, 7);
    invalidationInfos =
        MaterializationsInvalidationCache.get().getMaterializationInvalidationInfo(
            DB_NAME, ImmutableList.of(MV_NAME_1, MV_NAME_2));
    Assert.assertEquals(7L, invalidationInfos.get(MV_NAME_1).getInvalidationTime());
    Assert.assertEquals(12L, invalidationInfos.get(MV_NAME_2).getInvalidationTime());
    // Cleanup (current = 24, duration = 4) -> Removes txn9, txn14, txn15, txn16, txn17, txn18
    removed = MaterializationsInvalidationCache.get().cleanup(20L);
    Assert.assertEquals(6L, removed);
    invalidationInfos =
        MaterializationsInvalidationCache.get().getMaterializationInvalidationInfo(
            DB_NAME, ImmutableList.of(MV_NAME_1, MV_NAME_2));
    Assert.assertEquals(7L, invalidationInfos.get(MV_NAME_1).getInvalidationTime());
    Assert.assertEquals(12L, invalidationInfos.get(MV_NAME_2).getInvalidationTime());
    // Cleanup (current = 28, duration = 4) -> Removes txn9
    removed = MaterializationsInvalidationCache.get().cleanup(24L);
    Assert.assertEquals(0L, removed);
  }

  private static BasicTxnInfo createTxnInfo(String dbName, String tableName, int i) {
    BasicTxnInfo r = new BasicTxnInfo();
    r.setDbname(dbName);
    r.setTablename(tableName);
    r.setTxnid(i);
    r.setTime(i);
    return r;
  }
}
