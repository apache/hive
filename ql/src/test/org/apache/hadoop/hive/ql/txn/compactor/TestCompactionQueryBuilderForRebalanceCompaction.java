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
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.hive.common.ValidCompactorWriteIdList;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.junit.Assert;
import org.junit.Test;

public class TestCompactionQueryBuilderForRebalanceCompaction extends CompactionQueryBuilderTestBase {

  @Test
  public void testCreate() {
    CompactionQueryBuilder queryBuilder = getRebalanceCompactionQueryBuilderForCreate();
    Table sourceTable = createSourceTableWithProperties();
    queryBuilder.setSourceTab(sourceTable);
    queryBuilder.setLocation(SOME_TEST_LOCATION);
    queryBuilder.setPartitioned(true);
    String query = queryBuilder.build();
    String expectedQuery =
        "CREATE temporary external table comp_test_result_table(`operation` int, `originalTransaction` bigint, `bucket` int, `rowId` bigint, `currentTransaction` bigint, `row` struct<`column_1` :string,`column_2` :int,`column_3` :boolean>)  PARTITIONED BY (`file_name` STRING)  stored as orc LOCATION 'some_test_path' TBLPROPERTIES ('compactiontable'='REBALANCE', 'orc.property_2'='44', 'transactional'='false')";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testCreateWithNonPartitionedSourceTable() {
    CompactionQueryBuilder queryBuilder = getRebalanceCompactionQueryBuilderForCreate();
    Table sourceTable = createSourceTableWithProperties();
    queryBuilder.setSourceTab(sourceTable);
    queryBuilder.setLocation(SOME_TEST_LOCATION);
    String query = queryBuilder.build();
    String expectedQuery =
        "CREATE temporary external table comp_test_result_table(`operation` int, `originalTransaction` bigint, `bucket` int, `rowId` bigint, `currentTransaction` bigint, `row` struct<`column_1` :string,`column_2` :int,`column_3` :boolean>)  stored as orc LOCATION 'some_test_path' TBLPROPERTIES ('compactiontable'='REBALANCE', 'orc.property_2'='44', 'transactional'='false')";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testCreateWithNoLocationAndNoTableProperties() {
    CompactionQueryBuilder queryBuilder = getRebalanceCompactionQueryBuilderForCreate();
    Table sourceTable = createSourceTable();
    queryBuilder.setSourceTab(sourceTable);
    String query = queryBuilder.build();
    String expectedQuery =
        "CREATE temporary external table comp_test_result_table(`operation` int, `originalTransaction` bigint, `bucket` int, `rowId` bigint, `currentTransaction` bigint, `row` struct<`column_1` :string,`column_2` :int,`column_3` :boolean>)  stored as orc TBLPROPERTIES ('compactiontable'='REBALANCE', 'transactional'='false')";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testInsert() {
    CompactionQueryBuilder queryBuilder = getRebalanceCompactionQueryBuilderForInsert();
    Table sourceTable = createSourceTable();
    queryBuilder.setSourceTabForInsert(COMP_TEST_SOURCE_TABLE_FOR_INSERT);
    queryBuilder.setSourceTab(sourceTable);
    queryBuilder.setNumberOfBuckets(5);
    queryBuilder.setOrderByClause("ORDER BY column_1 ASC, column_3 DESC");
    String query = queryBuilder.build();
    String expectedQuery =
        "INSERT overwrite table comp_test_result_table select 0, t2.writeId, t2.rowId DIV CEIL(numRows / 5), t2.rowId, t2.writeId, t2.data from (select count(ROW__ID.writeId) over() as numRows, MAX(ROW__ID.writeId) over() as writeId, row_number() OVER (ORDER BY column_1 ASC, column_3 DESC) - 1 AS rowId, NAMED_STRUCT('column_1', `column_1`, 'column_2', `column_2`, 'column_3', `column_3`) as data from comp_test_db.comp_test_insert_table ORDER BY column_1 ASC, column_3 DESC) t2";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testInsertWithPartitionedTable() {
    CompactionQueryBuilder queryBuilder = getRebalanceCompactionQueryBuilderForInsert();
    Table sourceTable = createSourceTable();
    queryBuilder.setSourceTabForInsert(COMP_TEST_SOURCE_TABLE_FOR_INSERT);
    Partition sourcePartition = new Partition();
    sourcePartition.addToValues("source_part_1");
    sourceTable.addToPartitionKeys(new FieldSchema("source_part_1", "string", "comment 1"));
    queryBuilder.setSourceTab(sourceTable);
    queryBuilder.setSourcePartition(sourcePartition);

    String query = queryBuilder.build();
    String expectedQuery =
        "INSERT overwrite table comp_test_result_table select 0, t2.writeId, t2.rowId DIV CEIL(numRows / 0), t2.rowId, t2.writeId, t2.data from (select count(ROW__ID.writeId) over() as numRows, ROW__ID.writeId as writeId, row_number() OVER (order by ROW__ID.writeId ASC, ROW__ID.bucketId ASC, ROW__ID.rowId ASC) - 1 AS rowId, NAMED_STRUCT('column_1', `column_1`, 'column_2', `column_2`, 'column_3', `column_3`) as data from comp_test_db.comp_test_insert_table order by ROW__ID.writeId ASC, ROW__ID.bucketId ASC, ROW__ID.rowId ASC) t2";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testInsertOnlySourceTableIsSet() {
    CompactionQueryBuilder queryBuilder = getRebalanceCompactionQueryBuilderForInsert();
    Table sourceTable = createSourceTable();
    queryBuilder.setSourceTab(sourceTable);
    String query = queryBuilder.build();
    String expectedQuery =
        "INSERT overwrite table comp_test_result_table select 0, t2.writeId, t2.rowId DIV CEIL(numRows / 0), t2.rowId, t2.writeId, t2.data from (select count(ROW__ID.writeId) over() as numRows, ROW__ID.writeId as writeId, row_number() OVER (order by ROW__ID.writeId ASC, ROW__ID.bucketId ASC, ROW__ID.rowId ASC) - 1 AS rowId, NAMED_STRUCT('column_1', `column_1`, 'column_2', `column_2`, 'column_3', `column_3`) as data from comp_test_db.comp_test_source_table order by ROW__ID.writeId ASC, ROW__ID.bucketId ASC, ROW__ID.rowId ASC) t2";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testInsertNoSourceTable() {
    CompactionQueryBuilder queryBuilder = getRebalanceCompactionQueryBuilderForInsert();
    queryBuilder.setSourceTabForInsert(COMP_TEST_SOURCE_TABLE_FOR_INSERT);
    String query = queryBuilder.build();
    String expectedQuery =
        "INSERT overwrite table comp_test_result_table select  from comp_test_db.comp_test_insert_table order by ROW__ID.writeId ASC, ROW__ID.bucketId ASC, ROW__ID.rowId ASC) t2";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testAlter() {
    CompactionQueryBuilder queryBuilder = getRebalanceCompactionQueryBuilderForAlter();
    AcidDirectory dir = createAcidDirectory();
    ValidCompactorWriteIdList writeIds = createWriteId(5L);
    queryBuilder.setValidWriteIdList(writeIds);
    queryBuilder.setDir(dir);
    queryBuilder.setIsDeleteDelta(true);
    String query = queryBuilder.build();
    String expectedQuery =
        "ALTER table comp_test_result_table add partition (file_name='test_delta_1') location '/compaction/test/table/test_delta_1' partition (file_name='test_delta_3') location '/compaction/test/table/test_delta_3' ";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testDrop() {
    CompactionQueryBuilder queryBuilder = getRebalanceCompactionQueryBuilderForDrop();
    String query = queryBuilder.build();
    String expectedQuery = "DROP table if exists comp_test_result_table";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testRebalanceCompactionWithBuckets() {
    CompactionQueryBuilder queryBuilder = getRebalanceCompactionQueryBuilderForInsert();
    String expectedMessage = "Rebalance compaction is supported only on implicitly-bucketed tables!";
    Assert.assertThrows(expectedMessage, IllegalArgumentException.class, () -> {
      queryBuilder.setBucketed(true);
    });
  }

  private CompactionQueryBuilder getRebalanceCompactionQueryBuilderForCreate() {
    return getRebalanceCompactionBuilder().setOperation(CompactionQueryBuilder.Operation.CREATE);
  }

  private CompactionQueryBuilder getRebalanceCompactionQueryBuilderForInsert() {
    return getRebalanceCompactionBuilder().setOperation(CompactionQueryBuilder.Operation.INSERT);
  }

  private CompactionQueryBuilder getRebalanceCompactionQueryBuilderForAlter() {
    return getRebalanceCompactionBuilder().setOperation(CompactionQueryBuilder.Operation.ALTER);
  }

  private CompactionQueryBuilder getRebalanceCompactionQueryBuilderForDrop() {
    return getRebalanceCompactionBuilder().setOperation(CompactionQueryBuilder.Operation.DROP);
  }

  private CompactionQueryBuilder getRebalanceCompactionBuilder() {
    CompactionQueryBuilder compactionQueryBuilder =
        new CompactionQueryBuilderFactory().getCompactionQueryBuilder(CompactionType.REBALANCE, false);
    return compactionQueryBuilder.setResultTableName(RESULT_TABLE_NAME);
  }
}
