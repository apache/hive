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
import org.mockito.Mockito;

import java.util.Collections;

import static org.mockito.Mockito.when;

public class TestCompactionQueryBuilderForMajorCompaction extends CompactionQueryBuilderTestBase {

  @Test
  public void testCreateNoSourceTable() {
    CompactionQueryBuilder queryBuilder = getMajorCompactionQueryBuilderForCreate();
    String query = queryBuilder.build();
    String expectedQuery =
        "CREATE temporary external table comp_test_result_table stored as orc TBLPROPERTIES ('compactiontable'='MAJOR', 'transactional'='false')";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testCreate() {
    CompactionQueryBuilder queryBuilder = getMajorCompactionQueryBuilderForCreate();
    Table sourceTable = createSourceTable();
    queryBuilder.setSourceTab(sourceTable);
    String query = queryBuilder.build();
    String expectedQuery =
        "CREATE temporary external table comp_test_result_table(`operation` int, `originalTransaction` bigint, `bucket` int, `rowId` bigint, `currentTransaction` bigint, `row` struct<`column_1` :string,`column_2` :int,`column_3` :boolean>)  stored as orc TBLPROPERTIES ('compactiontable'='MAJOR', 'transactional'='false')";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testCreateWithSourceTableProperties() {
    CompactionQueryBuilder queryBuilder = getMajorCompactionQueryBuilderForCreate();
    Table sourceTable = createSourceTableWithProperties();
    queryBuilder.setSourceTab(sourceTable);
    String query = queryBuilder.build();
    String expectedQuery =
        "CREATE temporary external table comp_test_result_table(`operation` int, `originalTransaction` bigint, `bucket` int, `rowId` bigint, `currentTransaction` bigint, `row` struct<`column_1` :string,`column_2` :int,`column_3` :boolean>)  stored as orc TBLPROPERTIES ('compactiontable'='MAJOR', 'orc.property_2'='44', 'transactional'='false')";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testCreateWithSourceTableLocation() {
    CompactionQueryBuilder queryBuilder = getMajorCompactionQueryBuilderForCreate();
    Table sourceTable = createSourceTable();
    queryBuilder.setSourceTab(sourceTable);
    queryBuilder.setLocation(SOME_TEST_LOCATION);
    String query = queryBuilder.build();
    String expectedQuery =
        "CREATE temporary external table comp_test_result_table(`operation` int, `originalTransaction` bigint, `bucket` int, `rowId` bigint, `currentTransaction` bigint, `row` struct<`column_1` :string,`column_2` :int,`column_3` :boolean>)  stored as orc LOCATION 'some_test_path' TBLPROPERTIES ('compactiontable'='MAJOR', 'transactional'='false')";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testCreateWithPartitionedSourceTable() {
    CompactionQueryBuilder queryBuilder = getMajorCompactionQueryBuilderForCreate();
    Table sourceTable = createSourceTable();
    queryBuilder.setSourceTab(sourceTable);
    queryBuilder.setPartitioned(true);
    String query = queryBuilder.build();
    String expectedQuery =
        "CREATE temporary external table comp_test_result_table(`operation` int, `originalTransaction` bigint, `bucket` int, `rowId` bigint, `currentTransaction` bigint, `row` struct<`column_1` :string,`column_2` :int,`column_3` :boolean>)  PARTITIONED BY (`file_name` STRING)  stored as orc TBLPROPERTIES ('compactiontable'='MAJOR', 'transactional'='false')";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testInsert() {
    CompactionQueryBuilder queryBuilder = getMajorCompactionQueryBuilderForInsert();
    Table sourceTable = createSourceTable();
    queryBuilder.setSourceTabForInsert(COMP_TEST_SOURCE_TABLE_FOR_INSERT);
    Partition sourcePartition = new Partition();
    sourcePartition.addToValues("source_part_1");
    sourcePartition.addToValues("true");
    sourcePartition.addToValues("4444");

    sourceTable.addToPartitionKeys(new FieldSchema("source_part_1", "string", "comment 1"));
    sourceTable.addToPartitionKeys(new FieldSchema("source_part_2", "boolean", "comment 2"));
    sourceTable.addToPartitionKeys(new FieldSchema("source_part_3", "int", "comment 3"));
    queryBuilder.setSourceTab(sourceTable);
    queryBuilder.setSourcePartition(sourcePartition);

    String query = queryBuilder.build();
    String expectedQuery =
        "INSERT into table comp_test_result_table select validate_acid_sort_order(ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId), ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId, ROW__ID.writeId, NAMED_STRUCT('column_1', `column_1`, 'column_2', `column_2`, 'column_3', `column_3`)  from comp_test_db.comp_test_insert_table  where `source_part_1`='source_part_1' and `source_part_2`=true and `source_part_3`='4444'";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testInsertPartitionMismatch() {
    CompactionQueryBuilder queryBuilder = getMajorCompactionQueryBuilderForInsert();
    Table sourceTable = createSourceTable();
    queryBuilder.setSourceTabForInsert(COMP_TEST_SOURCE_TABLE_FOR_INSERT);
    Partition sourcePartition = new Partition();
    sourcePartition.addToValues("source_part_1");
    sourcePartition.addToValues("true");
    sourcePartition.addToValues("4444");

    sourceTable.addToPartitionKeys(new FieldSchema("source_part_1", "string", "comment 1"));
    sourceTable.addToPartitionKeys(new FieldSchema("source_part_2", "boolean", "comment 2"));
    queryBuilder.setSourceTab(sourceTable);
    queryBuilder.setSourcePartition(sourcePartition);

    String expectedMessage =
        "source partition values ([source_part_1, true, 4444]) do not match source table values ([FieldSchema(name:source_part_1, type:string, comment:comment 1), FieldSchema(name:source_part_2, type:boolean, comment:comment 2)]). Failing compaction.";
    Assert.assertThrows(expectedMessage, IllegalStateException.class, queryBuilder::build);
  }

  @Test
  public void testInsertNoSourcePartition() {
    CompactionQueryBuilder queryBuilder = getMajorCompactionQueryBuilderForInsert();
    Table sourceTable = createSourceTable();
    queryBuilder.setSourceTabForInsert(COMP_TEST_SOURCE_TABLE_FOR_INSERT);
    queryBuilder.setSourceTab(sourceTable);
    String query = queryBuilder.build();
    String expectedQuery =
        "INSERT into table comp_test_result_table select validate_acid_sort_order(ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId), ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId, ROW__ID.writeId, NAMED_STRUCT('column_1', `column_1`, 'column_2', `column_2`, 'column_3', `column_3`)  from comp_test_db.comp_test_insert_table ";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testInsertNoSourceTableForInsert() {
    CompactionQueryBuilder queryBuilder = getMajorCompactionQueryBuilderForInsert();
    Table sourceTable = createSourceTable();
    queryBuilder.setSourceTab(sourceTable);
    String query = queryBuilder.build();
    String expectedQuery =
        "INSERT into table comp_test_result_table select validate_acid_sort_order(ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId), ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId, ROW__ID.writeId, NAMED_STRUCT('column_1', `column_1`, 'column_2', `column_2`, 'column_3', `column_3`)  from comp_test_db.comp_test_source_table ";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testInsertNoSourceTable() {
    CompactionQueryBuilder queryBuilder = getMajorCompactionQueryBuilderForInsert();
    queryBuilder.setSourceTabForInsert(COMP_TEST_SOURCE_TABLE_FOR_INSERT);
    String query = queryBuilder.build();
    String expectedQuery = "INSERT into table comp_test_result_table select  from comp_test_db.comp_test_insert_table ";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testAlter() {
    CompactionQueryBuilder queryBuilder = getMajorCompactionQueryBuilderForAlter();
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
  public void testAlterEmptyDir() {
    CompactionQueryBuilder queryBuilder = getMajorCompactionQueryBuilderForAlter();
    AcidDirectory dir = Mockito.mock(AcidDirectory.class);
    when(dir.getCurrentDirectories()).thenReturn(Collections.emptyList());
    ValidCompactorWriteIdList writeIds = createWriteId(5L);
    queryBuilder.setValidWriteIdList(writeIds);
    queryBuilder.setDir(dir);
    queryBuilder.setIsDeleteDelta(true);
    String query = queryBuilder.build();
    Assert.assertTrue(query.isEmpty());
  }

  @Test
  public void testAlterMinWriteIdIsNull() {
    CompactionQueryBuilder queryBuilder = getMajorCompactionQueryBuilderForAlter();
    AcidDirectory dir = createAcidDirectory();
    ValidCompactorWriteIdList writeIds = createWriteId(Long.MAX_VALUE);
    queryBuilder.setValidWriteIdList(writeIds);
    queryBuilder.setDir(dir);
    String query = queryBuilder.build();
    String expectedQuery =
        "ALTER table comp_test_result_table add partition (file_name='test_delta_5') location '/compaction/test/table/test_delta_5' ";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testAlterDirIsNull() {
    CompactionQueryBuilder queryBuilder = getMajorCompactionQueryBuilderForAlter();
    ValidCompactorWriteIdList writeIds = createWriteId(Long.MAX_VALUE);
    queryBuilder.setValidWriteIdList(writeIds);
    queryBuilder.setDir(null);
    String query = queryBuilder.build();
    Assert.assertTrue(query.isEmpty());
  }

  @Test
  public void testAlterWalidWriteIdListIsNull() {
    CompactionQueryBuilder queryBuilder = getMajorCompactionQueryBuilderForAlter();
    AcidDirectory dir = createAcidDirectory();
    queryBuilder.setValidWriteIdList(null);
    queryBuilder.setDir(dir);
    String query = queryBuilder.build();
    Assert.assertTrue(query.isEmpty());
  }

  @Test
  public void testDrop() {
    CompactionQueryBuilder queryBuilder = getMajorCompactionQueryBuilderForDrop();
    String query = queryBuilder.build();
    String expectedQuery = "DROP table if exists comp_test_result_table";
    Assert.assertEquals(expectedQuery, query);
  }

  private CompactionQueryBuilder getMajorCompactionQueryBuilderForCreate() {
    return getMajorCompactionBuilder().setOperation(CompactionQueryBuilder.Operation.CREATE);
  }

  private CompactionQueryBuilder getMajorCompactionQueryBuilderForInsert() {
    return getMajorCompactionBuilder().setOperation(CompactionQueryBuilder.Operation.INSERT);
  }

  private CompactionQueryBuilder getMajorCompactionQueryBuilderForAlter() {
    return getMajorCompactionBuilder().setOperation(CompactionQueryBuilder.Operation.ALTER);
  }

  private CompactionQueryBuilder getMajorCompactionQueryBuilderForDrop() {
    return getMajorCompactionBuilder().setOperation(CompactionQueryBuilder.Operation.DROP);
  }

  private CompactionQueryBuilder getMajorCompactionBuilder() {
    CompactionQueryBuilder compactionQueryBuilder =
        new CompactionQueryBuilderFactory().getCompactionQueryBuilder(CompactionType.MAJOR, false);
    return compactionQueryBuilder.setResultTableName(RESULT_TABLE_NAME);
  }

}
