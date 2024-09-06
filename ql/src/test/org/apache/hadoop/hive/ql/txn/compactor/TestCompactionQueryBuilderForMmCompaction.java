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
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestCompactionQueryBuilderForMmCompaction extends CompactionQueryBuilderTestBase {

  @Test
  public void testMajorCompactionCreateWithoutSourceTable() {
    CompactionQueryBuilder queryBuilder = getMmMajorCompactionQueryBuilderForCreate();
    String query = queryBuilder.build();
    String expectedQuery =
        "CREATE temporary external table comp_test_result_table TBLPROPERTIES ('transactional'='false')";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testMajorCompactionCreateWithTablePropertiesWithLocation() {
    CompactionQueryBuilder queryBuilder = getMmMajorCompactionQueryBuilderForCreate();
    Table sourceTable = createSourceTableWithProperties();
    queryBuilder.setSourceTab(sourceTable);
    queryBuilder.setLocation(SOME_TEST_LOCATION);
    String query = queryBuilder.build();
    String expectedQuery =
        "CREATE temporary external table comp_test_result_table(`column_1` string,`column_2` int,`column_3` boolean)  LOCATION 'some_test_path' TBLPROPERTIES ('property_1'='true', 'orc.property_2'='44', 'transactional'='false')";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testMajorCompactionCreatePartitioned() {
    CompactionQueryBuilder queryBuilder = getMmMajorCompactionQueryBuilderForCreate();
    Table sourceTable = createSourceTable();
    queryBuilder.setSourceTab(sourceTable);
    queryBuilder.setPartitioned(true);
    queryBuilder.setLocation(SOME_TEST_LOCATION);
    String query = queryBuilder.build();
    String expectedQuery =
        "CREATE temporary external table comp_test_result_table(`column_1` string,`column_2` int,`column_3` boolean)  PARTITIONED BY (`file_name` STRING)  LOCATION 'some_test_path' TBLPROPERTIES ('transactional'='false')";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testMajorCompactionCreateWithBucketedSourceTable() throws HiveException {
    CompactionQueryBuilder queryBuilder = getMmMajorCompactionQueryBuilderForCreate();
    Table sourceTable = createSourceTableBucketed();
    queryBuilder.setSourceTab(sourceTable);
    String query = queryBuilder.build();
    String expectedQuery =
        "CREATE temporary external table comp_test_result_table(`column_1` string,`column_2` int,`column_3` boolean) CLUSTERED BY (column_1,column_3) INTO 4 BUCKETS TBLPROPERTIES ('transactional'='false')";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testMajorCompactionCreateWithBucketedSortedSourceTable() throws HiveException {
    CompactionQueryBuilder queryBuilder = getMmMajorCompactionQueryBuilderForCreate();
    Table sourceTable = createSourceTableBucketedSorted();
    queryBuilder.setSourceTab(sourceTable);
    String query = queryBuilder.build();
    String expectedQuery =
        "CREATE temporary external table comp_test_result_table(`column_1` string,`column_2` int,`column_3` boolean) CLUSTERED BY (column_1,column_3) SORTED BY (column_1 DESC, column_2 ASC) INTO 4 BUCKETS TBLPROPERTIES ('transactional'='false')";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testMajorCompactionCreateWithStorageDescriptor() throws HiveException {
    CompactionQueryBuilder queryBuilder = getMmMajorCompactionQueryBuilderForCreate();
    Table sourceTable = createSourceTable();
    queryBuilder.setSourceTab(sourceTable);
    StorageDescriptor storageDescriptor = createStorageDescriptor();
    queryBuilder.setStorageDescriptor(storageDescriptor);
    String query = queryBuilder.build();
    String expectedQuery =
        "CREATE temporary external table comp_test_result_table(`column_1` string,`column_2` int,`column_3` boolean)  ROW FORMAT SERDE '/some/test/serialization_lib'WITH SERDEPROPERTIES ( \n" + "  'test_param_1'='test_value', \n" + "  'test_param_2'='test_value')STORED AS INPUTFORMAT 'some.test.InputFormat' OUTPUTFORMAT 'some.test.OutputFormat' TBLPROPERTIES ('transactional'='false')";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testMajorCompactionCreateWithSkewedByClause() throws HiveException {
    CompactionQueryBuilder queryBuilder = getMmMajorCompactionQueryBuilderForCreate();
    Table sourceTable = createSourceTable();
    StorageDescriptor storageDescriptor = sourceTable.getSd();
    SkewedInfo skewedInfo = new SkewedInfo();
    skewedInfo.addToSkewedColNames("column_1");
    List<String> skewedColValues = new ArrayList<>();
    skewedColValues.add("value1");
    skewedColValues.add("value2");
    skewedColValues.add("value3");
    skewedInfo.addToSkewedColValues(skewedColValues);
    storageDescriptor.setSkewedInfo(skewedInfo);
    storageDescriptor.setStoredAsSubDirectories(true);
    sourceTable.setSd(storageDescriptor);
    queryBuilder.setSourceTab(sourceTable);
    String query = queryBuilder.build();
    String expectedQuery =
        "CREATE temporary external table comp_test_result_table(`column_1` string,`column_2` int,`column_3` boolean)  SKEWED BY (column_1) ON ('value1','value2','value3')) STORED AS DIRECTORIES TBLPROPERTIES ('transactional'='false')";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testMajorCompactionCreateWithNonNativeTable() throws HiveException {
    CompactionQueryBuilder queryBuilder = getMmMajorCompactionQueryBuilderForCreate();
    Table sourceTable = createSourceTable();
    Map<String, String> parameters = new HashMap<>();
    parameters.put("storage_handler", "test_storage_handler");
    sourceTable.setParameters(parameters);
    queryBuilder.setSourceTab(sourceTable);
    StorageDescriptor storageDescriptor = createStorageDescriptor();
    queryBuilder.setStorageDescriptor(storageDescriptor);
    String expectedMessage =
        "Table comp_test_source_table has a storage handler (test_storage_handler). Failing compaction for this non-native table.";
    Assert.assertThrows(expectedMessage, RuntimeException.class, queryBuilder::build);
  }

  @Test
  public void testMinorCompactionCreateWithoutSourceTable() {
    CompactionQueryBuilder queryBuilder = getMmMinorCompactionQueryBuilderForCreate();
    String query = queryBuilder.build();
    String expectedQuery =
        "CREATE temporary external table comp_test_result_table TBLPROPERTIES ('transactional'='false')";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testMinorCompactionCreateWithTablePropertiesWithLocation() {
    CompactionQueryBuilder queryBuilder = getMmMinorCompactionQueryBuilderForCreate();
    Table sourceTable = createSourceTableWithProperties();
    queryBuilder.setSourceTab(sourceTable);
    queryBuilder.setLocation(SOME_TEST_LOCATION);
    String query = queryBuilder.build();
    String expectedQuery =
        "CREATE temporary external table comp_test_result_table(`column_1` string,`column_2` int,`column_3` boolean)  LOCATION 'some_test_path' TBLPROPERTIES ('property_1'='true', 'orc.property_2'='44', 'transactional'='false')";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testMinorCompactionCreatePartitioned() {
    CompactionQueryBuilder queryBuilder = getMmMinorCompactionQueryBuilderForCreate();
    Table sourceTable = createSourceTable();
    queryBuilder.setSourceTab(sourceTable);
    queryBuilder.setPartitioned(true);
    queryBuilder.setLocation(SOME_TEST_LOCATION);
    String query = queryBuilder.build();
    String expectedQuery =
        "CREATE temporary external table comp_test_result_table(`column_1` string,`column_2` int,`column_3` boolean)  PARTITIONED BY (`file_name` STRING)  LOCATION 'some_test_path' TBLPROPERTIES ('transactional'='false')";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testMinorCompactionCreateWithBucketedSourceTable() throws HiveException {
    CompactionQueryBuilder queryBuilder = getMmMinorCompactionQueryBuilderForCreate();
    Table sourceTable = createSourceTableBucketed();
    queryBuilder.setSourceTab(sourceTable);
    String query = queryBuilder.build();
    String expectedQuery =
        "CREATE temporary external table comp_test_result_table(`column_1` string,`column_2` int,`column_3` boolean) CLUSTERED BY (column_1,column_3) INTO 4 BUCKETS TBLPROPERTIES ('transactional'='false')";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testMinorCompactionCreateWithBucketedSortedSourceTable() throws HiveException {
    CompactionQueryBuilder queryBuilder = getMmMinorCompactionQueryBuilderForCreate();
    Table sourceTable = createSourceTableBucketedSorted();
    queryBuilder.setSourceTab(sourceTable);
    String query = queryBuilder.build();
    String expectedQuery =
        "CREATE temporary external table comp_test_result_table(`column_1` string,`column_2` int,`column_3` boolean) CLUSTERED BY (column_1,column_3) SORTED BY (column_1 DESC, column_2 ASC) INTO 4 BUCKETS TBLPROPERTIES ('transactional'='false')";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testMinorCompactionCreateWithStorageDescriptor() throws HiveException {
    CompactionQueryBuilder queryBuilder = getMmMinorCompactionQueryBuilderForCreate();
    Table sourceTable = createSourceTable();
    queryBuilder.setSourceTab(sourceTable);
    StorageDescriptor storageDescriptor = createStorageDescriptor();
    queryBuilder.setStorageDescriptor(storageDescriptor);
    String query = queryBuilder.build();
    String expectedQuery =
        "CREATE temporary external table comp_test_result_table(`column_1` string,`column_2` int,`column_3` boolean)  ROW FORMAT SERDE '/some/test/serialization_lib'WITH SERDEPROPERTIES ( \n" + "  'test_param_1'='test_value', \n" + "  'test_param_2'='test_value')STORED AS INPUTFORMAT 'some.test.InputFormat' OUTPUTFORMAT 'some.test.OutputFormat' TBLPROPERTIES ('transactional'='false')";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testMinorCompactionCreateWithSkewedByClause() throws HiveException {
    CompactionQueryBuilder queryBuilder = getMmMinorCompactionQueryBuilderForCreate();
    Table sourceTable = createSourceTable();
    StorageDescriptor storageDescriptor = sourceTable.getSd();
    SkewedInfo skewedInfo = new SkewedInfo();
    skewedInfo.addToSkewedColNames("column_1");
    List<String> skewedColValues = new ArrayList<>();
    skewedColValues.add("value1");
    skewedColValues.add("value2");
    skewedColValues.add("value3");
    skewedInfo.addToSkewedColValues(skewedColValues);
    storageDescriptor.setSkewedInfo(skewedInfo);
    storageDescriptor.setStoredAsSubDirectories(true);
    sourceTable.setSd(storageDescriptor);
    queryBuilder.setSourceTab(sourceTable);
    String query = queryBuilder.build();
    String expectedQuery =
        "CREATE temporary external table comp_test_result_table(`column_1` string,`column_2` int,`column_3` boolean)  SKEWED BY (column_1) ON ('value1','value2','value3')) STORED AS DIRECTORIES TBLPROPERTIES ('transactional'='false')";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testMinorCompactionCreateLWithNonNativeTable() throws HiveException {
    CompactionQueryBuilder queryBuilder = getMmMinorCompactionQueryBuilderForCreate();
    Table sourceTable = createSourceTable();
    Map<String, String> parameters = new HashMap<>();
    parameters.put("storage_handler", "test_storage_handler");
    sourceTable.setParameters(parameters);
    queryBuilder.setSourceTab(sourceTable);
    StorageDescriptor storageDescriptor = createStorageDescriptor();
    queryBuilder.setStorageDescriptor(storageDescriptor);
    String expectedMessage =
        "Table comp_test_source_tablehas a storage handler (test_storage_handler). Failing compaction for this non-native table.";
    Assert.assertThrows(expectedMessage, RuntimeException.class, queryBuilder::build);
  }

  @Test
  public void testInsertMajorCompaction() {
    CompactionQueryBuilder queryBuilder = getMmMajorCompactionQueryBuilderForInsert();
    Table sourceTable = createSourceTableBucketedSorted();
    queryBuilder.setSourceTabForInsert(COMP_TEST_SOURCE_TABLE_FOR_INSERT);
    queryBuilder.setOrderByClause("ORDER BY column_1 ASC, column_2 DESC");
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
        "INSERT into table comp_test_result_table select `column_1`, `column_2`, `column_3` from comp_test_db.comp_test_insert_table ORDER BY column_1 ASC, column_2 DESC where `source_part_1`='source_part_1' and `source_part_2`=true and `source_part_3`='4444'";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testInsertMajorCompactionPartitionMismatch() {
    CompactionQueryBuilder queryBuilder = getMmMajorCompactionQueryBuilderForInsert();
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
  public void testInsertMajorCompactionNoSourceTabForInsert() {
    CompactionQueryBuilder queryBuilder = getMmMajorCompactionQueryBuilderForInsert();
    Table sourceTable = createSourceTable();
    Partition sourcePartition = new Partition();
    sourcePartition.addToValues("source_part_1");
    sourceTable.addToPartitionKeys(new FieldSchema("source_part_1", "string", "comment 1"));
    queryBuilder.setSourceTab(sourceTable);
    queryBuilder.setSourcePartition(sourcePartition);

    String query = queryBuilder.build();
    String expectedQuery =
        "INSERT into table comp_test_result_table select `column_1`, `column_2`, `column_3` from comp_test_db.comp_test_source_table  where `source_part_1`='source_part_1'";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testInsertMajorCompactionOnlySourceTableSet() {
    CompactionQueryBuilder queryBuilder = getMmMajorCompactionQueryBuilderForInsert();
    Table sourceTable = createSourceTable();
    queryBuilder.setSourceTab(sourceTable);

    String query = queryBuilder.build();
    String expectedQuery =
        "INSERT into table comp_test_result_table select * from comp_test_db.comp_test_source_table ";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testInsertMajorCompactionNoSourceTable() {
    CompactionQueryBuilder queryBuilder = getMmMajorCompactionQueryBuilderForInsert();
    queryBuilder.setSourceTabForInsert(COMP_TEST_SOURCE_TABLE_FOR_INSERT);
    String query = queryBuilder.build();
    String expectedQuery =
        "INSERT into table comp_test_result_table select * from comp_test_db.comp_test_insert_table ";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testInsertMinorCompaction() {
    CompactionQueryBuilder queryBuilder = getMmMinorCompactionQueryBuilderForInsert();
    Table sourceTable = createSourceTable();
    queryBuilder.setSourceTabForInsert(COMP_TEST_SOURCE_TABLE_FOR_INSERT);
    queryBuilder.setSourceTab(sourceTable);
    String query = queryBuilder.build();
    String expectedQuery =
        "INSERT into table comp_test_result_table select `column_1`, `column_2`, `column_3` from comp_test_db.comp_test_insert_table ";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testInsertMinorCompactionWithoutSourceTableForInsert() {
    CompactionQueryBuilder queryBuilder = getMmMinorCompactionQueryBuilderForInsert();
    Table sourceTable = createSourceTable();
    queryBuilder.setSourceTab(sourceTable);
    String query = queryBuilder.build();
    String expectedQuery =
        "INSERT into table comp_test_result_table select `column_1`, `column_2`, `column_3` from comp_test_db.comp_test_source_table ";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testInsertMinorCompactionNoSourceTable() {
    CompactionQueryBuilder queryBuilder = getMmMinorCompactionQueryBuilderForInsert();
    queryBuilder.setSourceTabForInsert(COMP_TEST_SOURCE_TABLE_FOR_INSERT);
    String query = queryBuilder.build();
    String expectedQuery = "INSERT into table comp_test_result_table select  from comp_test_db.comp_test_insert_table ";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testAlterMajorCompaction() {
    CompactionQueryBuilder queryBuilder = getMmMajorCompactionQueryBuilderForAlter();
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
  public void testAlterMinorCompaction() {
    CompactionQueryBuilder queryBuilder = getMmMinorCompactionQueryBuilderForAlter();
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
  public void testDropMajorCompaction() {
    CompactionQueryBuilder queryBuilder = getMmMajorCompactionQueryBuilderForDrop();
    String query = queryBuilder.build();
    String expectedQuery = "DROP table if exists comp_test_result_table";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testDropMinorCompaction() {
    CompactionQueryBuilder queryBuilder = getMmMinorCompactionQueryBuilderForDrop();
    String query = queryBuilder.build();
    String expectedQuery = "DROP table if exists comp_test_result_table";
    Assert.assertEquals(expectedQuery, query);
  }

  private StorageDescriptor createStorageDescriptor() {
    SerDeInfo serdeInfo = new SerDeInfo();
    serdeInfo.setSerializationLib("/some/test/serialization_lib");
    Map<String, String> serdeParams = new HashMap<>();
    serdeParams.put("test_param_1", "test_value");
    serdeParams.put("test_param_2", "test_value");
    serdeInfo.setParameters(serdeParams);
    StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setSerdeInfo(serdeInfo);
    storageDescriptor.setInputFormat("some.test.InputFormat");
    storageDescriptor.setOutputFormat("some.test.OutputFormat");
    return storageDescriptor;
  }

  private CompactionQueryBuilder getMmMajorCompactionQueryBuilderForCreate() {
    return getMmMajorCompactionBuilder().setOperation(CompactionQueryBuilder.Operation.CREATE);
  }

  private CompactionQueryBuilder getMmMajorCompactionQueryBuilderForInsert() {
    return getMmMajorCompactionBuilder().setOperation(CompactionQueryBuilder.Operation.INSERT);
  }

  private CompactionQueryBuilder getMmMajorCompactionQueryBuilderForAlter() {
    return getMmMajorCompactionBuilder().setOperation(CompactionQueryBuilder.Operation.ALTER);
  }

  private CompactionQueryBuilder getMmMajorCompactionQueryBuilderForDrop() {
    return getMmMajorCompactionBuilder().setOperation(CompactionQueryBuilder.Operation.DROP);
  }

  private CompactionQueryBuilder getMmMinorCompactionQueryBuilderForCreate() {
    return getMmMinorCompactionBuilder().setOperation(CompactionQueryBuilder.Operation.CREATE);
  }

  private CompactionQueryBuilder getMmMinorCompactionQueryBuilderForInsert() {
    return getMmMinorCompactionBuilder().setOperation(CompactionQueryBuilder.Operation.INSERT);
  }

  private CompactionQueryBuilder getMmMinorCompactionQueryBuilderForAlter() {
    return getMmMinorCompactionBuilder().setOperation(CompactionQueryBuilder.Operation.ALTER);
  }

  private CompactionQueryBuilder getMmMinorCompactionQueryBuilderForDrop() {
    return getMmMinorCompactionBuilder().setOperation(CompactionQueryBuilder.Operation.DROP);
  }

  private CompactionQueryBuilder getMmMajorCompactionBuilder() {
    CompactionQueryBuilder compactionQueryBuilder =
        new CompactionQueryBuilderFactory().getCompactionQueryBuilder(CompactionType.MAJOR, true);
    return compactionQueryBuilder.setResultTableName(RESULT_TABLE_NAME);
  }

  private CompactionQueryBuilder getMmMinorCompactionBuilder() {
    CompactionQueryBuilder compactionQueryBuilder =
        new CompactionQueryBuilderFactory().getCompactionQueryBuilder(CompactionType.MINOR, true);
    return compactionQueryBuilder.setResultTableName(RESULT_TABLE_NAME);
  }
}
