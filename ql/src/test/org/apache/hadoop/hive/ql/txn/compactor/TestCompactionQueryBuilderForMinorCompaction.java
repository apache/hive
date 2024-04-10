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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class TestCompactionQueryBuilderForMinorCompaction extends CompactionQueryBuilderTest {

  class CompactionQueryBuilderMock extends CompactionQueryBuilder {
    private boolean throwException = false;
    CompactionQueryBuilderMock(CompactionType compactionType, Operation operation, boolean insertOnly, String resultTableName) {
      super(compactionType, operation, insertOnly, resultTableName);
    }

    public void setThrowException(boolean throwException) {
      this.throwException = throwException;
    }

    @Override
    public org.apache.hadoop.hive.ql.metadata.Table getTable() throws HiveException {
      org.apache.hadoop.hive.ql.metadata.Table t = Mockito.mock(org.apache.hadoop.hive.ql.metadata.Table.class);
      if (throwException) {
        throw new HiveException();
      } else {
        when(t.getBucketingVersion()).thenReturn(2);
        when(t.getNumBuckets()).thenReturn(4);
        return t;
      }
    }
  }

  @Test
  public void testCreateDDLWithoutSourceTable() {
    CompactionQueryBuilder queryBuilder = getMinorCompactionQueryBuilderForCreate();
    String query = queryBuilder.build();
    String expectedQuery = "CREATE temporary external table comp_test_result_table stored as orc TBLPROPERTIES ('compactiontable'='MINOR', 'transactional'='false')";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testCreateDDLWithTablePropertiesWithLocation() {
    CompactionQueryBuilder queryBuilder = getMinorCompactionQueryBuilderForCreate();
    Table sourceTable = createSourceTableWithProperties();
    queryBuilder.setSourceTab(sourceTable);
    queryBuilder.setLocation("some_test_path");
    String query = queryBuilder.build();
    String expectedQuery = "CREATE temporary external table comp_test_result_table(`operation` int, `originalTransaction` bigint, `bucket` int, `rowId` bigint, `currentTransaction` bigint, `row` struct<`column_1` :string,`column_2` :int,`column_3` :boolean>)  stored as orc LOCATION 'some_test_path' TBLPROPERTIES ('compactiontable'='MINOR', 'orc.property_2'='44', 'transactional'='false')";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testCreateDDLPartitioned() {
    CompactionQueryBuilder queryBuilder = getMinorCompactionQueryBuilderForCreate();
    Table sourceTable = createSourceTable();
    queryBuilder.setSourceTab(sourceTable);
    queryBuilder.setPartitioned(true);
    queryBuilder.setLocation("some_test_path");
    String query = queryBuilder.build();
    String expectedQuery = "CREATE temporary external table comp_test_result_table(`operation` int, `originalTransaction` bigint, `bucket` int, `rowId` bigint, `currentTransaction` bigint, `row` struct<`column_1` :string,`column_2` :int,`column_3` :boolean>)  PARTITIONED BY (`file_name` STRING)  stored as orc LOCATION 'some_test_path' TBLPROPERTIES ('compactiontable'='MINOR', 'transactional'='false')";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testInsert() {
    CompactionQueryBuilder queryBuilder = getMinorCompactionQueryBuilderForInsert();
    Table sourceTable = createSourceTable();
    queryBuilder.setSourceTabForInsert("comp_test_db.comp_test_insert_table");
    long[] abortedWriteIdList = {1111L, 2222L};
    ValidCompactorWriteIdList writeIds = new ValidCompactorWriteIdList("comp_test_source_table",
        abortedWriteIdList, null, 111111L);
    queryBuilder.setValidWriteIdList(writeIds);
    queryBuilder.setSourceTab(sourceTable);
    String query = queryBuilder.build();
    String expectedQuery = "INSERT into table comp_test_result_table select `operation`, `originalTransaction`, `bucket`, `rowId`, `currentTransaction`, `row` from comp_test_db.comp_test_insert_table  where `originalTransaction` not in (1111,2222)";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testInsertWithoutValidWriteIdsAndSourceTableForInsert() {
    CompactionQueryBuilder queryBuilder = getMinorCompactionQueryBuilderForInsert();
    Table sourceTable = createSourceTable();
    queryBuilder.setSourceTab(sourceTable);
    String query = queryBuilder.build();
    String expectedQuery = "INSERT into table comp_test_result_table select `operation`, `originalTransaction`, `bucket`, `rowId`, `currentTransaction`, `row` from comp_test_db.comp_test_source_table ";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testAlter() {
    CompactionQueryBuilder queryBuilder = getMinorCompactionQueryBuilderForAlter();
    AcidDirectory dir = createAcidDirectory();
    ValidCompactorWriteIdList writeIds = createWriteId(5L);
    queryBuilder.setValidWriteIdList(writeIds);
    queryBuilder.setDir(dir);
    queryBuilder.setIsDeleteDelta(true);
    String query = queryBuilder.build();
    String expectedQuery = "ALTER table comp_test_result_table add partition (file_name='test_delta_1') location '/compaction/test/table/test_delta_1' partition (file_name='test_delta_3') location '/compaction/test/table/test_delta_3' ";
    Assert.assertEquals(expectedQuery, query);
  }

  @Test
  public void testDrop() {
    CompactionQueryBuilder queryBuilder = getMinorCompactionQueryBuilderForDrop();
    String query = queryBuilder.build();
    String expectedQuery = "DROP table if exists comp_test_result_table";
    Assert.assertEquals(expectedQuery, query);
  }

  private CompactionQueryBuilder getMinorCompactionQueryBuilderForCreate() {
    return new CompactionQueryBuilder(CompactionType.MINOR, CompactionQueryBuilder.Operation.CREATE, false,
        RESULT_TABLE_NAME);
  }

  private CompactionQueryBuilder getMinorCompactionQueryBuilderForInsert() {
    return new CompactionQueryBuilder(CompactionType.MINOR, CompactionQueryBuilder.Operation.INSERT, false,
        RESULT_TABLE_NAME);
  }

  private CompactionQueryBuilder getMinorCompactionQueryBuilderForAlter() {
    return new CompactionQueryBuilder(CompactionType.MINOR, CompactionQueryBuilder.Operation.ALTER, false,
        RESULT_TABLE_NAME);
  }

  private CompactionQueryBuilder getMinorCompactionQueryBuilderForDrop() {
    return new CompactionQueryBuilder(CompactionType.MINOR, CompactionQueryBuilder.Operation.DROP, false,
        RESULT_TABLE_NAME);
  }
}
