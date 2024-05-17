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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidCompactorWriteIdList;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;

public class CompactionQueryBuilderTestBase {

  public static final String DB_NAME = "comp_test_db";
  public static final String SOURCE_TABLE_NAME = "comp_test_source_table";
  public static final String RESULT_TABLE_NAME = "comp_test_result_table";
  public static final String COLUMN_1_NAME = "column_1";
  public static final String COLUMN_2_NAME = "column_2";
  public static final String COLUMN_3_NAME = "column_3";
  public static final String SOME_TEST_LOCATION = "some_test_path";
  public static final String COMP_TEST_SOURCE_TABLE_FOR_INSERT = "comp_test_db.comp_test_insert_table";

  public Table createSourceTable() {
    return createSourceTable(false, false, false);
  }

  public Table createSourceTableWithProperties() {
    return createSourceTable(true, false, false);
  }

  public Table createSourceTableBucketed() {
    return createSourceTable(false, true, false);
  }

  public Table createSourceTableBucketedSorted() {
    return createSourceTable(false, true, true);
  }

  private Table createSourceTable(boolean addTableProperties, boolean bucketed, boolean sorted) {
    Table sourceTable = new Table();
    sourceTable.setDbName(DB_NAME);
    sourceTable.setTableName(SOURCE_TABLE_NAME);

    StorageDescriptor sd = new StorageDescriptor();
    List<FieldSchema> columns = new ArrayList<>();
    FieldSchema col1 = new FieldSchema(COLUMN_1_NAME, "string", "First column");
    FieldSchema col2 = new FieldSchema(COLUMN_2_NAME, "int", null);
    FieldSchema col3 = new FieldSchema(COLUMN_3_NAME, "boolean", "Third column");
    columns.add(col1);
    columns.add(col2);
    columns.add(col3);
    sd.setCols(columns);

    if (bucketed) {
      sd.addToBucketCols(COLUMN_1_NAME);
      sd.addToBucketCols(COLUMN_3_NAME);
      sd.setNumBuckets(4);
    } else {
      sd.setBucketCols(Collections.emptyList());
    }

    if (sorted) {
      sd.addToSortCols(new Order(COLUMN_1_NAME, 0));
      sd.addToSortCols(new Order(COLUMN_2_NAME, 1));
    } else {
      sd.setSortCols(Collections.emptyList());
    }

    Map<String, String> parameters = new HashMap<>();
    if (addTableProperties) {
      parameters.put("property_1", "true");
      parameters.put("orc.property_2", "44");
      parameters.put("COLUMN_STATS_ACCURATE", "false");
      parameters.put("columns.types", "test");
    }
    sourceTable.setParameters(parameters);
    sourceTable.setSd(sd);
    return sourceTable;
  }

  protected AcidDirectory createAcidDirectory() {
    AcidDirectory dir = Mockito.mock(AcidDirectory.class);
    AcidUtils.ParsedDelta d1 = Mockito.mock(AcidUtils.ParsedDelta.class);
    AcidUtils.ParsedDelta d2 = Mockito.mock(AcidUtils.ParsedDelta.class);
    AcidUtils.ParsedDelta d3 = Mockito.mock(AcidUtils.ParsedDelta.class);
    AcidUtils.ParsedDelta d4 = Mockito.mock(AcidUtils.ParsedDelta.class);
    AcidUtils.ParsedDelta d5 = Mockito.mock(AcidUtils.ParsedDelta.class);

    List<AcidUtils.ParsedDelta> dirs = new ArrayList<>();
    dirs.add(d1);
    dirs.add(d2);
    dirs.add(d3);
    dirs.add(d4);
    dirs.add(d5);

    when(dir.getCurrentDirectories()).thenReturn(dirs);
    when(d1.isDeleteDelta()).thenReturn(true);
    when(d1.getMinWriteId()).thenReturn(7L);
    when(d1.getMaxWriteId()).thenReturn(11L);
    when(d1.getPath()).thenReturn(new Path("/compaction/test/table", "test_delta_1"));
    when(d2.isDeleteDelta()).thenReturn(true);
    when(d2.getMinWriteId()).thenReturn(1L);
    when(d2.getMaxWriteId()).thenReturn(11L);
    when(d2.getPath()).thenReturn(new Path("/compaction/test/table", "test_delta_2"));
    when(d3.isDeleteDelta()).thenReturn(true);
    when(d3.getMinWriteId()).thenReturn(5L);
    when(d3.getMaxWriteId()).thenReturn(15L);
    when(d3.getPath()).thenReturn(new Path("/compaction/test/table", "test_delta_3"));
    when(d4.isDeleteDelta()).thenReturn(true);
    when(d4.getMinWriteId()).thenReturn(7L);
    when(d4.getMaxWriteId()).thenReturn(20L);
    when(d4.getPath()).thenReturn(new Path("/compaction/test/table", "test_delta_4"));
    when(d5.isDeleteDelta()).thenReturn(false);
    when(d5.getMinWriteId()).thenReturn(6L);
    when(d5.getMaxWriteId()).thenReturn(11L);
    when(d5.getPath()).thenReturn(new Path("/compaction/test/table", "test_delta_5"));
    return dir;
  }

  protected ValidCompactorWriteIdList createWriteId(long minWriteId) {
    long[] abortedWriteIdList = { 1111L };
    return new ValidCompactorWriteIdList("comp_test_source_table", abortedWriteIdList, null, 15L, minWriteId);
  }
}
