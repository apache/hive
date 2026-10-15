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

package org.apache.hadoop.hive.ql.metadata;

import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestTableValidateColumns {

  private static List<FieldSchema> column(String type) {
    return List.of(new FieldSchema("c", type, null));
  }

  @ParameterizedTest
  @ValueSource(strings = {"variant", "array<variant>", "struct<a:int,v:variant>", "map<string,variant>",
      "uniontype<int,variant>", "array<struct<v:variant>>", "map<variant,string>"})
  void persistentNonIcebergTableRejectsVariantAtAnyDepth(String type) {
    HiveException e = assertThrows(HiveException.class,
        () -> Table.validateColumns(column(type), null, false, false));
    assertTrue(e.getMessage().contains("non-Iceberg tables"), e.getMessage());
    // an Iceberg table, and a view or temporary table, may hold it
    assertDoesNotThrow(() -> Table.validateColumns(column(type), null, true, false));
    assertDoesNotThrow(() -> Table.validateColumns(column(type), null, false, true));
  }

  @Test
  void variantAllowanceDoesNotCoverOtherIcebergOnlyTypes() {
    assertThrows(HiveException.class, () -> Table.validateColumns(column("unknown"), null, false, true));
  }

  @Test
  void aFieldNamedLikeTheTypeIsNoVariant() {
    assertDoesNotThrow(() -> Table.validateColumns(column("struct<variant_id:int>"), null, false, false));
  }

  @Test
  void checkValidityExemptsViewsAndTemporaryTables() {
    HiveConf conf = new HiveConf();
    HiveException e = assertThrows(HiveException.class, () -> variantTable(TableType.MANAGED_TABLE, false)
        .checkValidity(conf));
    assertTrue(e.getMessage().contains("non-Iceberg tables"), e.getMessage());
    assertDoesNotThrow(() -> variantTable(TableType.VIRTUAL_VIEW, false).checkValidity(conf));
    assertDoesNotThrow(() -> variantTable(TableType.MANAGED_TABLE, true).checkValidity(conf));
  }

  @Test
  void checkValidityRejectsANonNativeTemporaryTable() {
    Table table = variantTable(TableType.MANAGED_TABLE, true);
    table.setProperty(hive_metastoreConstants.META_TABLE_STORAGE, DefaultStorageHandler.class.getName());
    HiveException e = assertThrows(HiveException.class, () -> table.checkValidity(new HiveConf()));
    assertTrue(e.getMessage().contains("non-Iceberg tables"), e.getMessage());
  }

  @Test
  void partitionOfTemporaryTableMayHoldVariant() throws HiveException {
    assertDoesNotThrow(() -> variantPartition(true, false).checkValidity());
    HiveException e = assertThrows(HiveException.class, () -> variantPartition(false, false).checkValidity());
    assertTrue(e.getMessage().contains("non-Iceberg tables"), e.getMessage());
    e = assertThrows(HiveException.class, () -> variantPartition(true, true).checkValidity());
    assertTrue(e.getMessage().contains("non-Iceberg tables"), e.getMessage());
  }

  private static Partition variantPartition(boolean temporary, boolean nonNative) throws HiveException {
    Table table = variantTable(TableType.MANAGED_TABLE, temporary);
    if (nonNative) {
      table.setProperty(hive_metastoreConstants.META_TABLE_STORAGE, DefaultStorageHandler.class.getName());
    }
    table.setPartCols(List.of(new FieldSchema("p", "int", null)));
    org.apache.hadoop.hive.metastore.api.Partition tPartition = new org.apache.hadoop.hive.metastore.api.Partition();
    tPartition.setValues(List.of("1"));
    tPartition.setSd(table.getSd().deepCopy());
    // a location of its own makes the descriptor differ from the table's, so checkValidity validates the columns
    tPartition.getSd().setLocation("/warehouse/t/p=1");
    Partition partition = new Partition(table, tPartition);
    assertEquals(1, partition.getCols().size());
    return partition;
  }

  private static Table variantTable(TableType type, boolean temporary) {
    Table table = new Table("default", "t");
    table.setTableType(type);
    table.setTemporary(temporary);
    table.setFields(column("array<variant>"));
    if (type == TableType.VIRTUAL_VIEW) {
      table.setViewOriginalText("SELECT 1");
      table.setViewExpandedText("SELECT 1");
    }
    return table;
  }
}
