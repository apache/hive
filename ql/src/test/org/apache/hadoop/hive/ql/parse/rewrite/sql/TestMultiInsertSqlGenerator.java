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

package org.apache.hadoop.hive.ql.parse.rewrite.sql;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context.Operation;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestMultiInsertSqlGenerator {

  private static final List<FieldSchema> DATA_COLS = Arrays.asList(
      new FieldSchema("a", "int", null), new FieldSchema("b", "string", null));
  private static final List<FieldSchema> ALL_COLS = Arrays.asList(
      new FieldSchema("a", "int", null), new FieldSchema("b", "string", null),
      new FieldSchema("p", "string", null));
  private static final HiveConf CONF = new HiveConf();

  private static MultiInsertSqlGenerator generator(boolean nonNativePartitionSupport) {
    Table table = Mockito.mock(Table.class);
    Mockito.when(table.getCols()).thenReturn(DATA_COLS);
    Mockito.when(table.getAllCols()).thenReturn(ALL_COLS);
    Mockito.when(table.hasNonNativePartitionSupport()).thenReturn(nonNativePartitionSupport);
    return new MultiInsertSqlGenerator(table, "t", CONF, null) {
      @Override
      public void appendAcidSelectColumns(Operation operation) {
      }

      @Override
      public List<String> getDeleteValues(Operation operation) {
        return Collections.emptyList();
      }

      @Override
      public List<String> getSortKeys(Operation operation) {
        return Collections.emptyList();
      }
    };
  }

  /**
   * HIVE-29580: for natively partitioned tables the partition columns are emitted by
   * appendAcidSelectColumns, so they must be omitted here or the rewritten projection contains
   * duplicate column names.
   */
  @Test
  public void testAppendNonPartitionColsOmitsPartitionColsForNativeTable() {
    MultiInsertSqlGenerator generator = generator(false);
    generator.appendNonPartitionColsOfTargetTable();
    Assert.assertEquals("`a`, `b`", generator.toString());
  }

  @Test
  public void testAppendNonPartitionColsKeepsAllColsForNonNativeTable() {
    MultiInsertSqlGenerator generator = generator(true);
    generator.appendNonPartitionColsOfTargetTable();
    Assert.assertEquals("`a`, `b`, `p`", generator.toString());
  }
}
