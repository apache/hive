/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive;

import java.util.List;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.relocated.com.google.common.base.Throwables;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;

/**
 * Tests Merge Related SQL features.
 */
public class TestHiveIcebergMerge extends HiveIcebergStorageHandlerWithEngineBase {

  static final Schema SCHEMA = new Schema(
      optional(1, "a", Types.IntegerType.get()),
      optional(2, "b", Types.IntegerType.get(), "This is first name")
  );

  static final List<Record> TGT_RECORDS = TestHelper.RecordsBuilder.newInstance(SCHEMA)
      .add(0, 1)
      .add(9, 9)
      .build();


  @Override
  protected void validateTestParams() {
  }

  @Test
  public void testMergeIntoOnClauseColumnsNoAssignedTables() {
    testTables.createTable(shell, "tgt", SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, TGT_RECORDS, formatVersion);
    shell.executeStatement("alter table default.tgt set tblproperties('write.merge.mode'='copy-on-write')");
    shell.executeStatement("drop table if exists default.src");
    shell.executeStatement("create table default.src (a int, b int) stored as orc");
    shell.executeStatement("use default");
    String sql = "MERGE INTO tgt using src on a= src.a when matched then update set b=10";
    try {
      shell.executeStatement(sql);
      Assert.assertFalse(true); // place we should not touch
    } catch (Throwable ex) {
      Throwable cause = Throwables.getRootCause(ex);
      Assert.assertTrue(cause instanceof SemanticException);
    }
  }

  @Test
  public void testMergeIntoOnClauseColumns() {
    testTables.createTable(shell, "tgt", SCHEMA,
            PartitionSpec.unpartitioned(), fileFormat, TGT_RECORDS, formatVersion);
    shell.executeStatement("alter table default.tgt set tblproperties('write.merge.mode'='copy-on-write')");
    shell.executeStatement("drop table if exists default.src");
    shell.executeStatement("create table default.src (a int, b int) stored as orc");
    shell.executeStatement("insert into table default.src select 0, 10");
    shell.executeStatement("use default");
    shell.executeStatement("merge into tgt using src on tgt.a = src.a when matched then update set b=src.b");
    List<Object[]> objects = shell.executeStatement("SELECT * FROM tgt ORDER BY a");
    Assert.assertEquals(2, objects.size());
    List<Record> expected = TestHelper.RecordsBuilder.newInstance(SCHEMA)
            .add(0, 10)
            .add(9, 9)
            .build();
    HiveIcebergTestUtils.validateData(expected,
            HiveIcebergTestUtils.valueForRow(SCHEMA, objects), 0);
  }
}
