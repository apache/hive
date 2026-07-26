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

import java.util.Collection;
import java.util.List;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.mr.hive.test.TestTables.TestTableType;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestHiveIcebergUnknownType extends HiveIcebergStorageHandlerWithEngineBase {

  @Parameters(name = "fileFormat={0}, catalog={1}, isVectorized={2}, formatVersion={3}")
  public static Collection<Object[]> parameters() {
    return HiveIcebergStorageHandlerWithEngineBase.getParameters(p ->
        p.fileFormat() == FileFormat.PARQUET &&
        p.testTableType() == TestTableType.HIVE_CATALOG &&
        p.formatVersion() == 3);
  }

  @Test
  public void testUnknownTypeReadWrite() {
    Schema schema = new Schema(
        optional(1, "id", Types.IntegerType.get()),
        optional(2, "placeholder", Types.UnknownType.get()));

    TableIdentifier table = TableIdentifier.of("default", "unknown_type_basic");

    testTables.createTable(shell, table.name(),
        schema, org.apache.iceberg.PartitionSpec.unpartitioned(), fileFormat, ImmutableList.of(), formatVersion,
        ImmutableMap.of());

    shell.executeStatement(
        String.format("INSERT INTO %s VALUES (1, NULL), (2, NULL)", table));

    List<Object[]> rows = shell.executeStatement(
        String.format("SELECT id, placeholder FROM %s ORDER BY id", table));

    Assert.assertEquals(2, rows.size());
    Assert.assertEquals(1, rows.get(0)[0]);
    Assert.assertNull(rows.get(0)[1]);
    Assert.assertEquals(2, rows.get(1)[0]);
    Assert.assertNull(rows.get(1)[1]);
  }

  @Test
  public void testUnknownTypeDdl() {
    TableIdentifier table = TableIdentifier.of("default", "unknown_type_ddl");

    shell.executeStatement(String.format(
        "CREATE EXTERNAL TABLE %s (id INT, placeholder UNKNOWN) STORED BY ICEBERG " +
            "TBLPROPERTIES ('format-version'='3')", table));

    shell.executeStatement(String.format("INSERT INTO %s VALUES (1, NULL)", table));

    List<Object[]> rows = shell.executeStatement(
        String.format("SELECT id, placeholder FROM %s", table));

    Assert.assertEquals(1, rows.size());
    Assert.assertEquals(1, rows.get(0)[0]);
    Assert.assertNull(rows.get(0)[1]);
  }
}
