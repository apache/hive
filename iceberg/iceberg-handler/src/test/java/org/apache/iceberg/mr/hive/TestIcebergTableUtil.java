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

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;

public class TestIcebergTableUtil {

  private static final Schema SCHEMA = new Schema(
      Types.NestedField.optional(1, "first_name", Types.StringType.get()),
      Types.NestedField.optional(2, "dept_id", Types.LongType.get())
  );

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void testGetPartitionSpecAmbiguousAfterPartitionEvolution() throws IOException {
    Table table = createV1Table();
    setPartitionSpec(table, "dept_id");
    setPartitionSpec(table);
    setPartitionSpec(table, "dept_id");

    // v1 evolution: spec 1 is identity(dept_id), spec 2 is void(dept_id); both share the same field name.
    assertEquals(4, table.specs().size());
    assertEquals("identity", table.specs().get(1).fields().get(0).transform().toString());
    assertEquals("void", table.specs().get(2).fields().get(0).transform().toString());

    AssertHelpers.assertThrows(
        "Should reject ambiguous partition spec resolution",
        HiveException.class,
        "Ambiguous partition spec for partition path dept_id=1: matched spec ids 1, 2",
        () -> IcebergTableUtil.getPartitionSpec(table, "dept_id=1"));
  }

  @Test
  public void testGetPartitionSpecReturnsUniqueMatch() throws Exception {
    Table table = createV1Table();
    setPartitionSpec(table, "dept_id");

    PartitionSpec result = IcebergTableUtil.getPartitionSpec(table, "dept_id=1");
    assertEquals(1, result.specId());
  }

  @Test
  public void testGetPartitionSpecNoMatchingSpec() throws IOException {
    Table table = createV1Table();

    AssertHelpers.assertThrows(
        "Should fail when no spec matches partition path fields",
        HiveException.class,
        "No matching partition spec found for partition path: dept_id=1",
        () -> IcebergTableUtil.getPartitionSpec(table, "dept_id=1"));
  }

  private Table createV1Table() throws IOException {
    File location = tmp.newFolder();
    Configuration conf = new Configuration();
    HadoopTables tables = new HadoopTables(conf);
    return tables.create(
        SCHEMA,
        PartitionSpec.unpartitioned(),
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION, "1",
            TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name()),
        location.getAbsolutePath());
  }

  /**
   * Mirrors Hive's {@code ALTER TABLE ... SET PARTITION SPEC (...)}: drop all current partition
   * fields, then add identity fields for the requested spec (empty varargs → unpartitioned).
   */
  private void setPartitionSpec(Table table, String... identityFields) {
    UpdatePartitionSpec update = table.updateSpec().caseSensitive(false);
    table.spec().fields().forEach(field -> update.removeField(field.name()));
    for (String field : identityFields) {
      update.addField(field);
    }
    update.commit();
    table.refresh();
  }
}
