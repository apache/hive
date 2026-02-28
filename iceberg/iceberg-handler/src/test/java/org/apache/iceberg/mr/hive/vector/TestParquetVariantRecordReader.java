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

package org.apache.iceberg.mr.hive.vector;

import java.util.List;
import java.util.Set;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.Schema;
import org.apache.iceberg.mr.hive.variant.VariantProjectionUtil.VariantColumnDescriptor;
import org.apache.iceberg.mr.hive.variant.VariantProjectionUtil.VariantProjection;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Assert;
import org.junit.Test;

public class TestParquetVariantRecordReader {

  private static final String SIMPLE_SCHEMA =
      "message root {" +
      "  optional group col {" +
      "    required binary metadata;" +
      "    required binary value;" +
      "    optional group typed_value {" +
      "      optional binary city (STRING);" +
      "      optional int32 age;" +
      "    }" +
      "  }" +
      "}";

  private static final String STRUCT_SCHEMA =
      "message root {" +
      "  optional group col {" +
      "    required binary metadata;" +
      "    required binary value;" +
      "    optional group typed_value {" +
      "      optional group address {" +
      "        optional binary city (STRING);" +
      "        optional int32 zip;" +
      "      }" +
      "      optional binary name (STRING);" +
      "    }" +
      "  }" +
      "}";

  @Test
  public void testProjectionExcludesUnprojectedColumns() {
    VariantProjection projection =
        createProjection(SIMPLE_SCHEMA, List.of("col.city"));

    Assert.assertNotNull(projection);
    Set<String> leaves = getLeafNames(projection.requestedColumns());

    Assert.assertTrue("Should include metadata", leaves.contains("metadata"));
    Assert.assertTrue("Should include value", leaves.contains("value"));
    Assert.assertTrue("Should include projected field city", leaves.contains("city"));
    Assert.assertFalse("Should NOT include unprojected field age", leaves.contains("age"));
    Assert.assertEquals("Total columns", 3, projection.requestedColumns().size());
  }

  @Test
  public void testNoProjectionFetchesAllColumns() {
    VariantProjection projection =
        createProjection(SIMPLE_SCHEMA, List.of());

    Assert.assertNotNull(projection);
    Assert.assertEquals("Should read all fields when no projection", 4, projection.requestedColumns().size());
  }

  @Test
  public void testProjectionCombinesSelectAndFilter() {
    // Simulates: SELECT variant_get(col, '$.city') WHERE variant_get(col, '$.age') > 30
    String schemaStr = "message root {" +
        "  optional group col {" +
        "    required binary metadata;" +
        "    required binary value;" +
        "    optional group typed_value {" +
        "      optional binary city (STRING);" +
        "      optional int32 age;" +
        "      optional binary name (STRING);" +
        "    }" +
        "  }" +
        "}";

    VariantProjection projection =
        createProjection(schemaStr, List.of("col.city", "col.age"));

    Assert.assertNotNull(projection);
    Set<String> leaves = getLeafNames(projection.requestedColumns());

    Assert.assertTrue("Should include city from SELECT", leaves.contains("city"));
    Assert.assertTrue("Should include age from WHERE", leaves.contains("age"));
    Assert.assertFalse("Should NOT include unreferenced name", leaves.contains("name"));
    Assert.assertEquals("Total columns", 4, projection.requestedColumns().size());
  }

  @Test
  public void testStructProjectionIncludesAllChildren() {
    VariantProjection projection =
        createProjection(STRUCT_SCHEMA, List.of("col.address"));

    Assert.assertNotNull(projection);
    Set<String> leaves = getLeafNames(projection.requestedColumns());

    Assert.assertTrue("Should include all children of address", leaves.contains("city"));
    Assert.assertTrue("Should include all children of address", leaves.contains("zip"));
    Assert.assertEquals("Total columns", 4, projection.requestedColumns().size());
  }

  @Test
  public void testProjectionOfFieldInStruct() {
    VariantProjection projection =
        createProjection(STRUCT_SCHEMA, List.of("col.address.city"));

    Assert.assertNotNull(projection);
    Set<String> leaves = getLeafNames(projection.requestedColumns());

    Assert.assertTrue("Should include address.city", leaves.contains("city"));
    Assert.assertFalse("Should NOT include address.zip", leaves.contains("zip"));
    Assert.assertFalse("Should NOT include name", leaves.contains("name"));
    Assert.assertEquals("Total columns", 3, projection.requestedColumns().size());
  }

  @Test
  public void testProjectionOfVariantFieldInStruct() {
    String schemaStr = "message root {" +
        "  optional group top_struct {" +
        "    optional group variant_col {" +
        "      required binary metadata;" +
        "      required binary value;" +
        "      optional group typed_value {" +
        "        optional binary foo (STRING);" +
        "        optional binary bar (STRING);" +
        "      }" +
        "    }" +
        "    optional int32 other_col;" +
        "  }" +
        "}";

    MessageType fileSchema = MessageTypeParser.parseMessageType(schemaStr);

    Schema icebergSchema = new Schema(
        Types.NestedField.optional(1, "top_struct", Types.StructType.of(
            Types.NestedField.optional(2, "variant_col", Types.VariantType.get()),
            Types.NestedField.optional(3, "other_col", Types.IntegerType.get())
        ))
    );

    JobConf job = new JobConf();
    job.set(org.apache.hadoop.hive.ql.io.IOConstants.COLUMNS, "top_struct");
    ColumnProjectionUtils.appendReadColumns(
        job,
        List.of(0),
        List.of("top_struct"),
        List.of("top_struct.variant_col.foo"),
        false);

    VariantProjection projection =
        VariantProjection.create(fileSchema, job, icebergSchema);

    Assert.assertNotNull(projection);
    Set<String> leaves = getLeafNames(projection.requestedColumns());

    Assert.assertTrue("Should include variant_col.foo", leaves.contains("foo"));
    Assert.assertFalse("Should NOT include variant_col.bar", leaves.contains("bar"));
    Assert.assertEquals("Total columns", 3, projection.requestedColumns().size());

    VariantColumnDescriptor desc = projection.variantColumns().get(0);
    Assert.assertArrayEquals("Field path should point to variant_col index", new int[]{0}, desc.fieldPath());
  }

  @Test
  public void testProjectionOnMultipleVariantColumns() {
    String schemaStr = "message root {" +
        "  optional group col1 {" +
        "    required binary metadata;" +
        "    required binary value;" +
        "    optional group typed_value {" +
        "      optional binary a (STRING);" +
        "      optional binary b (STRING);" +
        "    }" +
        "  }" +
        "  optional group col2 {" +
        "    required binary metadata;" +
        "    required binary value;" +
        "    optional group typed_value {" +
        "      optional int32 x;" +
        "      optional int32 y;" +
        "    }" +
        "  }" +
        "}";
    MessageType fileSchema = MessageTypeParser.parseMessageType(schemaStr);

    Schema icebergSchema = new Schema(
        Types.NestedField.optional(1, "col1", Types.VariantType.get()),
        Types.NestedField.optional(2, "col2", Types.VariantType.get())
    );

    JobConf job = new JobConf();
    job.set(org.apache.hadoop.hive.ql.io.IOConstants.COLUMNS, "col1,col2");
    ColumnProjectionUtils.appendReadColumns(
        job,
        List.of(0, 1),
        List.of("col1", "col2"),
        List.of("col1.a", "col2.x"),
        false);

    VariantProjection projection =
        VariantProjection.create(fileSchema, job, icebergSchema);

    Assert.assertNotNull(projection);

    Set<String> col1Leaves = Sets.newHashSet();
    Set<String> col2Leaves = Sets.newHashSet();

    for (ColumnDescriptor desc : projection.requestedColumns()) {
      String[] path = desc.getPath();
      String leaf = path[path.length - 1];

      if ("col1".equals(path[0])) {
        col1Leaves.add(leaf);
      } else if ("col2".equals(path[0])) {
        col2Leaves.add(leaf);
      }
    }

    Assert.assertTrue("col1 should include a", col1Leaves.contains("a"));
    Assert.assertFalse("col1 should NOT include b", col1Leaves.contains("b"));
    Assert.assertTrue("col2 should include x", col2Leaves.contains("x"));
    Assert.assertFalse("col2 should NOT include y", col2Leaves.contains("y"));
    Assert.assertEquals("Total columns", 6, projection.requestedColumns().size());
  }

  @Test
  public void testPrunedSchemaMatchesProjection() {
    VariantProjection projection =
        createProjection(SIMPLE_SCHEMA, List.of("col.city"));

    Assert.assertNotNull(projection);
    Assert.assertEquals(1, projection.variantColumns().size());

    VariantColumnDescriptor desc = projection.variantColumns().get(0);
    org.apache.parquet.schema.Type pruned = desc.prunedSchema();

    org.apache.parquet.schema.GroupType prunedGroup = pruned.asGroupType();
    Assert.assertTrue("Should contain metadata", prunedGroup.containsField("metadata"));
    Assert.assertTrue("Should contain value", prunedGroup.containsField("value"));

    org.apache.parquet.schema.GroupType typedValue = prunedGroup.getType("typed_value").asGroupType();
    Assert.assertTrue("Should contain city", typedValue.containsField("city"));
    Assert.assertFalse("Should NOT contain age", typedValue.containsField("age"));
  }

  @Test
  public void testProjectionSupportsTopLevelRename() {
    String schemaStr = "message root {" +
        "  optional group old_col = 1 {" + // Parquet has old_name
        "    required binary metadata;" +
        "    required binary value;" +
        "    optional group typed_value {" +
        "      optional binary city (STRING);" +
        "      optional int32 age;" +
        "    }" +
        "  }" +
        "}";
    MessageType fileSchema = MessageTypeParser.parseMessageType(schemaStr);

    // Iceberg has new_name with same ID
    Schema icebergSchema = new Schema(Types.NestedField.optional(1, "new_col", Types.VariantType.get()));

    JobConf job = new JobConf();
    job.set(org.apache.hadoop.hive.ql.io.IOConstants.COLUMNS, "new_col");
    ColumnProjectionUtils.appendReadColumns(
        job,
        List.of(0),
        List.of("new_col"),
        List.of("new_col.city"), // Hive asks for new name
        false);

    VariantProjection projection =
        VariantProjection.create(fileSchema, job, icebergSchema);

    Assert.assertNotNull(projection);
    Assert.assertEquals(1, projection.variantColumns().size());

    VariantColumnDescriptor desc = projection.variantColumns().get(0);
    // Path should be physical (resolved from rename)
    Assert.assertEquals("old_col", desc.physicalPath()[0]);

    org.apache.parquet.schema.Type pruned = desc.prunedSchema();
    // Pruned schema should be based on physical type but pruned using logical paths
    // The root name of pruned schema comes from physical type ("old_col")
    Assert.assertEquals("old_col", pruned.getName());

    org.apache.parquet.schema.GroupType typedValue = pruned.asGroupType().getType("typed_value").asGroupType();
    Assert.assertTrue("Should contain city", typedValue.containsField("city"));
    Assert.assertFalse("Should NOT contain age", typedValue.containsField("age"));
  }

  @Test
  public void testUnshreddedVariantIgnored() {
    String schemaStr = "message root {" +
        "  optional group col {" +
        "    required binary metadata;" +
        "    required binary value;" +
        "  }" +
        "}";
    MessageType fileSchema = MessageTypeParser.parseMessageType(schemaStr);
    Schema icebergSchema = new Schema(Types.NestedField.optional(1, "col", Types.VariantType.get()));

    JobConf job = new JobConf();
    job.set(org.apache.hadoop.hive.ql.io.IOConstants.COLUMNS, "col");
    // Even if we request a path, if it's unshredded, we can't project
    ColumnProjectionUtils.appendReadColumns(
        job, List.of(0), List.of("col"), List.of("col.city"), false);

    VariantProjection projection =
        VariantProjection.create(fileSchema, job, icebergSchema);

    // Should return null (no shredded columns found)
    Assert.assertNull("Should return null for unshredded variant", projection);
  }

  @Test
  public void testListProjectionPruning() {
    // Tests that projecting a field inside a LIST retrieves the entire list structure
    // but still prunes siblings outside the list.
    String schemaStr = "message root {" +
        "  optional group col {" +
        "    required binary metadata;" +
        "    required binary value;" +
        "    optional group typed_value {" +
        "      optional group my_list (LIST) {" +
        "        repeated group list {" +
        "          optional group element {" +
        "            optional int32 item_id;" +
        "            optional binary item_name (STRING);" +
        "          }" +
        "        }" +
        "      }" +
        "      optional int32 other_field;" +
        "    }" +
        "  }" +
        "}";

    VariantProjection projection =
        createProjection(schemaStr, List.of("col.my_list.item_id"));

    Assert.assertNotNull(projection);
    Set<String> leaves = getLeafNames(projection.requestedColumns());

    // Standard fields
    Assert.assertTrue("Should include metadata", leaves.contains("metadata"));
    Assert.assertTrue("Should include value", leaves.contains("value"));

    // List fields - ALL should be present because we don't prune inside LIST
    Assert.assertTrue("Should include requested item_id", leaves.contains("item_id"));
    Assert.assertTrue("Should include unrequested item_name (sibling in list)", leaves.contains("item_name"));

    // Outer sibling - SHOULD be pruned
    Assert.assertFalse("Should NOT include other_field", leaves.contains("other_field"));
  }

  @Test
  public void testMapProjectionPruning() {
    // Tests that projecting a field inside a MAP retrieves the entire map structure
    String schemaStr = "message root {" +
        "  optional group col {" +
        "    required binary metadata;" +
        "    required binary value;" +
        "    optional group typed_value {" +
        "      optional group my_map (MAP) {" +
        "        repeated group key_value {" +
        "          required binary key (STRING);" +
        "          optional group value {" +
        "             optional int32 map_val;" +
        "             optional int32 map_extra;" +
        "          }" +
        "        }" +
        "      }" +
        "      optional int32 other_field;" +
        "    }" +
        "  }" +
        "}";

    VariantProjection projection =
        createProjection(schemaStr, List.of("col.my_map.map_val"));

    Assert.assertNotNull(projection);
    Set<String> leaves = getLeafNames(projection.requestedColumns());

    // Map fields - ALL should be present
    Assert.assertTrue("Should include map key", leaves.contains("key"));
    Assert.assertTrue("Should include requested map_val", leaves.contains("map_val"));
    Assert.assertTrue("Should include unrequested map_extra", leaves.contains("map_extra"));

    // Outer sibling - SHOULD be pruned
    Assert.assertFalse("Should NOT include other_field", leaves.contains("other_field"));
  }

  private static VariantProjection createProjection(
      String schemaStr, List<String> nestedPaths) {
    MessageType fileSchema = MessageTypeParser.parseMessageType(schemaStr);
    Schema icebergSchema = new Schema(Types.NestedField.optional(1, "col", Types.VariantType.get()));

    JobConf job = new JobConf();
    job.set(org.apache.hadoop.hive.ql.io.IOConstants.COLUMNS, "col");
    ColumnProjectionUtils.appendReadColumns(job, List.of(0), List.of("col"), nestedPaths, false);

    return VariantProjection.create(fileSchema, job, icebergSchema);
  }

  private static Set<String> getLeafNames(List<ColumnDescriptor> requested) {
    Set<String> leaves = Sets.newHashSet();
    for (ColumnDescriptor desc : requested) {
      leaves.add(desc.getPath()[desc.getPath().length - 1]);
    }
    return leaves;
  }
}
