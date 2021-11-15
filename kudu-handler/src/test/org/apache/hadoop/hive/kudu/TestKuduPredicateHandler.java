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
package org.apache.hadoop.hive.kudu;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseCompare;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hive.kudu.KuduHiveUtils.toHiveType;
import static org.apache.hadoop.hive.kudu.KuduStorageHandler.KUDU_MASTER_ADDRS_KEY;
import static org.apache.hadoop.hive.kudu.KuduStorageHandler.KUDU_TABLE_NAME_KEY;
import static org.apache.hadoop.hive.kudu.KuduTestUtils.getAllTypesSchema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the KuduPredicateHandler implementation.
 */
public class TestKuduPredicateHandler {

  private static final String TABLE_NAME = "default.TestKuduPredicateHandler";

  private static final Schema SCHEMA = getAllTypesSchema();

  private static final Configuration BASE_CONF = new Configuration();

  private static final long NOW_MS = System.currentTimeMillis();

  private static final PartialRow ROW;
  static {
    ROW = SCHEMA.newPartialRow();
    ROW.addByte("key", (byte) 1);
    ROW.addShort("int16", (short) 1);
    ROW.addInt("int32", 1);
    ROW.addLong("int64", 1L);
    ROW.addBoolean("bool", true);
    ROW.addFloat("float", 1.1f);
    ROW.addDouble("double", 1.1d);
    ROW.addString("string", "one");
    ROW.addBinary("binary", "one".getBytes(UTF_8));
    ROW.addTimestamp("timestamp", new Timestamp(NOW_MS));
    ROW.addDecimal("decimal", new BigDecimal("1.111"));
    ROW.setNull("null");
    // Not setting the "default" column.
  }

  private static final List<GenericUDF> COMPARISON_UDFS = Arrays.asList(
      new GenericUDFOPEqual(),
      new GenericUDFOPLessThan(),
      new GenericUDFOPEqualOrLessThan(),
      new GenericUDFOPGreaterThan(),
      new GenericUDFOPEqualOrGreaterThan()
  );

  private static final List<GenericUDF> NULLABLE_UDFS = Arrays.asList(
      new GenericUDFOPNull(),
      new GenericUDFOPNotNull()
  );

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Before
  public void setUp() throws Exception {
    // Set the base configuration values.
    BASE_CONF.set(KUDU_MASTER_ADDRS_KEY, harness.getMasterAddressesAsString());
    BASE_CONF.set(KUDU_TABLE_NAME_KEY, TABLE_NAME);
    BASE_CONF.set(FileInputFormat.INPUT_DIR, "dummy");

    // Create the test Kudu table.
    CreateTableOptions options = new CreateTableOptions()
        .setRangePartitionColumns(ImmutableList.of("key"));
    harness.getClient().createTable(TABLE_NAME, SCHEMA, options);
  }

  @Test
  public void testComparisonPredicates() throws Exception {
    for (ColumnSchema col : SCHEMA.getColumns()) {
      // Skip null and default columns because they don't have a value to use.
      if (col.getName().equals("null") || col.getName().equals("default")) {
        continue;
      }
      PrimitiveTypeInfo typeInfo = toHiveType(col.getType(), col.getTypeAttributes());
      ExprNodeDesc colExpr =  new ExprNodeColumnDesc(typeInfo, col.getName(), null, false);
      ExprNodeDesc constExpr = new ExprNodeConstantDesc(typeInfo, ROW.getObject(col.getName()));
      List<ExprNodeDesc> children = Lists.newArrayList();
      children.add(colExpr);
      children.add(constExpr);
      for (GenericUDF udf : COMPARISON_UDFS) {
        ExprNodeGenericFuncDesc predicateExpr =
            new ExprNodeGenericFuncDesc(typeInfo, udf, children);

        // Verify KuduPredicateHandler.decompose
        HiveStoragePredicateHandler.DecomposedPredicate decompose =
            KuduPredicateHandler.decompose(predicateExpr, SCHEMA);

        // Binary predicates are not supported. (HIVE-11370)
        if (col.getName().equals("binary")) {
          assertNull(decompose);
        } else {
          assertNotNull(String.format("Unsupported comparison UDF and type (%s, %s)", udf, typeInfo),
              decompose);
          assertNotNull(String.format("Unsupported comparison UDF and type (%s, %s)", udf, typeInfo),
              decompose.pushedPredicate);
          assertNull(String.format("Unsupported comparison UDF and type (%s, %s)", udf, typeInfo),
              decompose.residualPredicate);

          List<KuduPredicate> predicates = expressionToPredicates(predicateExpr);
          assertEquals(1, predicates.size());
          scanWithPredicates(predicates);
        }
      }
    }
  }

  @Test
  public void testNotComparisonPredicates() throws Exception {
    for (ColumnSchema col : SCHEMA.getColumns()) {
      // Skip null and default columns because they don't have a value to use.
      // Skip binary columns because binary predicates are not supported. (HIVE-11370)
      if (col.getName().equals("null") || col.getName().equals("default") ||
          col.getName().equals("binary")) {
        continue;
      }
      PrimitiveTypeInfo typeInfo = toHiveType(col.getType(), col.getTypeAttributes());
      ExprNodeDesc colExpr =  new ExprNodeColumnDesc(typeInfo, col.getName(), null, false);
      ExprNodeDesc constExpr = new ExprNodeConstantDesc(typeInfo, ROW.getObject(col.getName()));
      List<ExprNodeDesc> children = Lists.newArrayList();
      children.add(colExpr);
      children.add(constExpr);

      for (GenericUDF udf : COMPARISON_UDFS) {
        ExprNodeGenericFuncDesc childExpr =
            new ExprNodeGenericFuncDesc(typeInfo, udf, children);

        List<ExprNodeDesc> notChildren = Lists.newArrayList();
        notChildren.add(childExpr);
        ExprNodeGenericFuncDesc predicateExpr = new ExprNodeGenericFuncDesc(typeInfo,
            new GenericUDFOPNot(), notChildren);

        // Verify KuduPredicateHandler.decompose
        HiveStoragePredicateHandler.DecomposedPredicate decompose =
            KuduPredicateHandler.decompose(predicateExpr, SCHEMA);
        // See note in KuduPredicateHandler.newAnalyzer.
        assertNull(decompose);

        List<KuduPredicate> predicates = expressionToPredicates(predicateExpr);
        if (udf instanceof GenericUDFOPEqual) {
          // Kudu doesn't support !=.
          assertTrue(predicates.isEmpty());
        } else {
          assertEquals(1, predicates.size());
          scanWithPredicates(predicates);
        }
      }
    }
  }

  @Test
  public void testInPredicates() throws Exception {
    PrimitiveTypeInfo typeInfo = toHiveType(Type.STRING, null);
    ExprNodeDesc colExpr =  new ExprNodeColumnDesc(typeInfo, "string", null, false);
    ExprNodeConstantDesc constDesc = new ExprNodeConstantDesc("Alpha");
    ExprNodeConstantDesc constDesc2 = new ExprNodeConstantDesc("Bravo");
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(colExpr);
    children.add(constDesc);
    children.add(constDesc2);

    ExprNodeGenericFuncDesc predicateExpr =
        new ExprNodeGenericFuncDesc(typeInfo, new GenericUDFIn(), children);

    // Verify KuduPredicateHandler.decompose
    HiveStoragePredicateHandler.DecomposedPredicate decompose =
        KuduPredicateHandler.decompose(predicateExpr, SCHEMA);
    // See note in KuduPredicateHandler.newAnalyzer.
    assertNull(decompose);

    List<KuduPredicate> predicates = expressionToPredicates(predicateExpr);
    assertEquals(1, predicates.size());
    scanWithPredicates(predicates);

    // Also test NOT IN.
    List<ExprNodeDesc> notChildren = Lists.newArrayList();
    notChildren.add(predicateExpr);
    ExprNodeGenericFuncDesc notPredicateExpr = new ExprNodeGenericFuncDesc(typeInfo,
        new GenericUDFOPNot(), notChildren);

    // Verify KuduPredicateHandler.decompose
    HiveStoragePredicateHandler.DecomposedPredicate decomposeNot =
        KuduPredicateHandler.decompose(notPredicateExpr, SCHEMA);
    // See note in KuduPredicateHandler.newAnalyzer.
    assertNull(decomposeNot);

    List<KuduPredicate> notPredicates = expressionToPredicates(notPredicateExpr);
    assertEquals(0, notPredicates.size());
  }

  @Test
  public void testNullablePredicates() throws Exception {
    PrimitiveTypeInfo typeInfo = toHiveType(Type.STRING, null);
    ExprNodeDesc colExpr =  new ExprNodeColumnDesc(typeInfo, "null", null, false);
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(colExpr);

    for (GenericUDF udf : NULLABLE_UDFS) {
      ExprNodeGenericFuncDesc predicateExpr =
          new ExprNodeGenericFuncDesc(typeInfo, udf, children);

      // Verify KuduPredicateHandler.decompose
      HiveStoragePredicateHandler.DecomposedPredicate decompose =
          KuduPredicateHandler.decompose(predicateExpr, SCHEMA);
      // See note in KuduPredicateHandler.newAnalyzer.
      assertNull(decompose);

      List<KuduPredicate> predicates = expressionToPredicates(predicateExpr);
      assertEquals(1, predicates.size());
      scanWithPredicates(predicates);
    }
  }

  @Test
  public void testAndPredicates() throws Exception {
    for (ColumnSchema col : SCHEMA.getColumns()) {
      // Skip null and default columns because they don't have a value to use.
      if (col.getName().equals("null") || col.getName().equals("default")) {
        continue;
      }
      PrimitiveTypeInfo typeInfo = toHiveType(col.getType(), col.getTypeAttributes());
      ExprNodeDesc colExpr =  new ExprNodeColumnDesc(typeInfo, col.getName(), null, false);
      ExprNodeDesc constExpr = new ExprNodeConstantDesc(typeInfo, ROW.getObject(col.getName()));
      List<ExprNodeDesc> children = Lists.newArrayList();
      children.add(colExpr);
      children.add(constExpr);

      ExprNodeGenericFuncDesc gePredicateExpr =
          new ExprNodeGenericFuncDesc(typeInfo, new GenericUDFOPEqualOrGreaterThan(), children);
      ExprNodeGenericFuncDesc lePredicateExpr =
          new ExprNodeGenericFuncDesc(typeInfo, new GenericUDFOPEqualOrLessThan(), children);

      List<ExprNodeDesc> andChildren = Lists.newArrayList();
      andChildren.add(gePredicateExpr);
      andChildren.add(lePredicateExpr);
      ExprNodeGenericFuncDesc andPredicateExpr = new ExprNodeGenericFuncDesc(typeInfo,
          new GenericUDFOPAnd(), andChildren);

      // Verify KuduPredicateHandler.decompose
      HiveStoragePredicateHandler.DecomposedPredicate decompose =
          KuduPredicateHandler.decompose(andPredicateExpr, SCHEMA);

      // Binary predicates are not supported. (HIVE-11370)
      if (col.getName().equals("binary")) {
        assertNull(decompose);
      } else {
        assertNotNull(decompose);
        assertNotNull(decompose.pushedPredicate);
        assertNull(decompose.residualPredicate);

        List<KuduPredicate> predicates = expressionToPredicates(decompose.pushedPredicate);
        assertEquals(2, predicates.size());
        scanWithPredicates(predicates);

        // Also test NOT AND.
        List<ExprNodeDesc> notChildren = Lists.newArrayList();
        notChildren.add(andPredicateExpr);
        ExprNodeGenericFuncDesc notPredicateExpr = new ExprNodeGenericFuncDesc(typeInfo,
            new GenericUDFOPNot(), notChildren);

        // Verify KuduPredicateHandler.decompose
        HiveStoragePredicateHandler.DecomposedPredicate decomposeNot =
            KuduPredicateHandler.decompose(notPredicateExpr, SCHEMA);
        // See note in KuduPredicateHandler.newAnalyzer.
        assertNull(decomposeNot);

        List<KuduPredicate> notPredicates = expressionToPredicates(notPredicateExpr);
        assertEquals(0, notPredicates.size());
      }
    }
  }

  @Test
  public void testOrPredicates() throws Exception {
    for (ColumnSchema col : SCHEMA.getColumns()) {
      // Skip null and default columns because they don't have a value to use.
      // Skip binary columns because binary predicates are not supported. (HIVE-11370)
      if (col.getName().equals("null") || col.getName().equals("default") ||
          col.getName().equals("binary")) {
        continue;
      }
      PrimitiveTypeInfo typeInfo = toHiveType(col.getType(), col.getTypeAttributes());
      ExprNodeDesc colExpr =  new ExprNodeColumnDesc(typeInfo, col.getName(), null, false);
      ExprNodeDesc constExpr = new ExprNodeConstantDesc(typeInfo, ROW.getObject(col.getName()));
      List<ExprNodeDesc> children = Lists.newArrayList();
      children.add(colExpr);
      children.add(constExpr);

      ExprNodeGenericFuncDesc gePredicateExpr =
          new ExprNodeGenericFuncDesc(typeInfo, new GenericUDFOPEqualOrGreaterThan(), children);
      ExprNodeGenericFuncDesc lePredicateExpr =
          new ExprNodeGenericFuncDesc(typeInfo, new GenericUDFOPEqualOrLessThan(), children);

      List<ExprNodeDesc> orChildren = Lists.newArrayList();
      orChildren.add(gePredicateExpr);
      orChildren.add(lePredicateExpr);
      ExprNodeGenericFuncDesc predicateExpr = new ExprNodeGenericFuncDesc(typeInfo,
          new GenericUDFOPOr(), orChildren);

      // Verify KuduPredicateHandler.decompose
      HiveStoragePredicateHandler.DecomposedPredicate decompose =
          KuduPredicateHandler.decompose(predicateExpr, SCHEMA);
      // OR predicates are currently not supported.
      assertNull(decompose);
      List<KuduPredicate> predicates = expressionToPredicates(predicateExpr);
      assertEquals(0, predicates.size());

      // Also test NOT OR.
      List<ExprNodeDesc> notChildren = Lists.newArrayList();
      notChildren.add(predicateExpr);
      ExprNodeGenericFuncDesc notPredicateExpr = new ExprNodeGenericFuncDesc(typeInfo,
          new GenericUDFOPNot(), notChildren);

      // Verify KuduPredicateHandler.decompose
      HiveStoragePredicateHandler.DecomposedPredicate decomposeNot =
          KuduPredicateHandler.decompose(notPredicateExpr, SCHEMA);
      // See note in KuduPredicateHandler.newAnalyzer.
      assertNull(decomposeNot);

      List<KuduPredicate> notPredicates = expressionToPredicates(notPredicateExpr);
      assertEquals(2, notPredicates.size());
    }
  }

  @Test
  public void testMixedPredicates() throws Exception {
    for (ColumnSchema col : SCHEMA.getColumns()) {
      // Skip null and default columns because they don't have a value to use.
      // Skip binary columns because binary predicates are not supported. (HIVE-11370)
      if (col.getName().equals("null") || col.getName().equals("default") ||
          col.getName().equals("binary")) {
        continue;
      }
      PrimitiveTypeInfo typeInfo = toHiveType(col.getType(), col.getTypeAttributes());
      ExprNodeDesc colExpr =  new ExprNodeColumnDesc(typeInfo, col.getName(), null, false);
      ExprNodeDesc constExpr = new ExprNodeConstantDesc(typeInfo, ROW.getObject(col.getName()));
      List<ExprNodeDesc> children = Lists.newArrayList();
      children.add(colExpr);
      children.add(constExpr);

      ExprNodeGenericFuncDesc supportedPredicateExpr =
          new ExprNodeGenericFuncDesc(typeInfo, new GenericUDFOPEqualOrGreaterThan(), children);
      ExprNodeGenericFuncDesc unsupportedPredicateExpr =
          new ExprNodeGenericFuncDesc(typeInfo, new GenericUDFOPUnsupported(), children);

      List<ExprNodeDesc> andChildren = Lists.newArrayList();
      andChildren.add(supportedPredicateExpr);
      andChildren.add(unsupportedPredicateExpr);
      ExprNodeGenericFuncDesc andPredicateExpr = new ExprNodeGenericFuncDesc(typeInfo,
          new GenericUDFOPAnd(), andChildren);

      // Verify KuduPredicateHandler.decompose
      HiveStoragePredicateHandler.DecomposedPredicate decompose =
          KuduPredicateHandler.decompose(andPredicateExpr, SCHEMA);
      assertNotNull(decompose);
      assertNotNull(decompose.pushedPredicate);
      assertNotNull(decompose.residualPredicate);

      List<KuduPredicate> predicates = expressionToPredicates(decompose.pushedPredicate);
      assertEquals(1, predicates.size());
      scanWithPredicates(predicates);
    }
  }

  private List<KuduPredicate> expressionToPredicates(ExprNodeGenericFuncDesc predicateExpr) {
    String filterExpr = SerializationUtilities.serializeExpression(predicateExpr);
    Configuration conf = new Configuration();
    conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);
    return KuduPredicateHandler.getPredicates(conf, SCHEMA);
  }

  private void scanWithPredicates(List<KuduPredicate> predicates)
      throws KuduException {
    // Scan the table with the predicate to be sure there are no exceptions.
    KuduClient client = harness.getClient();
    KuduTable table = client.openTable(TABLE_NAME);
    KuduScanner.KuduScannerBuilder builder = client.newScannerBuilder(table);
    for (KuduPredicate predicate : predicates) {
      builder.addPredicate(predicate);
    }
    KuduScanner scanner = builder.build();
    while (scanner.hasMoreRows()) {
      scanner.nextRows();
    }
  }

  // Wrapper implementation to simplify testing unsupported UDFs.
  private class GenericUDFOPUnsupported extends GenericUDFBaseCompare {
    GenericUDFOPUnsupported() {
      this.opName = "UNSUPPORTED";
      this.opDisplayName = "UNSUPPORTED";
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) {
      return null;
    }

    @Override
    public GenericUDF flip() {
      return new GenericUDFOPUnsupported();
    }

    @Override
    public GenericUDF negative() {
      return new GenericUDFOPUnsupported();
    }
  }
}
