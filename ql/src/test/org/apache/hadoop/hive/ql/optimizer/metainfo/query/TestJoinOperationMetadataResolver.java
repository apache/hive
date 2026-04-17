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

package org.apache.hadoop.hive.ql.optimizer.metainfo.query;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link JoinOperationMetadataResolver}.
 * <p>
 * All Hive operator/plan objects are mocked so the test has no Hadoop/Hive
 * runtime dependency.
 */
class TestJoinOperationMetadataResolver {

  /**
   * Tracks the intermediate "schema-parent" operator created by rsWithColumnKey/rsWithColumnKeys
   * so that attachTso() can wire the TSO into the correct position in the chain.
   */
  private final Map<ReduceSinkOperator, Operator<OperatorDesc>> schemaParents = new HashMap<>();
  private JoinOperationMetadataResolver resolver;

  @BeforeEach
  void setUp() {
    resolver = new JoinOperationMetadataResolver();
    schemaParents.clear();
  }

  @Test
  void resolveJoinMetadataNullParentsLeavesArraysNull() {
    JoinOperator join = mock(JoinOperator.class);
    when(join.getParentOperators()).thenReturn(null);

    resolver.resolveJoinMetadata(join);

    assertNull(resolver.getKeyNames(), "keyNames should remain null when parent list is null");
    assertNull(resolver.getTableAliases(), "tableAliases should remain null when parent list is null");
  }

  @Test
  void resolveJoinMetadataEmptyParentsLeavesArraysNull() {
    JoinOperator join = mock(JoinOperator.class);
    when(join.getParentOperators()).thenReturn(Collections.emptyList());

    resolver.resolveJoinMetadata(join);

    assertNull(resolver.getKeyNames(), "keyNames should remain null when parent list is empty");
    assertNull(resolver.getTableAliases(), "tableAliases should remain null when parent list is empty");
  }

  @Test
  void resolveJoinMetadataNullParentSlotProducesUnknown() {
    JoinOperator join = mock(JoinOperator.class);
    when(join.getParentOperators()).thenReturn(Collections.singletonList(null));

    resolver.resolveJoinMetadata(join);

    assertEquals("unknown", resolver.getKeyNames()[0]);
    assertEquals("unknown", resolver.getTableAliases()[0]);
  }

  @Test
  void resolveTableAliasPrefersTsoAliasOverTableName() {
    RowSchema schema = mock(RowSchema.class);
    ReduceSinkOperator rs = rsWithColumnKey("_col0", schema);
    attachTso(rs, "a", "orders");

    JoinOperator join = joinOp(rs);
    resolver.resolveJoinMetadata(join);

    assertEquals("a", resolver.getTableAliases()[0], "Should prefer the TSO alias over the physical table name");
  }

  @Test
  void resolveTableAliasFallsBackToTableNameWhenAliasBlank() {
    RowSchema schema = mock(RowSchema.class);
    ReduceSinkOperator rs = rsWithColumnKey("_col0", schema);
    attachTso(rs, "", "orders");

    JoinOperator join = joinOp(rs);
    resolver.resolveJoinMetadata(join);

    assertEquals("orders", resolver.getTableAliases()[0], "Should fall back to table name when alias is blank");
  }

  @Test
  void resolveTableAliasUnknownWhenNoTsoFound() {
    // Parent is not a ReduceSinkOperator (no TS reachable)
    Operator<?> oddParent = mock(Operator.class);
    when(oddParent.getParentOperators()).thenReturn(Collections.emptyList());

    JoinOperator join = joinOp(oddParent);
    resolver.resolveJoinMetadata(join);

    assertEquals("unknown", resolver.getTableAliases()[0]);
  }

  @Test
  void resolveKeyNameColumnRefResolvesToSchemaAlias() {
    ColumnInfo ci = mock(ColumnInfo.class);
    when(ci.getAlias()).thenReturn("key");

    RowSchema schema = mock(RowSchema.class);
    when(schema.getColumnInfo("_col0")).thenReturn(ci);

    ReduceSinkOperator rs = rsWithColumnKey("_col0", schema);

    // attach a TSO so table alias is set too
    attachTso(rs, "a", "t");

    resolver.resolveJoinMetadata(joinOp(rs));

    assertEquals("key", resolver.getKeyNames()[0]);
  }

  @Test
  void resolveKeyNameColumnRefFallsBackToInternalNameWhenNoSchemaAlias() {
    ColumnInfo ci = mock(ColumnInfo.class);
    when(ci.getAlias()).thenReturn(""); // empty alias

    RowSchema schema = mock(RowSchema.class);
    when(schema.getColumnInfo("_col0")).thenReturn(ci);

    ReduceSinkOperator rs = rsWithColumnKey("_col0", schema);
    attachTso(rs, "a", "t");

    resolver.resolveJoinMetadata(joinOp(rs));

    assertEquals("_col0", resolver.getKeyNames()[0]);
  }

  @Test
  void resolveKeyNameColumnRefFallsBackToInternalNameWhenColumnInfoNull() {
    RowSchema schema = mock(RowSchema.class);
    when(schema.getColumnInfo("_col0")).thenReturn(null);

    ReduceSinkOperator rs = rsWithColumnKey("_col0", schema);
    attachTso(rs, "a", "t");

    resolver.resolveJoinMetadata(joinOp(rs));

    assertEquals("_col0", resolver.getKeyNames()[0]);
  }

  @Test
  void resolveKeyNameColumnRefUnknownWhenInternalNameNull() {
    ExprNodeColumnDesc keyExpr = mock(ExprNodeColumnDesc.class);
    when(keyExpr.getColumn()).thenReturn(null);

    ReduceSinkDesc rsConf = mock(ReduceSinkDesc.class);
    when(rsConf.getKeyCols()).thenReturn(Collections.singletonList(keyExpr));

    ReduceSinkOperator rs = mock(ReduceSinkOperator.class);
    when(rs.getConf()).thenReturn(rsConf);
    when(rs.getParentOperators()).thenReturn(Collections.emptyList());

    resolver.resolveJoinMetadata(joinOp(rs));

    assertEquals("unknown", resolver.getKeyNames()[0]);
  }

  @Test
  void resolveKeyNameExprKeyUsesExprString() {
    ReduceSinkOperator rs = rsWithExprKey("upper(val)");

    resolver.resolveJoinMetadata(joinOp(rs));

    assertEquals("upper(val)", resolver.getKeyNames()[0]);
  }

  @Test
  void resolveKeyNameExprKeyUnknownWhenExprStringNull() {
    ExprNodeDesc keyExpr = mock(ExprNodeDesc.class);
    when(keyExpr.getExprString()).thenReturn(null);

    ReduceSinkDesc rsConf = mock(ReduceSinkDesc.class);
    when(rsConf.getKeyCols()).thenReturn(Collections.singletonList(keyExpr));

    ReduceSinkOperator rs = mock(ReduceSinkOperator.class);
    when(rs.getConf()).thenReturn(rsConf);
    when(rs.getParentOperators()).thenReturn(Collections.emptyList());

    resolver.resolveJoinMetadata(joinOp(rs));

    assertEquals("unknown", resolver.getKeyNames()[0]);
  }

  @Test
  void resolveKeyNameUnknownWhenReduceSinkConfNull() {
    ReduceSinkOperator rs = mock(ReduceSinkOperator.class);
    when(rs.getConf()).thenReturn(null);
    when(rs.getParentOperators()).thenReturn(Collections.emptyList());

    resolver.resolveJoinMetadata(joinOp(rs));

    assertEquals("unknown", resolver.getKeyNames()[0]);
  }

  @Test
  void resolveKeyNameUnknownWhenKeyColsNull() {
    ReduceSinkDesc rsConf = mock(ReduceSinkDesc.class);
    when(rsConf.getKeyCols()).thenReturn(null);

    ReduceSinkOperator rs = mock(ReduceSinkOperator.class);
    when(rs.getConf()).thenReturn(rsConf);
    when(rs.getParentOperators()).thenReturn(Collections.emptyList());

    resolver.resolveJoinMetadata(joinOp(rs));

    assertEquals("unknown", resolver.getKeyNames()[0]);
  }

  @Test
  void resolveKeyNameUnknownWhenKeyColsEmpty() {
    ReduceSinkDesc rsConf = mock(ReduceSinkDesc.class);
    when(rsConf.getKeyCols()).thenReturn(Collections.emptyList());

    ReduceSinkOperator rs = mock(ReduceSinkOperator.class);
    when(rs.getConf()).thenReturn(rsConf);
    when(rs.getParentOperators()).thenReturn(Collections.emptyList());

    resolver.resolveJoinMetadata(joinOp(rs));

    assertEquals("unknown", resolver.getKeyNames()[0]);
  }

  @Test
  void resolveKeyNameCompoundKeyJoinedWithCommaSpace() {
    ColumnInfo ci1 = mock(ColumnInfo.class);
    when(ci1.getAlias()).thenReturn("dept_id");
    ColumnInfo ci2 = mock(ColumnInfo.class);
    when(ci2.getAlias()).thenReturn("year");

    RowSchema schema = mock(RowSchema.class);
    when(schema.getColumnInfo("_col0")).thenReturn(ci1);
    when(schema.getColumnInfo("_col1")).thenReturn(ci2);

    ReduceSinkOperator rs = rsWithColumnKeys(Arrays.asList("_col0", "_col1"), schema);
    attachTso(rs, "a", "t");

    resolver.resolveJoinMetadata(joinOp(rs));

    assertEquals("dept_id, year", resolver.getKeyNames()[0]);
  }

  @Test
  void resolveJoinMetadataThreePositionsPopulatesAllThreeSlots() {
    String[] tables = {"employees", "salaries", "departments"};
    String[] aliases = {"e", "s", "d"};
    String[] internalCols = {"_col0", "_col0", "_col0"};
    String[] expectedKeys = {"emp_id", "sid", "dept_id"};

    ReduceSinkOperator[] rsList = new ReduceSinkOperator[expectedKeys.length];
    for (int i = 0; i < expectedKeys.length; i++) {
      ColumnInfo ci = mock(ColumnInfo.class);
      when(ci.getAlias()).thenReturn(expectedKeys[i]);

      RowSchema schema = mock(RowSchema.class);
      when(schema.getColumnInfo(internalCols[i])).thenReturn(ci);

      ReduceSinkOperator rs = rsWithColumnKey(internalCols[i], schema);
      attachTso(rs, aliases[i], tables[i]);
      rsList[i] = rs;
    }

    resolver.resolveJoinMetadata(joinOp(rsList[0], rsList[1], rsList[2]));

    assertArrayEquals(expectedKeys, resolver.getKeyNames());
    assertArrayEquals(aliases, resolver.getTableAliases());
  }

  @Test
  void resolveJoinMetadataArraySizeMatchesParentCount() {
    ReduceSinkOperator rs0 = rsWithExprKey("id");
    ReduceSinkOperator rs1 = rsWithExprKey("id");
    ReduceSinkOperator rs2 = rsWithExprKey("id");

    resolver.resolveJoinMetadata(joinOp(rs0, rs1, rs2));

    assertEquals(3, resolver.getKeyNames().length);
    assertEquals(3, resolver.getTableAliases().length);
  }

  // helpers

  /**
   * Build a mocked ReduceSinkOperator whose key list is a single ExprNodeColumnDesc.
   * <p>
   * The RS parent is a generic operator that carries {@code inputSchema}.
   * When {@link #attachTso} is later called on the RS, it places the TSO as the
   * parent of <em>that schema operator</em> (not directly as parent of RS), so
   * that both the schema and the upstream TSO remain reachable for traversal.
   */
  private ReduceSinkOperator rsWithColumnKey(String internalColName, RowSchema inputSchema) {
    ExprNodeColumnDesc keyExpr = mock(ExprNodeColumnDesc.class);
    when(keyExpr.getColumn()).thenReturn(internalColName);

    ReduceSinkDesc rsConf = mock(ReduceSinkDesc.class);
    when(rsConf.getKeyCols()).thenReturn(Collections.singletonList(keyExpr));

    // schemaParent is the direct parent of RS; it exposes the input schema and
    // will have the TSO attached as its own parent by attachTso().
    @SuppressWarnings("unchecked")
    Operator<OperatorDesc> schemaParent = mock(Operator.class);
    when(schemaParent.getSchema()).thenReturn(inputSchema);
    when(schemaParent.getParentOperators()).thenReturn(Collections.emptyList());

    @SuppressWarnings("unchecked")
    ReduceSinkOperator rs = mock(ReduceSinkOperator.class);
    when(rs.getConf()).thenReturn(rsConf);
    when(rs.getParentOperators()).thenReturn(Collections.singletonList(schemaParent));
    // store schemaParent so attachTso() can wire the TSO into it
    schemaParents.put(rs, schemaParent);
    return rs;
  }

  /**
   * Build a mocked ReduceSinkOperator with multiple key columns.
   */
  private ReduceSinkOperator rsWithColumnKeys(List<String> internalColNames, RowSchema inputSchema) {
    List<ExprNodeDesc> keyExprs = new java.util.ArrayList<>();
    for (String name : internalColNames) {
      ExprNodeColumnDesc keyExpr = mock(ExprNodeColumnDesc.class);
      when(keyExpr.getColumn()).thenReturn(name);
      keyExprs.add(keyExpr);
    }

    ReduceSinkDesc rsConf = mock(ReduceSinkDesc.class);
    when(rsConf.getKeyCols()).thenReturn(keyExprs);

    @SuppressWarnings("unchecked")
    Operator<OperatorDesc> schemaParent = mock(Operator.class);
    when(schemaParent.getSchema()).thenReturn(inputSchema);
    when(schemaParent.getParentOperators()).thenReturn(Collections.emptyList());

    @SuppressWarnings("unchecked")
    ReduceSinkOperator rs = mock(ReduceSinkOperator.class);
    when(rs.getConf()).thenReturn(rsConf);
    when(rs.getParentOperators()).thenReturn(Collections.singletonList(schemaParent));
    schemaParents.put(rs, schemaParent);
    return rs;
  }

  /**
   * Build a mocked ReduceSinkOperator whose key is a generic (non-column) expression.
   */
  private ReduceSinkOperator rsWithExprKey(String exprString) {
    ExprNodeDesc keyExpr = mock(ExprNodeDesc.class);
    when(keyExpr.getExprString()).thenReturn(exprString);

    ReduceSinkDesc rsConf = mock(ReduceSinkDesc.class);
    when(rsConf.getKeyCols()).thenReturn(Collections.singletonList(keyExpr));

    @SuppressWarnings("unchecked")
    ReduceSinkOperator rs = mock(ReduceSinkOperator.class);
    when(rs.getConf()).thenReturn(rsConf);
    when(rs.getParentOperators()).thenReturn(Collections.emptyList());
    return rs;
  }

  /**
   * Attach a mocked TableScanOperator into the upstream chain of {@code rs}.
   * <p>
   * The resolver resolves the table name by calling
   * {@code OperatorUtils.findSingleOperatorUpstream(parent, TableScanOperator.class)},
   * where {@code parent} is the direct parent of the join-input RS.
   * That traversal starts at the RS, checks if it is a TSO, then recurses into
   * {@code rs.getParentOperators()}.  The RS's direct parent is the schema-carrying
   * operator created by {@code rsWithColumnKey}; we wire the TSO as <em>that</em>
   * operator's parent so the BFS finds it without disturbing the schema lookup.
   * <p>
   * Chain: Join → RS → schemaParent → TSO
   */
  @SuppressWarnings("unchecked")
  private TableScanOperator attachTso(ReduceSinkOperator rs, String alias, String tableName) {
    TableScanDesc tsoConf = mock(TableScanDesc.class);
    when(tsoConf.getAlias()).thenReturn(alias);
    when(tsoConf.getTableName()).thenReturn(tableName);

    TableScanOperator tso = mock(TableScanOperator.class);
    when(tso.getConf()).thenReturn(tsoConf);
    when(tso.getParentOperators()).thenReturn(Collections.emptyList());

    // Wire TSO as parent of the schema-parent so findSingleOperatorUpstream finds it.
    Operator<OperatorDesc> schemaParent = schemaParents.get(rs);
    if (schemaParent != null) {
      when(schemaParent.getParentOperators()).thenReturn(Collections.singletonList(tso));
    }
    return tso;
  }

  /**
   * Build a JoinOperator mock whose parent list is the supplied operators.
   */
  @SuppressWarnings("unchecked")
  private JoinOperator joinOp(Operator<?>... parents) {
    JoinOperator join = mock(JoinOperator.class);
    when(join.getParentOperators()).thenReturn(Arrays.asList(parents));
    return join;
  }
}

