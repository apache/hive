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

package org.apache.hadoop.hive.ql.metadata;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelDigest;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.IncrementalRebuildMode;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * Functional and parallel execution tests for {@link MaterializedViewsCache}.
 * Parallel execution test is disabled by default.
 */
class TestMaterializedViewsCache {
  private MaterializedViewsCache materializedViewsCache;
  private Table defaultMV1;
  private HiveRelOptMaterialization defaultMaterialization1;
  private Table defaultMV1Same;
  private HiveRelOptMaterialization defaultMaterialization1Same;
  private Table defaultMVUpCase;
  private HiveRelOptMaterialization defaultMaterializationUpCase;
  private Table db1MV1;
  private HiveRelOptMaterialization db1Materialization1;

  @BeforeEach
  void setUp() throws ParseException {
    defaultMV1 = getTable("default", "mat1", "select col0 from t1 where col0 = 'foo'");
    defaultMaterialization1 = createMaterialization(defaultMV1);
    defaultMV1Same = getTable("default", "mat_same", "select col0 from t1 where col0 = 'foo'");
    defaultMaterialization1Same = createMaterialization(defaultMV1Same);
    defaultMVUpCase = getTable("default", "mat2", "select col0 from t1 where col0 = 'FOO'");
    defaultMaterializationUpCase = createMaterialization(defaultMVUpCase);
    db1MV1 = getTable("db1", "mat1", "select col0 from t1 where col0 = 'foo'");
    db1Materialization1 = createMaterialization(db1MV1);

    materializedViewsCache = new MaterializedViewsCache();
  }

  @Test
  void testEmptyCache() {
    MaterializedViewsCache emptyCache = new MaterializedViewsCache();

    ASTNode any = (ASTNode) ParseDriver.adaptor.create(HiveParser.Identifier, "any");

    assertThat(emptyCache.get(any).isEmpty(), is(true));
    assertThat(emptyCache.isEmpty(), is(true));
    assertThat(emptyCache.values().isEmpty(), is(true));
  }

  @Test
  void testGetByTableNameFromEmptyCache() {
    MaterializedViewsCache emptyCache = new MaterializedViewsCache();

    assertThat(emptyCache.get(defaultMV1.getDbName(), defaultMV1.getTableName()), is(nullValue()));
  }

  @Test
  void testQueryDoesNotMatchAnyMVDefinition() {
    materializedViewsCache.putIfAbsent(defaultMV1, defaultMaterialization1);

    ASTNode notFound = (ASTNode) ParseDriver.adaptor.create(HiveParser.Identifier, "notFound");

    assertThat(materializedViewsCache.get(notFound).isEmpty(), is(true));
    assertThat(materializedViewsCache.values().size(), is(1));
  }

  @Test
  void testAdd() {
    materializedViewsCache.putIfAbsent(defaultMV1, defaultMaterialization1);

    assertThat(materializedViewsCache.get(defaultMV1.getDbName(), defaultMV1.getTableName()), is(defaultMaterialization1));
    assertThat(materializedViewsCache.get(defaultMaterialization1.getAst()).size(), is(1));
    assertThat(materializedViewsCache.get(defaultMaterialization1.getAst()).get(0), is(defaultMaterialization1));
    assertThat(materializedViewsCache.isEmpty(), is(false));
    assertThat(materializedViewsCache.values().size(), is(1));
    assertThat(materializedViewsCache.values().get(0), is(defaultMaterialization1));
  }

  @Test
  void testAddSameMVTwice() {
    materializedViewsCache.putIfAbsent(defaultMV1, defaultMaterialization1);
    materializedViewsCache.putIfAbsent(defaultMV1, defaultMaterialization1);

    assertThat(materializedViewsCache.get(defaultMV1.getDbName(), defaultMV1.getTableName()), is(defaultMaterialization1));
    assertThat(materializedViewsCache.get(defaultMaterialization1.getAst()).size(), is(1));
    assertThat(materializedViewsCache.get(defaultMaterialization1.getAst()).get(0), is(defaultMaterialization1));
    assertThat(materializedViewsCache.values().size(), is(1));
    assertThat(materializedViewsCache.values().get(0), is(defaultMaterialization1));
  }

  private Table getTable(String db, String tableName, String definition) {
    Table table = new Table(new org.apache.hadoop.hive.metastore.api.Table());
    table.setDbName(db);
    table.setTableName(tableName);
    table.setViewExpandedText(definition);
    return table;
  }

  private static HiveRelOptMaterialization createMaterialization(Table table) throws ParseException {
    return new HiveRelOptMaterialization(
            new DummyRel(table), new DummyRel(table), null, asList(table.getDbName(), table.getTableName()),
            RewriteAlgorithm.ALL,
            IncrementalRebuildMode.AVAILABLE, ParseUtils.parse(table.getViewExpandedText(), null));
  }

  @Test
  void testAddMVsWithSameDefinition() {
    materializedViewsCache.putIfAbsent(defaultMV1, defaultMaterialization1);
    materializedViewsCache.putIfAbsent(defaultMV1Same, defaultMaterialization1Same);

    assertThat(materializedViewsCache.get(defaultMV1.getDbName(), defaultMV1.getTableName()), is(defaultMaterialization1));
    assertThat(materializedViewsCache.get(defaultMV1Same.getDbName(), defaultMV1Same.getTableName()), is(defaultMaterialization1Same));
    assertThat(materializedViewsCache.get(defaultMaterialization1.getAst()).size(), is(2));
    assertThat(materializedViewsCache.get(defaultMaterialization1.getAst()), hasItem(defaultMaterialization1));
    assertThat(materializedViewsCache.get(defaultMaterialization1.getAst()), hasItem(defaultMaterialization1Same));
    assertThat(materializedViewsCache.isEmpty(), is(false));
    assertThat(materializedViewsCache.values().size(), is(2));
  }

  @Test
  void testAddMVsWithSameDefinitionButDifferentDatabase() {
    materializedViewsCache.putIfAbsent(defaultMV1, defaultMaterialization1);
    materializedViewsCache.putIfAbsent(db1MV1, db1Materialization1);

    assertThat(materializedViewsCache.get(defaultMV1.getDbName(), defaultMV1.getTableName()), is(defaultMaterialization1));
    assertThat(materializedViewsCache.get(db1MV1.getDbName(), db1MV1.getTableName()), is(db1Materialization1));
    assertThat(materializedViewsCache.get(defaultMaterialization1.getAst()).size(), is(2));
    assertThat(materializedViewsCache.get(defaultMaterialization1.getAst()), hasItem(defaultMaterialization1));
    assertThat(materializedViewsCache.get(defaultMaterialization1.getAst()), hasItem(db1Materialization1));
    assertThat(materializedViewsCache.isEmpty(), is(false));
    assertThat(materializedViewsCache.values().size(), is(2));
  }

  @Test
  void testLookupByTextIsCaseSensitive() {
    materializedViewsCache.putIfAbsent(defaultMV1, defaultMaterialization1);
    materializedViewsCache.putIfAbsent(defaultMVUpCase, defaultMaterializationUpCase);

    assertThat(materializedViewsCache.get(defaultMaterialization1.getAst()).size(), is(1));
    assertThat(materializedViewsCache.get(defaultMaterialization1.getAst()), hasItem(defaultMaterialization1));
    assertThat(materializedViewsCache.isEmpty(), is(false));
    assertThat(materializedViewsCache.values().size(), is(2));
  }

  @Test
  void testRefreshWhenMVWasNotCached() {
    materializedViewsCache.refresh(defaultMV1, defaultMV1, defaultMaterialization1);

    assertThat(materializedViewsCache.get(defaultMV1.getDbName(), defaultMV1.getTableName()), is(defaultMaterialization1));
    assertThat(materializedViewsCache.get(defaultMaterialization1.getAst()).size(), is(1));
    assertThat(materializedViewsCache.get(defaultMaterialization1.getAst()), hasItem(defaultMaterialization1));
    assertThat(materializedViewsCache.isEmpty(), is(false));
    assertThat(materializedViewsCache.values().size(), is(1));
    assertThat(materializedViewsCache.values(), hasItem(defaultMaterialization1));
  }

  @Test
  void testRefreshWhenMVIsCachedButWasUpdated() throws ParseException {
    materializedViewsCache.putIfAbsent(defaultMV1, defaultMaterialization1);
    HiveRelOptMaterialization newMaterialization = createMaterialization(defaultMV1);
    materializedViewsCache.refresh(defaultMV1, defaultMV1, newMaterialization);

    assertThat(newMaterialization, is(not(defaultMaterialization1)));
    assertThat(materializedViewsCache.get(defaultMV1.getDbName(), defaultMV1.getTableName()), is(newMaterialization));
    assertThat(materializedViewsCache.get(defaultMaterialization1.getAst()).size(), is(1));
    assertThat(materializedViewsCache.get(defaultMaterialization1.getAst()), hasItem(newMaterialization));
    assertThat(materializedViewsCache.isEmpty(), is(false));
    assertThat(materializedViewsCache.values().size(), is(1));
    assertThat(materializedViewsCache.values(), hasItem(newMaterialization));
  }

  @Test
  void testRefreshWhenMVRefersToANewMaterialization() {
    materializedViewsCache.putIfAbsent(defaultMV1, defaultMaterialization1);
    materializedViewsCache.refresh(defaultMV1Same, defaultMV1, defaultMaterialization1);

    assertThat(materializedViewsCache.get(defaultMV1.getDbName(), defaultMV1.getTableName()), is(defaultMaterialization1));
    assertThat(materializedViewsCache.get(defaultMaterialization1.getAst()).size(), is(1));
    assertThat(materializedViewsCache.get(defaultMaterialization1.getAst()), hasItem(defaultMaterialization1));
    assertThat(materializedViewsCache.isEmpty(), is(false));
    assertThat(materializedViewsCache.values().size(), is(1));
    assertThat(materializedViewsCache.values(), hasItem(defaultMaterialization1));
  }

  @Test
  void testRemoveByTable() {
    materializedViewsCache.putIfAbsent(defaultMV1, defaultMaterialization1);

    materializedViewsCache.remove(defaultMV1);

    assertThat(materializedViewsCache.get(defaultMV1.getDbName(), defaultMV1.getTableName()), is(nullValue()));
    assertThat(materializedViewsCache.get(defaultMaterialization1.getAst()).isEmpty(), is(true));
    assertThat(materializedViewsCache.isEmpty(), is(true));
    assertThat(materializedViewsCache.values().isEmpty(), is(true));
  }

  @Test
  void testRemoveByTableName() {
    materializedViewsCache.putIfAbsent(defaultMV1, defaultMaterialization1);

    materializedViewsCache.remove(defaultMV1.getDbName(), defaultMV1.getTableName());

    assertThat(materializedViewsCache.get(defaultMV1.getDbName(), defaultMV1.getTableName()), is(nullValue()));
    assertThat(materializedViewsCache.get(defaultMaterialization1.getAst()).isEmpty(), is(true));
    assertThat(materializedViewsCache.isEmpty(), is(true));
    assertThat(materializedViewsCache.values().isEmpty(), is(true));
  }

  @Disabled("Testing parallelism only")
  @Test
  void testParallelism() throws ParseException {
    int ITERATIONS = 1000000;

    List<Pair<Table, HiveRelOptMaterialization>> testData = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      Table table = new Table(new org.apache.hadoop.hive.metastore.api.Table());
      table.setDbName("default");
      table.setTableName("mat" + i);
      table.setViewExpandedText("select col0 from t" + i);
      HiveRelOptMaterialization materialization = createMaterialization(table);
      testData.add(new Pair<>(table, materialization));
    }
    for (int i = 0; i < 10; ++i) {
      Table table = new Table(new org.apache.hadoop.hive.metastore.api.Table());
      table.setDbName("db1");
      table.setTableName("mat" + i);
      table.setViewExpandedText("select col0 from t" + i);
      HiveRelOptMaterialization materialization = createMaterialization(table);
      testData.add(new Pair<>(table, materialization));
    }

    List<Callable<Void>> callableList = new ArrayList<>();
    callableList.add(() -> {
      for (Pair<Table, HiveRelOptMaterialization> entry : testData) {
        materializedViewsCache.refresh(entry.left, entry.left, entry.right);
      }
      return null;
    });
    callableList.add(() -> {
      for (int j = 0; j < ITERATIONS; ++j) {
        for (Pair<Table, HiveRelOptMaterialization> entry : testData) {
          materializedViewsCache.remove(entry.left);
          materializedViewsCache.putIfAbsent(entry.left, entry.right);
        }
      }
      return null;
    });
    for (Pair<Table, HiveRelOptMaterialization> entry : testData) {
      callableList.add(() -> {
        for (int j = 0; j < ITERATIONS; ++j) {
          materializedViewsCache.get(entry.right.getAst());
        }
        return null;
      });
    }
    callableList.add(() -> {
      for (int j = 0; j < ITERATIONS; ++j) {
        List<HiveRelOptMaterialization> materializations = materializedViewsCache.values();
      }
      return null;
    });


    ExecutorService executor = Executors.newFixedThreadPool(12);
    try {
      executor.invokeAll(callableList);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private static class DummyRel implements RelNode {

    private final RelOptHiveTable dummyTable;

    public DummyRel(Table table) {
      this.dummyTable = new RelOptHiveTable(null, null,
              singletonList(table.getDbName() + "." + table.getTableName()), null, table,
              emptyList(), emptyList(), emptyList(), null, null, null,
              null, null, null);
    }

    @Override
    public RelOptTable getTable() {
      return dummyTable;
    }

    @Override
    public Convention getConvention() {
      return null;
    }

    @Override
    public String getCorrelVariable() {
      return null;
    }

    @Override
    public RelNode getInput(int i) {
      return null;
    }

    @Override
    public int getId() {
      return 0;
    }

    @Override
    public String getDigest() {
      return null;
    }

    @Override
    public RelDigest getRelDigest() {
      return null;
    }

    @Override
    public RelTraitSet getTraitSet() {
      return null;
    }

    @Override
    public RelDataType getRowType() {
      return null;
    }

    @Override
    public String getDescription() {
      return null;
    }

    @Override
    public RelDataType getExpectedInputRowType(int i) {
      return null;
    }

    @Override
    public List<RelNode> getInputs() {
      return null;
    }

    @Override
    public RelOptCluster getCluster() {
      return null;
    }

    @Override
    public double estimateRowCount(RelMetadataQuery relMetadataQuery) {
      return 0;
    }

    @Override
    public Set<CorrelationId> getVariablesSet() {
      return null;
    }

    @Override
    public void collectVariablesUsed(Set<CorrelationId> set) {

    }

    @Override
    public void collectVariablesSet(Set<CorrelationId> set) {

    }

    @Override
    public void childrenAccept(RelVisitor relVisitor) {

    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner relOptPlanner, RelMetadataQuery relMetadataQuery) {
      return null;
    }

    @Override
    public <M extends Metadata> M metadata(Class<M> aClass, RelMetadataQuery relMetadataQuery) {
      return null;
    }

    @Override
    public void explain(RelWriter relWriter) {

    }

    @Override
    public RelNode onRegister(RelOptPlanner relOptPlanner) {
      return null;
    }

    @Override
    public void recomputeDigest() {
    }

    @Override
    public boolean deepEquals(Object obj) {
      return false;
    }

    @Override
    public int deepHashCode() {
      return 0;
    }

    @Override
    public void replaceInput(int i, RelNode relNode) {

    }

    @Override
    public String getRelTypeName() {
      return null;
    }

    @Override
    public boolean isValid(Litmus litmus, Context context) {
      return false;
    }

    @Override
    public RelNode copy(RelTraitSet relTraitSet, List<RelNode> list) {
      return null;
    }

    @Override
    public void register(RelOptPlanner relOptPlanner) {

    }

    @Override
    public RelNode accept(RelShuttle relShuttle) {
      return null;
    }

    @Override
    public RelNode accept(RexShuttle rexShuttle) {
      return null;
    }
  }
}
