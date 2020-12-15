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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptQuery;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
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
  private RelOptMaterialization defaultRelOptMaterialization1;
  private Table defaultMV2;
  private RelOptMaterialization defaultRelOptMaterialization2;
  private Table db1MV1;
  private RelOptMaterialization db1RelOptMaterialization1;

  @BeforeEach
  void setUp() {
    defaultMV1 = getTable("default", "mat1", "select col0 from t1 where col0 = 'foo'");
    defaultRelOptMaterialization1 = createRelOptMaterialization(defaultMV1);
    defaultMV2 = getTable("default", "mat2", "select col0 from t1 where col0 = 'FOO'");
    defaultRelOptMaterialization2 = createRelOptMaterialization(defaultMV2);
    db1MV1 = getTable("db1", "mat1", "select col0 from t1 where col0 = 'foo'");
    db1RelOptMaterialization1 = createRelOptMaterialization(db1MV1);

    materializedViewsCache = new MaterializedViewsCache();
  }

  @Test
  void testEmptyCache() {
    MaterializedViewsCache emptyCache = new MaterializedViewsCache();

    assertThat(emptyCache.get("select 'any definition'").isEmpty(), is(true));
    assertThat(emptyCache.values().isEmpty(), is(true));
  }

  @Test
  void testGetByTableNameFromEmptyCache() {
    MaterializedViewsCache emptyCache = new MaterializedViewsCache();

    assertThat(emptyCache.get(defaultMV1.getDbName(), defaultMV1.getTableName()), is(nullValue()));
  }

  @Test
  void testQueryDoesNotMatchAnyMVDefinition() {
    materializedViewsCache.putIfAbsent(defaultMV1, defaultRelOptMaterialization1);

    assertThat(materializedViewsCache.get("select 'not found'").isEmpty(), is(true));
    assertThat(materializedViewsCache.values().size(), is(1));
  }

  @Test
  void testAdd() {
    materializedViewsCache.putIfAbsent(defaultMV1, defaultRelOptMaterialization1);

    assertThat(materializedViewsCache.get(defaultMV1.getDbName(), defaultMV1.getTableName()), is(defaultRelOptMaterialization1));
    assertThat(materializedViewsCache.get(defaultMV1.getViewExpandedText()).size(), is(1));
    assertThat(materializedViewsCache.get(defaultMV1.getViewExpandedText()).get(0), is(defaultRelOptMaterialization1));
    assertThat(materializedViewsCache.values().size(), is(1));
    assertThat(materializedViewsCache.values().get(0), is(defaultRelOptMaterialization1));
  }

  private Table getTable(String db, String tableName, String definition) {
    Table table = new Table(new org.apache.hadoop.hive.metastore.api.Table());
    table.setDbName(db);
    table.setTableName(tableName);
    table.setViewExpandedText(definition);
    return table;
  }

  private static RelOptMaterialization createRelOptMaterialization(Table table) {
    return new RelOptMaterialization(
            new DummyRel(table), new DummyRel(table), null, asList(table.getDbName(), table.getTableName()));
  }

  @Test
  void testAddMVsWithSameDefinition() {
    materializedViewsCache.putIfAbsent(defaultMV1, defaultRelOptMaterialization1);
    materializedViewsCache.putIfAbsent(defaultMV2, defaultRelOptMaterialization2);

    assertThat(materializedViewsCache.get(defaultMV1.getDbName(), defaultMV1.getTableName()), is(defaultRelOptMaterialization1));
    assertThat(materializedViewsCache.get(defaultMV2.getDbName(), defaultMV2.getTableName()), is(defaultRelOptMaterialization2));
    assertThat(materializedViewsCache.get(defaultMV1.getViewExpandedText()).size(), is(1));
    assertThat(materializedViewsCache.get(defaultMV1.getViewExpandedText()), hasItem(defaultRelOptMaterialization1));
    assertThat(materializedViewsCache.values().size(), is(2));
  }

  @Test
  void testAddMVsWithSameDefinitionButDifferentDatabase() {
    materializedViewsCache.putIfAbsent(defaultMV1, defaultRelOptMaterialization1);
    materializedViewsCache.putIfAbsent(db1MV1, db1RelOptMaterialization1);

    assertThat(materializedViewsCache.get(defaultMV1.getDbName(), defaultMV1.getTableName()), is(defaultRelOptMaterialization1));
    assertThat(materializedViewsCache.get(db1MV1.getDbName(), db1MV1.getTableName()), is(db1RelOptMaterialization1));
    assertThat(materializedViewsCache.get(defaultMV1.getViewExpandedText()).size(), is(2));
    assertThat(materializedViewsCache.get(defaultMV1.getViewExpandedText()), hasItem(defaultRelOptMaterialization1));
    assertThat(materializedViewsCache.get(defaultMV1.getViewExpandedText()), hasItem(db1RelOptMaterialization1));
    assertThat(materializedViewsCache.values().size(), is(2));
  }

  @Test
  void testRefreshWhenMVWasNotCached() {
    materializedViewsCache.refresh(defaultMV1, defaultMV1, defaultRelOptMaterialization1);

    assertThat(materializedViewsCache.get(defaultMV1.getDbName(), defaultMV1.getTableName()), is(defaultRelOptMaterialization1));
    assertThat(materializedViewsCache.get(defaultMV1.getViewExpandedText()).size(), is(1));
    assertThat(materializedViewsCache.get(defaultMV1.getViewExpandedText()), hasItem(defaultRelOptMaterialization1));
    assertThat(materializedViewsCache.values().size(), is(1));
    assertThat(materializedViewsCache.values(), hasItem(defaultRelOptMaterialization1));
  }

  @Test
  void testRefreshWhenMVIsCachedButWasUpdated() {
    materializedViewsCache.putIfAbsent(defaultMV1, defaultRelOptMaterialization1);
    RelOptMaterialization newMaterialization = createRelOptMaterialization(defaultMV1);
    materializedViewsCache.refresh(defaultMV1, defaultMV1, newMaterialization);

    assertThat(newMaterialization, is(not(defaultRelOptMaterialization1)));
    assertThat(materializedViewsCache.get(defaultMV1.getDbName(), defaultMV1.getTableName()), is(newMaterialization));
    assertThat(materializedViewsCache.get(defaultMV1.getViewExpandedText()).size(), is(1));
    assertThat(materializedViewsCache.get(defaultMV1.getViewExpandedText()), hasItem(newMaterialization));
    assertThat(materializedViewsCache.values().size(), is(1));
    assertThat(materializedViewsCache.values(), hasItem(newMaterialization));
  }

  @Test
  void testRefreshWhenMVRefersToANewMaterialization() {
    materializedViewsCache.putIfAbsent(defaultMV1, defaultRelOptMaterialization1);
    materializedViewsCache.refresh(defaultMV2, defaultMV1, defaultRelOptMaterialization1);

    assertThat(materializedViewsCache.get(defaultMV1.getDbName(), defaultMV1.getTableName()), is(defaultRelOptMaterialization1));
    assertThat(materializedViewsCache.get(defaultMV1.getViewExpandedText()).size(), is(1));
    assertThat(materializedViewsCache.get(defaultMV1.getViewExpandedText()), hasItem(defaultRelOptMaterialization1));
    assertThat(materializedViewsCache.values().size(), is(1));
    assertThat(materializedViewsCache.values(), hasItem(defaultRelOptMaterialization1));
  }

  @Test
  void testRemoveByTable() {
    materializedViewsCache.putIfAbsent(defaultMV1, defaultRelOptMaterialization1);

    materializedViewsCache.remove(defaultMV1);

    assertThat(materializedViewsCache.get(defaultMV1.getDbName(), defaultMV1.getTableName()), is(nullValue()));
    assertThat(materializedViewsCache.get(defaultMV1.getViewExpandedText()).isEmpty(), is(true));
    assertThat(materializedViewsCache.values().isEmpty(), is(true));
  }

  @Test
  void testRemoveByTableName() {
    materializedViewsCache.putIfAbsent(defaultMV1, defaultRelOptMaterialization1);

    materializedViewsCache.remove(defaultMV1.getDbName(), defaultMV1.getTableName());

    assertThat(materializedViewsCache.get(defaultMV1.getDbName(), defaultMV1.getTableName()), is(nullValue()));
    assertThat(materializedViewsCache.get(defaultMV1.getViewExpandedText()).isEmpty(), is(true));
    assertThat(materializedViewsCache.values().isEmpty(), is(true));
  }

  @Disabled("Testing parallelism only")
  @Test
  void testParallelism() {
    int ITERATIONS = 1000000;

    List<Pair<Table, RelOptMaterialization>> testData = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      Table table = new Table(new org.apache.hadoop.hive.metastore.api.Table());
      table.setDbName("default");
      table.setTableName("mat" + i);
      table.setViewOriginalText("select col0 from t" + i);
      RelOptMaterialization relOptMaterialization = createRelOptMaterialization(table);
      testData.add(new Pair<>(table, relOptMaterialization));
    }
    for (int i = 0; i < 10; ++i) {
      Table table = new Table(new org.apache.hadoop.hive.metastore.api.Table());
      table.setDbName("db1");
      table.setTableName("mat" + i);
      table.setViewOriginalText("select col0 from t" + i);
      RelOptMaterialization relOptMaterialization = createRelOptMaterialization(table);
      testData.add(new Pair<>(table, relOptMaterialization));
    }

    List<Callable<Void>> callableList = new ArrayList<>();
    callableList.add(() -> {
      for (Pair<Table, RelOptMaterialization> entry : testData) {
        materializedViewsCache.refresh(entry.left, entry.left, entry.right);
      }
      return null;
    });
    callableList.add(() -> {
      for (int j = 0; j < ITERATIONS; ++j) {
        for (Pair<Table, RelOptMaterialization> entry : testData) {
          materializedViewsCache.remove(entry.left);
          materializedViewsCache.putIfAbsent(entry.left, entry.right);
        }
      }
      return null;
    });
    for (Pair<Table, RelOptMaterialization> entry : testData) {
      callableList.add(() -> {
        for (int j = 0; j < ITERATIONS; ++j) {
          materializedViewsCache.get(entry.left.getViewExpandedText());
        }
        return null;
      });
    }
    callableList.add(() -> {
      for (int j = 0; j < ITERATIONS; ++j) {
        List<RelOptMaterialization> materializations = materializedViewsCache.values();
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
    public List<RexNode> getChildExps() {
      return null;
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
    public boolean isDistinct() {
      return false;
    }

    @Override
    public RelNode getInput(int i) {
      return null;
    }

    @Override
    public RelOptQuery getQuery() {
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
    public double getRows() {
      return 0;
    }

    @Override
    public Set<String> getVariablesStopped() {
      return null;
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
    public RelOptCost computeSelfCost(RelOptPlanner relOptPlanner) {
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
    public String recomputeDigest() {
      return null;
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
    public boolean isValid(boolean b) {
      return false;
    }

    @Override
    public List<RelCollation> getCollationList() {
      return null;
    }

    @Override
    public RelNode copy(RelTraitSet relTraitSet, List<RelNode> list) {
      return null;
    }

    @Override
    public void register(RelOptPlanner relOptPlanner) {

    }

    @Override
    public boolean isKey(ImmutableBitSet immutableBitSet) {
      return false;
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