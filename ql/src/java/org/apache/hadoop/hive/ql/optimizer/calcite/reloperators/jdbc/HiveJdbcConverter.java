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
package org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.jdbc;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcProject;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;

import org.apache.calcite.util.ControlFlowException;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRelNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.jdbc.HiveJdbcImplementor;

/**
 * This is a designated RelNode that splits the Hive operators and the Jdbc operators,
 * every successor of this node will be Jdbc operator.
 */
public class HiveJdbcConverter extends ConverterImpl implements HiveRelNode {

  private final JdbcConvention convention;
  private final String url;
  private final String user;

  public HiveJdbcConverter(RelOptCluster cluster,  RelTraitSet traits,
      RelNode input, JdbcConvention jc, String url, String user) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
    this.convention = jc;
    this.url = url;
    this.user = user;
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("convention", convention);
  }

  public JdbcConvention getJdbcConvention() {
    return convention;
  }

  public SqlDialect getJdbcDialect() {
    return convention.dialect;
  }

  public String getConnectionUrl() {
    return url;
  }

  public String getConnectionUser() {
    return user;
  }

  @Override
  public RelNode copy(
      RelTraitSet traitSet,
      List<RelNode> inputs) {
    return new HiveJdbcConverter(getCluster(), traitSet, sole(inputs), convention, url, user);
  }

  public RelNode copy(RelTraitSet traitSet, RelNode input) {
    return new HiveJdbcConverter(getCluster(), traitSet, input, convention, url, user);
  }

  public String generateSql() {
    SqlDialect dialect = getJdbcDialect();
    final HiveJdbcImplementor jdbcImplementor =
        new HiveJdbcImplementor(dialect,
            (JavaTypeFactory) getCluster().getTypeFactory());
    Project topProject;
    if (getInput() instanceof Project) {
      topProject = (Project) getInput();
    } else {
      // If it is not a project operator, we add it on top of the input
      // to force generating the column names instead of * while
      // translating to SQL
      RelNode nodeToTranslate = getInput();
      RexBuilder builder = getCluster().getRexBuilder();
      List<RexNode> projects = new ArrayList<>(
          nodeToTranslate.getRowType().getFieldList().size());
      for (int i = 0; i < nodeToTranslate.getRowType().getFieldCount(); i++) {
        projects.add(builder.makeInputRef(nodeToTranslate, i));
      }
      topProject = new JdbcProject(nodeToTranslate.getCluster(),
          nodeToTranslate.getTraitSet(), nodeToTranslate,
          projects, nodeToTranslate.getRowType());
    }
    final HiveJdbcImplementor.Result result =
        jdbcImplementor.visitRoot(topProject);
    return result.asStatement().toSqlString(dialect).getSql();
  }

  /**
   * Whether the execution of the query below this jdbc converter
   * can be split by Hive.
   */
  public boolean splittingAllowed() {
    JdbcRelVisitor visitor = new JdbcRelVisitor();
    visitor.go(getInput());
    return visitor.splittingAllowed;
  }

  public JdbcHiveTableScan getTableScan() {
    final JdbcHiveTableScan[] tmpJdbcHiveTableScan = new JdbcHiveTableScan[1];
    new RelVisitor() {

      public void visit(
          RelNode node,
          int ordinal,
          RelNode parent) {
        if (node instanceof JdbcHiveTableScan && tmpJdbcHiveTableScan [0] == null) {
          tmpJdbcHiveTableScan [0] = (JdbcHiveTableScan) node;
        } else {
          super.visit(node, ordinal, parent);
        }
      }
    }.go(this);

    JdbcHiveTableScan jdbcHiveTableScan = tmpJdbcHiveTableScan [0];

    assert jdbcHiveTableScan != null;
    return jdbcHiveTableScan;
  }

  private static class JdbcRelVisitor extends RelVisitor {

    private boolean splittingAllowed;

    public JdbcRelVisitor() {
      this.splittingAllowed = true;
    }

    @Override
    public void visit(RelNode node, int ordinal, RelNode parent) {
      if (node instanceof Project ||
          node instanceof Filter ||
          node instanceof TableScan) {
        // We can continue
        super.visit(node, ordinal, parent);
      } else {
        throw new ReturnedValue(false);
      }
    }

    /**
     * Starts an iteration.
     */
    public RelNode go(RelNode p) {
      try {
        visit(p, 0, null);
      } catch (ReturnedValue e) {
        // Splitting cannot be performed
        splittingAllowed = e.value;
      }
      return p;
    }

    /**
     * Exception used to interrupt a visitor walk.
     */
    private static class ReturnedValue extends ControlFlowException {
      private final boolean value;

      public ReturnedValue(boolean value) {
        this.value = value;
      }
    }

  }
}
