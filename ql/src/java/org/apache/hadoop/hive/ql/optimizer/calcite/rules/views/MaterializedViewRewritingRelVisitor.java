/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite.rules.views;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ControlFlowException;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class is a helper to check whether a materialized view rebuild
 * can be transformed from INSERT OVERWRITE to INSERT INTO.
 *
 * We are verifying that:
 *   1) the rewriting is rooted by legal operators (Filter and Project)
 *   before reaching a Union operator,
 *   2) the left branch uses the MV that we are trying to rebuild and
 *   legal operators (Filter and Project), and
 *   3) the right branch only uses legal operators (i.e., Filter, Project,
 *   Join, and TableScan)
 */
public class MaterializedViewRewritingRelVisitor extends RelVisitor {

  private static final Logger LOG = LoggerFactory.getLogger(MaterializedViewRewritingRelVisitor.class);


  private boolean containsAggregate;
  private boolean fullAcidView;
  private boolean rewritingAllowed;
  private int countIndex;

  public MaterializedViewRewritingRelVisitor(boolean fullAcidView) {
    this.containsAggregate = false;
    this.fullAcidView = fullAcidView;
    this.rewritingAllowed = false;
    this.countIndex = -1;
  }

  @Override
  public void visit(RelNode node, int ordinal, RelNode parent) {
    if (node instanceof Aggregate) {
      this.containsAggregate = true;
      // Aggregate mode - it should be followed by union
      // that we need to analyze
      RelNode input = node.getInput(0);
      if (input instanceof Union) {
        check((Union) input);
      }
    } else if (node instanceof Union) {
      // Non aggregate mode - analyze union operator
      check((Union) node);
    } else if (node instanceof Project) {
      // Project operator, we can continue
      super.visit(node, ordinal, parent);
    }
    throw new ReturnedValue(false);
  }

  private void check(Union union) {
    // We found the Union
    if (union.getInputs().size() != 2) {
      // Bail out
      throw new ReturnedValue(false);
    }
    // First branch should have the query (with write ID filter conditions)
    new RelVisitor() {
      @Override
      public void visit(RelNode node, int ordinal, RelNode parent) {
        if (node instanceof TableScan ||
            node instanceof Filter ||
            node instanceof Project ||
            node instanceof Join) {
          // We can continue
          super.visit(node, ordinal, parent);
        } else if (node instanceof Aggregate && containsAggregate) {
          Aggregate aggregate = (Aggregate) node;
          for (int i = 0; i < aggregate.getAggCallList().size(); ++i) {
            AggregateCall aggregateCall = aggregate.getAggCallList().get(i);
            if (aggregateCall.getAggregation().getKind() == SqlKind.COUNT && aggregateCall.getArgList().size() == 0) {
              countIndex = i + aggregate.getGroupCount();
              break;
            }
          }
          // We can continue
          super.visit(node, ordinal, parent);
        } else {
          throw new ReturnedValue(false);
        }
      }
    }.go(union.getInput(0));
    // Second branch should only have the MV
    new RelVisitor() {
      @Override
      public void visit(RelNode node, int ordinal, RelNode parent) {
        if (node instanceof TableScan) {
          // We can continue
          // TODO: Need to check that this is the same MV that we are rebuilding
          RelOptHiveTable hiveTable = (RelOptHiveTable) node.getTable();
          if (!hiveTable.getHiveTableMD().isMaterializedView()) {
            // If it is not a materialized view, we do not rewrite it
            throw new ReturnedValue(false);
          }
          if (containsAggregate && !fullAcidView) {
            // If it contains an aggregate and it is not a full acid table,
            // we do not rewrite it (we need MERGE support)
            throw new ReturnedValue(false);
          }
        } else if (node instanceof Project) {
          // We can continue
          super.visit(node, ordinal, parent);
        } else {
          throw new ReturnedValue(false);
        }
      }
    }.go(union.getInput(1));
    // We pass all the checks, we can rewrite
    throw new ReturnedValue(true);
  }

  /**
   * Starts an iteration.
   */
  public RelNode go(RelNode p) {
    try {
      visit(p, 0, null);
    } catch (ReturnedValue e) {
      // Rewriting cannot be performed
      rewritingAllowed = e.value;
    }
    return p;
  }

  public boolean isContainsAggregate() {
    return containsAggregate;
  }

  public boolean isRewritingAllowed() {
    return rewritingAllowed;
  }

  public int getCountIndex() {
    return countIndex;
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
