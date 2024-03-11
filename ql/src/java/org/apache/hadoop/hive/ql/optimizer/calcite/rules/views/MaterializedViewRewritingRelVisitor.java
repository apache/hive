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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.util.ControlFlowException;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;

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

  private boolean containsAggregate;
  private final boolean fullAcidView;
  private IncrementalRebuildMode incrementalRebuildMode;
  private int countIndex;

  public MaterializedViewRewritingRelVisitor(boolean fullAcidView) {
    this.containsAggregate = false;
    this.fullAcidView = fullAcidView;
    this.incrementalRebuildMode = IncrementalRebuildMode.NOT_AVAILABLE;
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
    throw new ReturnedValue(IncrementalRebuildMode.NOT_AVAILABLE);
  }

  private void check(Union union) {
    // We found the Union
    if (union.getInputs().size() != 2) {
      // Bail out
      throw new ReturnedValue(IncrementalRebuildMode.NOT_AVAILABLE);
    }
    // First branch should have the query (with write ID filter conditions)
    RelNode queryBranch = union.getInput(0);
    MaterializedViewIncrementalRewritingRelVisitor.Result result =
        new MaterializedViewIncrementalRewritingRelVisitor().go(queryBranch);
    incrementalRebuildMode = result.getIncrementalRebuildMode();
    containsAggregate = result.containsAggregate();
    countIndex = result.getCountStarIndex();

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
            throw new ReturnedValue(IncrementalRebuildMode.NOT_AVAILABLE);
          }
          if (containsAggregate && !fullAcidView) {
            // If it contains an aggregate and it is not a full acid table,
            // we do not rewrite it (we need MERGE support)
            throw new ReturnedValue(IncrementalRebuildMode.NOT_AVAILABLE);
          }
        } else if (node instanceof Project) {
          // We can continue
          super.visit(node, ordinal, parent);
        } else {
          throw new ReturnedValue(IncrementalRebuildMode.NOT_AVAILABLE);
        }
      }
    }.go(union.getInput(1));
    // We pass all the checks, we can rewrite
    throw new ReturnedValue(result.getIncrementalRebuildMode());
  }

  /**
   * Starts an iteration.
   */
  public RelNode go(RelNode p) {
    try {
      visit(p, 0, null);
    } catch (ReturnedValue e) {
      // Rewriting cannot be performed
      incrementalRebuildMode = e.incrementalRebuildMode;
    }
    return p;
  }

  public boolean isContainsAggregate() {
    return containsAggregate;
  }

  public IncrementalRebuildMode getIncrementalRebuildMode() {
    return incrementalRebuildMode;
  }

  public int getCountIndex() {
    return countIndex;
  }

  /**
   * Exception used to interrupt a visitor walk.
   */
  private static final class ReturnedValue extends ControlFlowException {
    private final IncrementalRebuildMode incrementalRebuildMode;

    public ReturnedValue(IncrementalRebuildMode incrementalRebuildMode) {
      this.incrementalRebuildMode = incrementalRebuildMode;
    }
  }
}
