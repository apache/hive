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
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a helper to check whether a materialized view rebuild
 * can be transformed from INSERT OVERWRITE to INSERT INTO.
 *
 * We are verifying that:
 *   1) Plan only uses legal operators (i.e., Filter, Project,
 *   Join, and TableScan)
 *   2) Whether the plane has aggregate
 *   3) Whether the plane has an count(*) aggregate function call
 */
public class MaterializedViewIncrementalRewritingRelVisitor extends RelVisitor {

  private boolean containsAggregate;
  private boolean rewritingAllowed;
  private boolean hasCountStar;

  public MaterializedViewIncrementalRewritingRelVisitor() {
    this.containsAggregate = false;
    this.rewritingAllowed = true;
    this.hasCountStar = false;
  }

  @Override
  public void visit(RelNode node, int ordinal, RelNode parent) {
    if (node instanceof Aggregate) {
      this.containsAggregate = true;
      check((Aggregate) node);
      super.visit(node, ordinal, parent);
    } else if (node instanceof TableScan ||
            node instanceof Filter ||
            node instanceof Project ||
            node instanceof Join) {
      super.visit(node, ordinal, parent);
    } else {
      rewritingAllowed = false;
    }
  }

  private void check(Aggregate aggregate) {
    for (int i = 0; i < aggregate.getAggCallList().size(); ++i) {
      AggregateCall aggregateCall = aggregate.getAggCallList().get(i);
      if (aggregateCall.getAggregation().getKind() == SqlKind.COUNT && aggregateCall.getArgList().size() == 0) {
        hasCountStar = true;
        break;
      }
    }
  }

  /**
   * Starts an iteration.
   */
  public RelNode go(RelNode p) {
    visit(p, 0, null);
    return p;
  }

  public boolean isContainsAggregate() {
    return containsAggregate;
  }

  public boolean isRewritingAllowed() {
    return rewritingAllowed;
  }

  public boolean hasCountStar() {
    return hasCountStar;
  }
}
