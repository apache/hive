package org.apache.hadoop.hive.ql.optimizer.calcite;/*
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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexVisitorImpl;

public class HiveSubQueryVisitor extends RelVisitor {

  @Override
  public void visit(RelNode node, int ordinal, RelNode parent) {
    if (node instanceof Filter) {
      visit((Filter) node);
    } else if (node instanceof Project) {
      visit((Project) node);
    }

    super.visit(node, ordinal, parent);
  }

  protected void visit(Filter filter) {
    filter.getCondition().accept(new SubQueryRexVisitor(true, this));
  }

  protected void visit(Project project) {
    SubQueryRexVisitor subQueryRexVisitor = new SubQueryRexVisitor(true, this);
    for (RexNode expression : project.getProjects()) {
      expression.accept(subQueryRexVisitor);
    }
  }

  private static class SubQueryRexVisitor extends RexVisitorImpl<Void> {

    private final HiveSubQueryVisitor subQueryVisitor;

    public SubQueryRexVisitor(boolean deep, HiveSubQueryVisitor subQueryVisitor) {
      super(deep);
      this.subQueryVisitor = subQueryVisitor;
    }

    @Override
    public Void visitSubQuery(RexSubQuery subQuery) {
      subQuery.rel.childrenAccept(subQueryVisitor);
      return super.visitSubQuery(subQuery);
    }
  }
}
