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
package org.apache.hadoop.hive.ql.optimizer.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.fun.SqlQuantifyOperator;

/**
 * Traverse expressions and tries to rewrite subQuery expressions to Materialized view scans.
 */
public class HiveMaterializedViewASTSubQueryRewriteRexShuttle extends RexShuttle {

  private final HiveMaterializedViewASTSubQueryRewriteShuttle relShuttle;

  public HiveMaterializedViewASTSubQueryRewriteRexShuttle(HiveMaterializedViewASTSubQueryRewriteShuttle relShuttle) {
    this.relShuttle = relShuttle;
  }

  @Override
  public RexNode visitSubQuery(RexSubQuery subQuery) {

    RelNode newSubquery = subQuery.rel.accept(relShuttle);

    RexNode newSubQueryRex;
    switch (subQuery.op.kind) {
      case IN:
        newSubQueryRex = RexSubQuery.in(newSubquery, subQuery.operands);
        break;

      case EXISTS:
        newSubQueryRex = RexSubQuery.exists(newSubquery);
        break;

      case SCALAR_QUERY:
        newSubQueryRex = RexSubQuery.scalar(newSubquery);
        break;

      case SOME:
      case ALL:
        newSubQueryRex = RexSubQuery.some(newSubquery, subQuery.operands, (SqlQuantifyOperator) subQuery.op);
        break;

      default:
        throw new RuntimeException("Unsupported op.kind " + subQuery.op.kind);
    }

    return newSubQueryRex;
  }
}
