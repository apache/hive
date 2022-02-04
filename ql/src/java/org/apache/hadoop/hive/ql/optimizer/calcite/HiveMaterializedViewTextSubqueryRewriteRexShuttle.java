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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.fun.SqlQuantifyOperator;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.HiveMaterializedViewsRegistry;
import org.apache.hadoop.hive.ql.metadata.HiveRelOptMaterialization;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveMaterializedViewUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;

import java.util.Map;
import java.util.Stack;

public class HiveMaterializedViewTextSubqueryRewriteRexShuttle extends RexShuttle {

  private final Map<RelNode, ASTNode> map;
  private final ASTNode ast;
  private final ASTNode expandedAST;
  private final RelBuilder relBuilder;

  public HiveMaterializedViewTextSubqueryRewriteRexShuttle(Map<RelNode, ASTNode> map, ASTNode ast, ASTNode expandedAST, RelBuilder relBuilder) {
    this.map = map;
    this.ast = ast;
    this.expandedAST = expandedAST;
    this.relBuilder = relBuilder;
  }

  @Override
  public RexNode visitSubQuery(RexSubQuery subQuery) {

     RelNode newSubquery = subQuery.rel.accept(new HiveMaterializedViewTextSubqueryRewriteShuttle(map, ast, expandedAST, relBuilder));

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
