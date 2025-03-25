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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.RangeSets;
import org.apache.calcite.util.Sarg;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public abstract class SearchTransformer<R> {
  private final RexBuilder rexBuilder;
  private final RexVisitor<R> rexVisitor;
  private final RexNode ref;
  private final Sarg<?> sarg;
  protected final RelDataType type;
  protected List<R> results;
  protected final boolean negate;
  protected R nullAsNode;
  protected boolean nullAsTrue;

  protected SearchTransformer(RexBuilder rexBuilder, RexCall search, RexVisitor<R> rexVisitor, boolean negate) {
    this.rexBuilder = rexBuilder;
    this.rexVisitor = rexVisitor;
    ref = search.getOperands().get(0);
    this.negate = negate;
    RexLiteral literal = (RexLiteral) search.operands.get(1);
    sarg = Objects.requireNonNull(literal.getValueAs(Sarg.class), "Sarg");
    type = literal.getType();
    nullAsNode = null;
    nullAsTrue = true;
  }

  protected SearchTransformer(RexBuilder rexBuilder, RexCall search, RexVisitor<R> rexVisitor) {
    this(rexBuilder, search, rexVisitor, false);
  }

  public R transform() {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.SEARCH_TRANSFORMER);

    try {
      RangeConverter consumer = new RangeConverter<>(rexBuilder, type, ref, rexVisitor, negate);
      RangeSets.forEach(sarg.rangeSet, consumer);
      computeNullAsNode();

      results = new ArrayList<>();
      if (!consumer.inNodes.isEmpty()) {
        results.add(transformInOperands((List<R>) consumer.inNodes));
      }
      results.addAll(consumer.nodes);

      if (results.size() == 1) {
        return transformWithNullAs(results.get(0));
      }
      return transformWithNullAs(transformAllNodes());
    } finally {
      perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.SEARCH_TRANSFORMER);
    }
  }

  private void computeNullAsNode() {
    if (sarg.nullAs == RexUnknownAs.UNKNOWN) {
      return;
    }

    RexCall call = null;
    if (sarg.nullAs == RexUnknownAs.TRUE) {
      call = negate ?
          (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, ref):
          (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, ref);
      nullAsTrue = !negate;
    }
    if (sarg.nullAs == RexUnknownAs.FALSE) {
      call = negate ?
          (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, ref):
          (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, ref);
      nullAsTrue = negate;
    }

    assert call != null;
    nullAsNode = call.accept(rexVisitor);
  }

  protected abstract R transformInOperands(List<R> inNodes);

  protected abstract R transformAllNodes();

  protected abstract R transformWithNullAs(R node);
}

