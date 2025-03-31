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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveIn;

import java.util.List;

public class InSearchShuttle extends RexShuttle {
  private final RexBuilder rexBuilder;

  public InSearchShuttle(RexBuilder rexBuilder) {
    this.rexBuilder = rexBuilder;
  }

  @Override
  public RexNode visitCall(final RexCall call) {
    if (HiveIn.INSTANCE.equals(call.op)) {
      List<RexNode> clonedOps = visitList(call.operands, (boolean[]) null);
      return rexBuilder.makeIn(clonedOps.get(0), clonedOps.subList(1, clonedOps.size()));
    } else {
      return super.visitCall(call);
    }
  }
}
