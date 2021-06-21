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

import java.util.HashSet;
import java.util.List;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.optimizer.ConstantPropagateProcFactory;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.ExprNodeConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.RexNodeConverter;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Executor for {@link RexNode} based on Hive semantics.
 */
public class HiveRexExecutorImpl extends RexExecutorImpl {

  private static final Logger LOG = LoggerFactory.getLogger(HiveRexExecutorImpl.class);


  public HiveRexExecutorImpl() {
    super(null);
  }

  @Override
  public void reduce(RexBuilder rexBuilder, List<RexNode> constExps, List<RexNode> reducedValues) {
    RexNodeConverter rexNodeConverter = new RexNodeConverter(rexBuilder, rexBuilder.getTypeFactory());
    for (RexNode rexNode : constExps) {
      // initialize the converter
      ExprNodeConverter converter = new ExprNodeConverter("", null, null, null,
          new HashSet<>(), rexBuilder);
      // convert RexNode to ExprNodeGenericFuncDesc
      ExprNodeDesc expr = rexNode.accept(converter);
      if (expr instanceof ExprNodeGenericFuncDesc) {
        // folding the constant
        ExprNodeDesc constant = ConstantPropagateProcFactory
            .foldExpr((ExprNodeGenericFuncDesc) expr);
        if (constant != null) {
          addExpressionToList(constant, rexNode, rexNodeConverter, reducedValues);
        } else {
          reducedValues.add(rexNode);
        }
      } else if (expr instanceof ExprNodeConstantDesc) {
        addExpressionToList(expr, rexNode, rexNodeConverter, reducedValues);
      } else {
        reducedValues.add(rexNode);
      }
    }
  }

  private void addExpressionToList(ExprNodeDesc reducedExpr, RexNode originalExpr,
      RexNodeConverter rexNodeConverter, List<RexNode> reducedValues) {
    try {
      // convert constant back to RexNode
      reducedValues.add(rexNodeConverter.convert(reducedExpr));
    } catch (Exception e) {
      LOG.warn(e.getMessage());
      reducedValues.add(originalExpr);
    }
  }

}
