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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.optimizer.ConstantPropagateProcFactory;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.ExprNodeConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.RexNodeConverter;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class HiveRexExecutorImpl extends RexExecutorImpl {

  private static final Logger LOG = LoggerFactory.getLogger(HiveRexExecutorImpl.class);

  private final RelOptCluster cluster;

  public HiveRexExecutorImpl(RelOptCluster cluster) {
    super(null);
    this.cluster = cluster;
  }

  @Override
  public void reduce(RexBuilder rexBuilder, List<RexNode> constExps, List<RexNode> reducedValues) {
    RexNodeConverter rexNodeConverter = new RexNodeConverter(cluster);
    for (RexNode rexNode : constExps) {
      // initialize the converter
      ExprNodeConverter converter = new ExprNodeConverter("", null, null, null,
          new HashSet<Integer>(), cluster.getTypeFactory());
      // convert RexNode to ExprNodeGenericFuncDesc
      ExprNodeDesc expr = rexNode.accept(converter);
      if (expr instanceof ExprNodeGenericFuncDesc) {
        // folding the constant
        ExprNodeDesc constant = ConstantPropagateProcFactory
            .foldExpr((ExprNodeGenericFuncDesc) expr);
        if (constant != null) {
          try {
            // convert constant back to RexNode
            reducedValues.add(rexNodeConverter.convert(constant));
          } catch (Exception e) {
            LOG.warn(e.getMessage());
            reducedValues.add(rexNode);
          }
        } else {
          reducedValues.add(rexNode);
        }
      } else {
        reducedValues.add(rexNode);
      }
    }
  }

}
