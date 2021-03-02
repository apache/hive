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
package org.apache.hadoop.hive.ql.optimizer.calcite.reloperators;

import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelShuttle;
import org.apache.hadoop.hive.ql.optimizer.calcite.TraitsUtil;
import org.apache.calcite.rel.core.CorrelationId;
import java.util.Set;
import java.util.HashSet;
 
public class HiveFilter extends Filter implements HiveRelNode {

  public HiveFilter(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
    super(cluster, TraitsUtil.getDefaultTraitSet(cluster), child, condition);
  }

  @Override
  public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    assert traitSet.containsIfApplicable(HiveRelNode.CONVENTION);
    return new HiveFilter(getCluster(), traitSet, input, condition);
  }

  @Override
  public void implement(Implementor implementor) {
  }

  private static void findCorrelatedVar(RexNode node, Set<CorrelationId> allVars) {
    if(node instanceof RexCall) {
      RexCall nd = (RexCall)node;
      for (RexNode rn : nd.getOperands()) {
        if (rn instanceof RexFieldAccess) {
          final RexNode ref = ((RexFieldAccess) rn).getReferenceExpr();
          if (ref instanceof RexCorrelVariable) {
              allVars.add(((RexCorrelVariable) ref).id);
          }
        } else {
          findCorrelatedVar(rn, allVars);
        }
      }
    }
  }

  //traverse the given node to find all correlated variables
  // Note that correlated variables are supported in Filter only i.e. Where & Having
  private static void traverseFilter(RexNode node, Set<CorrelationId> allVars) {
    Preconditions.checkNotNull(allVars);
    if (node instanceof RexSubQuery) {
      RelNode input = ((RexSubQuery)node).rel.getInput(0);
      Preconditions.checkNotNull(input);
      // initialize number of correlated variables input here to check if it changes.
      int correlatedVars = allVars.size();
      // Loop until we find the first filter that contains a correlated variable.
      // CDPD-24188: Is it possible to have both a having and where clause with
      // a correlated variable?
      while (input != null && input.getInputs().size() == 1 &&
          correlatedVars == allVars.size()) {
        if (input instanceof HiveFilter) {
          findCorrelatedVar(((HiveFilter)input).getCondition(), allVars);
        }
        input = input.getInput(0);
      }
    } else if (node instanceof RexCall) {
      //AND, NOT etc
      int numOperands = ((RexCall)node).getOperands().size();
      for(int i=0; i<numOperands; i++) {
          RexNode op = ((RexCall)node).getOperands().get(i);
          traverseFilter(op, allVars);
      }
    }
  }

  @Override
  public Set<CorrelationId> getVariablesSet() {
      Set<CorrelationId> allCorrVars = new HashSet<>();
      traverseFilter(condition, allCorrVars);
      return allCorrVars;
  }

  public static Set<CorrelationId> getVariablesSet(RexSubQuery e) {
      Set<CorrelationId> allCorrVars = new HashSet<>();
      traverseFilter(e, allCorrVars);
      return allCorrVars;
  }

  public RelNode accept(RelShuttle shuttle) {
    if (shuttle instanceof HiveRelShuttle) {
      return ((HiveRelShuttle)shuttle).visit(this);
    }
    return shuttle.visit(this);
  }

}
