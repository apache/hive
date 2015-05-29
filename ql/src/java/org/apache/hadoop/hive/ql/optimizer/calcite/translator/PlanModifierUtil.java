/**
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
package org.apache.hadoop.hive.ql.optimizer.calcite.translator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSort;

import com.google.common.collect.ImmutableMap;

public class PlanModifierUtil {

  private static final Log LOG = LogFactory.getLog(PlanModifierUtil.class);


  protected static void fixTopOBSchema(final RelNode rootRel,
      Pair<RelNode, RelNode> topSelparentPair, List<FieldSchema> resultSchema,
      boolean replaceProject) throws CalciteSemanticException {
    if (!(topSelparentPair.getKey() instanceof Sort)
        || !HiveCalciteUtil.orderRelNode(topSelparentPair.getKey())) {
      return;
    }
    HiveSort obRel = (HiveSort) topSelparentPair.getKey();
    Project obChild = (Project) topSelparentPair.getValue();
    if (obChild.getRowType().getFieldCount() <= resultSchema.size()) {
      return;
    }

    RelDataType rt = obChild.getRowType();
    @SuppressWarnings({ "unchecked", "rawtypes" })
    Set<Integer> collationInputRefs = new HashSet(
        RelCollations.ordinals(obRel.getCollation()));
    ImmutableMap.Builder<Integer, RexNode> inputRefToCallMapBldr = ImmutableMap.builder();
    for (int i = resultSchema.size(); i < rt.getFieldCount(); i++) {
      if (collationInputRefs.contains(i)) {
        RexNode obyExpr = obChild.getChildExps().get(i);
        if (obyExpr instanceof RexCall) {
          LOG.debug("Old RexCall : " + obyExpr);
          obyExpr = adjustOBSchema((RexCall) obyExpr, obChild, resultSchema);
          LOG.debug("New RexCall : " + obyExpr);
        }
        inputRefToCallMapBldr.put(i, obyExpr);
      }
    }
    ImmutableMap<Integer, RexNode> inputRefToCallMap = inputRefToCallMapBldr.build();

    if ((obChild.getRowType().getFieldCount() - inputRefToCallMap.size()) != resultSchema.size()) {
      LOG.error(generateInvalidSchemaMessage(obChild, resultSchema, inputRefToCallMap.size()));
      throw new CalciteSemanticException("Result Schema didn't match Optimized Op Tree Schema");
    }

    if (replaceProject) {
      // This removes order-by only expressions from the projections.
      HiveProject replacementProjectRel = HiveProject.create(obChild.getInput(), obChild
          .getChildExps().subList(0, resultSchema.size()), obChild.getRowType().getFieldNames()
          .subList(0, resultSchema.size()));
      obRel.replaceInput(0, replacementProjectRel);
    }
    obRel.setInputRefToCallMap(inputRefToCallMap);
  }

  private static RexCall adjustOBSchema(RexCall obyExpr, Project obChild,
          List<FieldSchema> resultSchema) {
    int a = -1;
    List<RexNode> operands = new ArrayList<>();
    for (int k = 0; k < obyExpr.operands.size(); k++) {
      RexNode rn = obyExpr.operands.get(k);
      for (int j = 0; j < resultSchema.size(); j++) {
        if( obChild.getChildExps().get(j).toString().equals(rn.toString())) {
          a = j;
          break;
        }
      }
      if (a != -1) {
        operands.add(new RexInputRef(a, rn.getType()));
      } else {
        if (rn instanceof RexCall) {
          operands.add(adjustOBSchema((RexCall)rn, obChild, resultSchema));
        } else {
          operands.add(rn);
        }
      }
      a = -1;
    }
    return (RexCall) obChild.getCluster().getRexBuilder().makeCall(
            obyExpr.getType(), obyExpr.getOperator(), operands);
  }

  protected static String generateInvalidSchemaMessage(Project topLevelProj,
      List<FieldSchema> resultSchema, int fieldsForOB) {
    String errorDesc = "Result Schema didn't match Calcite Optimized Op Tree; schema: ";
    for (FieldSchema fs : resultSchema) {
      errorDesc += "[" + fs.getName() + ":" + fs.getType() + "], ";
    }
    errorDesc += " projection fields: ";
    for (RexNode exp : topLevelProj.getChildExps()) {
      errorDesc += "[" + exp.toString() + ":" + exp.getType() + "], ";
    }
    if (fieldsForOB != 0) {
      errorDesc += fieldsForOB + " fields removed due to ORDER BY  ";
    }
    return errorDesc.substring(0, errorDesc.length() - 2);
  }

}
