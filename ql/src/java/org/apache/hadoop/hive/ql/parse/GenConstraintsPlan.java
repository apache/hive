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

package org.apache.hadoop.hive.ql.parse;

import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.type.ExprNodeTypeCheck;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCardinalityViolation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenConstraintsPlan {
  protected static final Logger LOG = LoggerFactory.getLogger(GenConstraintsPlan.class.getName());
 
  private boolean operatorCreated = false;
  private Operator<? extends OperatorDesc> constraintsOperator;
  private RowResolver inputRR;

  public GenConstraintsPlan(String dest, QB qb, Operator<? extends OperatorDesc> input,
      Map<Operator<? extends OperatorDesc>, OpParseContext> operatorMap,
      ReadOnlySemanticAnalyzer sa
      ) throws SemanticException {
    constraintsOperator = input;
    if (SemanticAnalyzer.deleting(dest)) {
      // for DELETE statements NOT NULL constraint need not be checked
      return;
    }

    if (SemanticAnalyzer.updating(dest) && sa.isCBOExecuted() &&
        sa.getContext().getOperation() != Context.Operation.MERGE) {
      // for UPDATE statements CBO already added and pushed down the constraints
      return;
    }

    //MERGE statements could have inserted a cardinality violation branch, we need to avoid that
    if (mergeCardinalityViolationBranch(input)){
      return;
    }

    // if this is an insert into statement we might need to add constraint check
    assert (input.getParentOperators().size() == 1);
    inputRR = operatorMap.get(input).getRowResolver();
    Table targetTable = SemanticAnalyzer.getTargetTable(qb, dest);
    ExprNodeDesc combinedConstraintExpr =
            ExprNodeTypeCheck.genConstraintsExpr(sa.getConf(), targetTable, SemanticAnalyzer.updating(dest), inputRR);

    if (combinedConstraintExpr != null) {
      constraintsOperator = OperatorUtils.createOperator(
          new FilterDesc(combinedConstraintExpr, false), new RowSchema(
              inputRR.getColumnInfos()), input);
      operatorCreated = true;
    }
  }

  private static boolean mergeCardinalityViolationBranch(final Operator input) {
    if(input instanceof SelectOperator) {
      SelectOperator selectOp = (SelectOperator)input;
      if(selectOp.getConf().getColList().size() == 1) {
        ExprNodeDesc colExpr = selectOp.getConf().getColList().get(0);
        if(colExpr instanceof ExprNodeGenericFuncDesc) {
          ExprNodeGenericFuncDesc func = (ExprNodeGenericFuncDesc)colExpr ;
          if(func.getGenericUDF() instanceof GenericUDFCardinalityViolation){
            return true;
          }
        }
      }
    }
    return false;
  }

  public boolean createdOperator() {
    return operatorCreated;
  }

  public Operator<? extends OperatorDesc> getOperator() {
    return constraintsOperator;
  }

  public RowResolver getRowResolver() {
    return inputRR;
  }
}
