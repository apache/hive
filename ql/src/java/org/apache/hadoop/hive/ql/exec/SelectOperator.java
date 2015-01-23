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

package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;

/**
 * Select operator implementation.
 */
public class SelectOperator extends Operator<SelectDesc> implements Serializable {

  private static final long serialVersionUID = 1L;
  protected transient ExprNodeEvaluator[] eval;

  transient Object[] output;

  private transient boolean isSelectStarNoCompute = false;

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    // Just forward the row as is
    if (conf.isSelStarNoCompute()) {
      initializeChildren(hconf);
      isSelectStarNoCompute = true;
      return;
    }
    List<ExprNodeDesc> colList = conf.getColList();
    eval = new ExprNodeEvaluator[colList.size()];
    for (int i = 0; i < colList.size(); i++) {
      assert (colList.get(i) != null);
      eval[i] = ExprNodeEvaluatorFactory.get(colList.get(i));
    }
    if (HiveConf.getBoolVar(hconf, HiveConf.ConfVars.HIVEEXPREVALUATIONCACHE)) {
      eval = ExprNodeEvaluatorFactory.toCachedEvals(eval);
    }
    output = new Object[eval.length];
    LOG.info("SELECT " + inputObjInspectors[0].getTypeName());
    outputObjInspector = initEvaluatorsAndReturnStruct(eval, conf.getOutputColumnNames(),
        inputObjInspectors[0]);
    initializeChildren(hconf);
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {
    if (isSelectStarNoCompute) {
      forward(row, inputObjInspectors[tag]);
      return;
    }
    int i = 0;
    try {
      for (; i < eval.length; ++i) {
        output[i] = eval[i].evaluate(row);
      }
    } catch (HiveException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new HiveException("Error evaluating " + conf.getColList().get(i).getExprString(), e);
    }
    forward(output, outputObjInspector);
  }

  /**
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return getOperatorName();
  }

  static public String getOperatorName() {
    return "SEL";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.SELECT;
  }

  @Override
  public boolean supportSkewJoinOptimization() {
    return true;
  }

  @Override
  public boolean columnNamesRowResolvedCanBeObtained() {
    return true;
  }

  @Override
  public boolean supportAutomaticSortMergeJoin() {
    return true;
  }

  @Override
  public boolean supportUnionRemoveOptimization() {
    return true;
  }

  @Override
  public boolean acceptLimitPushdown() {
    return true;
  }

  /**
   * Checks whether this select operator does something to the
   * input tuples.
   *
   * @return if it is an identity select operator or not
   */
  public boolean isIdentitySelect() {
    // Safety check
    if(this.getNumParent() != 1) {
      return false;
    }

    if(conf.isSelStarNoCompute()) {
      return true;
    }

    // Check whether the have the same schema
    RowSchema orig = this.getSchema();
    RowSchema dest = this.getParentOperators().get(0).getSchema();
    if(orig.getSignature() == null && dest.getSignature() == null) {
      return true;
    }
    if((orig.getSignature() == null && dest.getSignature() != null) ||
        (orig.getSignature() != null && dest.getSignature() == null) ) {
      return false;
    }

    if(orig.getSignature().size() != dest.getSignature().size() ||
            orig.getSignature().size() != conf.getColList().size()) {
      return false;
    }

    for(int i=0; i<orig.getSignature().size(); i++) {
      ColumnInfo origColumn = orig.getSignature().get(i);
      ColumnInfo destColumn = dest.getSignature().get(i);

      if(origColumn == null && destColumn == null) {
        continue;
      }

      if((origColumn == null && destColumn != null) ||
          (origColumn != null && destColumn == null) ) {
        return false;
      }

      if(!origColumn.equals(destColumn)) {
        return false;
      }

      // Now we check if though the schemas are the same,
      // the operator changes the order of columns in the
      // output
      if(!(conf.getColList().get(i) instanceof ExprNodeColumnDesc)) {
        return false;
      }
      ExprNodeColumnDesc col = (ExprNodeColumnDesc) conf.getColList().get(i);
      if(!col.getColumn().equals(origColumn.getInternalName())) {
        return false;
      }

    }

    return true;
  }

}
