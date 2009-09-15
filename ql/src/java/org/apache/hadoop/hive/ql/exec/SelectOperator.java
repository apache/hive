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
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.selectDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * Select operator implementation
 **/
public class SelectOperator extends Operator <selectDesc> implements Serializable {

  private static final long serialVersionUID = 1L;
  transient protected ExprNodeEvaluator[] eval;

  transient Object[] output;
  
  protected void initializeOp(Configuration hconf) throws HiveException {
    // Just forward the row as is
    if (conf.isSelStarNoCompute()) {
    	initializeChildren(hconf);
      return;
    }
    
    ArrayList<exprNodeDesc> colList = conf.getColList();
    eval = new ExprNodeEvaluator[colList.size()];
    for(int i=0; i<colList.size(); i++) {
      assert(colList.get(i) != null);
      eval[i] = ExprNodeEvaluatorFactory.get(colList.get(i));
    }
   
    output = new Object[eval.length];
    LOG.info("SELECT " + ((StructObjectInspector)inputObjInspectors[0]).getTypeName());
    outputObjInspector = initEvaluatorsAndReturnStruct(eval, conf
          .getOutputColumnNames(), inputObjInspectors[0]);
    initializeChildren(hconf);
  }

  public void processOp(Object row, int tag)
      throws HiveException {

    // Just forward the row as is
    if (conf.isSelStarNoCompute()) {
      forward(row, inputObjInspectors[tag]);
      return;
    }
    
    for(int i=0; i<eval.length; i++) {
      try {
        output[i] = eval[i].evaluate(row);
      } catch (HiveException e) {
        throw e;
      } catch (RuntimeException e) {
        throw new HiveException("Error evaluating "
            + conf.getColList().get(i).getExprString(), e);
      }
    }
    forward(output, outputObjInspector);
  }

  /**
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return new String("SEL");
  }
  
  public int getType() {
    return OperatorType.SELECT;
  }
}
