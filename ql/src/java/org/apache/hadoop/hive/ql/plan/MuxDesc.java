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

package org.apache.hadoop.hive.ql.plan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.plan.Explain.Level;



/**
 * Mux operator descriptor implementation..
 *
 */
@Explain(displayName = "Mux Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class MuxDesc extends AbstractOperatorDesc {
  private static final long serialVersionUID = 1L;

  private List<List<ExprNodeDesc>> parentToKeyCols;
  private List<List<ExprNodeDesc>> parentToValueCols;
  private List<List<String>> parentToOutputKeyColumnNames;
  private List<List<String>> parentToOutputValueColumnNames;
  private List<Integer> parentToTag;
  private Map<Integer, Integer> newParentIndexToOldParentIndex;

  public MuxDesc() {
  }

  // A MuxDesc is only generated from a corresponding ReduceSinkDesc.
  public MuxDesc(List<Operator<? extends OperatorDesc>> ops){
    int numParents = ops.size();
    parentToKeyCols = new ArrayList<List<ExprNodeDesc>>(numParents);
    parentToOutputKeyColumnNames = new ArrayList<List<String>>(numParents);
    parentToValueCols = new ArrayList<List<ExprNodeDesc>>(numParents);
    parentToOutputValueColumnNames = new ArrayList<List<String>>(numParents);
    parentToTag = new ArrayList<Integer>(numParents);

    for (Operator<? extends OperatorDesc> op: ops) {
      if (op != null && op instanceof ReduceSinkOperator) {
        ReduceSinkOperator rsop = (ReduceSinkOperator)op;
        List<ExprNodeDesc> keyCols = rsop.getConf().getKeyCols();
        List<ExprNodeDesc> valueCols = rsop.getConf().getValueCols();
        List<String> outputKeyColumnNames = rsop.getConf().getOutputKeyColumnNames();
        List<String> outputValueColumnNames = rsop.getConf().getOutputValueColumnNames();
        int tag = rsop.getConf().getTag();
        parentToKeyCols.add(keyCols);
        parentToValueCols.add(valueCols);
        parentToOutputKeyColumnNames.add(outputKeyColumnNames);
        parentToOutputValueColumnNames.add(outputValueColumnNames);
        parentToTag.add(tag);
      } else {
        parentToKeyCols.add(null);
        parentToValueCols.add(null);
        parentToOutputKeyColumnNames.add(null);
        parentToOutputValueColumnNames.add(null);
        parentToTag.add(null);
      }
    }
  }

  public List<List<ExprNodeDesc>> getParentToKeyCols() {
    return parentToKeyCols;
  }

  public void setParentToKeyCols(List<List<ExprNodeDesc>> parentToKeyCols) {
    this.parentToKeyCols = parentToKeyCols;
  }

  public List<List<ExprNodeDesc>> getParentToValueCols() {
    return parentToValueCols;
  }

  public void setParentToValueCols(List<List<ExprNodeDesc>> parentToValueCols) {
    this.parentToValueCols = parentToValueCols;
  }

  public List<List<String>> getParentToOutputKeyColumnNames() {
    return parentToOutputKeyColumnNames;
  }

  public void setParentToOutputKeyColumnNames(
      List<List<String>> parentToOutputKeyColumnNames) {
    this.parentToOutputKeyColumnNames = parentToOutputKeyColumnNames;
  }

  public List<List<String>> getParentToOutputValueColumnNames() {
    return parentToOutputValueColumnNames;
  }

  public void setParentToOutputValueColumnNames(
      List<List<String>> parentToOutputValueColumnNames) {
    this.parentToOutputValueColumnNames = parentToOutputValueColumnNames;
  }

  public List<Integer> getParentToTag() {
    return parentToTag;
  }

  public void setParentToTag(List<Integer> parentToTag) {
    this.parentToTag = parentToTag;
  }

  public Map<Integer, Integer> getNewParentIndexToOldParentIndex() {
    return newParentIndexToOldParentIndex;
  }

  public void setNewParentIndexToOldParentIndex(
      Map<Integer, Integer> newParentIndexToOldParentIndex) {
    this.newParentIndexToOldParentIndex = newParentIndexToOldParentIndex;
  }
}
