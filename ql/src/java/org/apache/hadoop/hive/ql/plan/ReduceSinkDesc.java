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


/**
 * ReduceSinkDesc.
 *
 */
@Explain(displayName = "Reduce Output Operator")
public class ReduceSinkDesc extends BaseReduceSinkDesc {
  private static final long serialVersionUID = 1L;

  private boolean needsOperationPathTagging;

  public boolean getNeedsOperationPathTagging() {
    return needsOperationPathTagging;
  }

  public void setNeedsOperationPathTagging(boolean isOperationPathTagged) {
    this.needsOperationPathTagging = isOperationPathTagged;
  }

  public ReduceSinkDesc() {
  }

  public ReduceSinkDesc(ArrayList<ExprNodeDesc> keyCols,
	      int numDistributionKeys,
	      ArrayList<ExprNodeDesc> valueCols,
	      ArrayList<String> outputKeyColumnNames,
	      List<List<Integer>> distinctColumnIndices,
	      ArrayList<String> outputValueColumnNames, int tag,
	      ArrayList<ExprNodeDesc> partitionCols, int numReducers,
	      final TableDesc keySerializeInfo, final TableDesc valueSerializeInfo) {
    this(keyCols, numDistributionKeys, valueCols,
      outputKeyColumnNames, distinctColumnIndices, outputValueColumnNames, tag,
      partitionCols, numReducers, keySerializeInfo, valueSerializeInfo, false);
  }

  public ReduceSinkDesc(ArrayList<ExprNodeDesc> keyCols,
      int numDistributionKeys,
      ArrayList<ExprNodeDesc> valueCols,
      ArrayList<String> outputKeyColumnNames,
      List<List<Integer>> distinctColumnIndices,
      ArrayList<String> outputValueColumnNames, int tag,
      ArrayList<ExprNodeDesc> partitionCols, int numReducers,
      final TableDesc keySerializeInfo, final TableDesc valueSerializeInfo,
      boolean needsOperationPathTagging) {
    this.keyCols = keyCols;
    this.numDistributionKeys = numDistributionKeys;
    this.valueCols = valueCols;
    this.outputKeyColumnNames = outputKeyColumnNames;
    this.outputValueColumnNames = outputValueColumnNames;
    this.tag = tag;
    this.numReducers = numReducers;
    this.partitionCols = partitionCols;
    this.keySerializeInfo = keySerializeInfo;
    this.valueSerializeInfo = valueSerializeInfo;
    this.distinctColumnIndices = distinctColumnIndices;
    this.needsOperationPathTagging = needsOperationPathTagging;
  }

  @Override
  public Object clone() {
    ReduceSinkDesc desc = new ReduceSinkDesc();
    desc.setKeyCols((ArrayList<ExprNodeDesc>) getKeyCols().clone());
    desc.setValueCols((ArrayList<ExprNodeDesc>) getValueCols().clone());
    desc.setOutputKeyColumnNames((ArrayList<String>) getOutputKeyColumnNames().clone());
    List<List<Integer>> distinctColumnIndicesClone = new ArrayList<List<Integer>>();
    for (List<Integer> distinctColumnIndex : getDistinctColumnIndices()) {
      List<Integer> tmp = new ArrayList<Integer>();
      tmp.addAll(distinctColumnIndex);
      distinctColumnIndicesClone.add(tmp);
    }
    desc.setDistinctColumnIndices(distinctColumnIndicesClone);
    desc.setOutputValueColumnNames((ArrayList<String>) getOutputValueColumnNames().clone());
    desc.setNumDistributionKeys(getNumDistributionKeys());
    desc.setTag(getTag());
    desc.setNumReducers(getNumReducers());
    desc.setPartitionCols((ArrayList<ExprNodeDesc>) getPartitionCols().clone());
    desc.setKeySerializeInfo((TableDesc) getKeySerializeInfo().clone());
    desc.setValueSerializeInfo((TableDesc) getValueSerializeInfo().clone());
    desc.setNeedsOperationPathTagging(needsOperationPathTagging);
    return desc;
  }
}
