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

import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression;

/**
 * VectorGroupByDesc.
 *
 * Extra parameters beyond GroupByDesc just for the VectorGroupByOperator.
 *
 * We don't extend GroupByDesc because the base OperatorDesc doesn't support
 * clone and adding it is a lot work for little gain.
 */
public class VectorGroupByDesc extends AbstractVectorDesc  {

  private static final long serialVersionUID = 1L;

  /**
   *     GLOBAL         No key.  All rows --> 1 full aggregation on end of input
   *
   *     HASH           Rows aggregated in to hash table on group key -->
   *                        1 partial aggregation per key (normally, unless there is spilling)
   *
   *     MERGE_PARTIAL  As first operator in a REDUCER, partial aggregations come grouped from
   *                    reduce-shuffle -->
   *                        aggregate the partial aggregations and emit full aggregation on
   *                        endGroup / closeOp
   *
   *     STREAMING      Rows come from PARENT operator already grouped -->
   *                        aggregate the rows and emit full aggregation on key change / closeOp
   *
   *     NOTE: Hash can spill partial result rows prematurely if it runs low on memory.
   *     NOTE: Streaming has to compare keys where MergePartial gets an endGroup call.
   */
  public static enum ProcessingMode {
    NONE,
    GLOBAL,
    HASH,
    MERGE_PARTIAL,
    STREAMING
  };

  private ProcessingMode processingMode;

  private boolean isVectorOutput;

  private VectorExpression[] keyExpressions;
  private VectorAggregateExpression[] aggregators;
  private int[] projectedOutputColumns;

  public VectorGroupByDesc() {
    this.processingMode = ProcessingMode.NONE;
    this.isVectorOutput = false;
  }

  public void setProcessingMode(ProcessingMode processingMode) {
    this.processingMode = processingMode;
  }
  public ProcessingMode getProcessingMode() {
    return processingMode;
  }

  public boolean isVectorOutput() {
    return isVectorOutput;
  }

  public void setVectorOutput(boolean isVectorOutput) {
    this.isVectorOutput = isVectorOutput;
  }

  public void setKeyExpressions(VectorExpression[] keyExpressions) {
    this.keyExpressions = keyExpressions;
  }

  public VectorExpression[] getKeyExpressions() {
    return keyExpressions;
  }

  public void setAggregators(VectorAggregateExpression[] aggregators) {
    this.aggregators = aggregators;
  }

  public VectorAggregateExpression[] getAggregators() {
    return aggregators;
  }

  public void setProjectedOutputColumns(int[] projectedOutputColumns) {
    this.projectedOutputColumns = projectedOutputColumns;
  }

  public int[] getProjectedOutputColumns() {
    return projectedOutputColumns;
  }

  /**
   * Which ProcessingMode for VectorGroupByOperator?
   *
   *     Decides using GroupByDesc.Mode and whether there are keys.
   *
   *         Mode.COMPLETE      --> (numKeys == 0 ? ProcessingMode.GLOBAL : ProcessingMode.STREAMING)
   *
   *         Mode.HASH          --> ProcessingMode.HASH
   *
   *         Mode.MERGEPARTIAL  --> (numKeys == 0 ? ProcessingMode.GLOBAL : ProcessingMode.MERGE_PARTIAL)
   *
   *         Mode.PARTIAL1,
   *         Mode.PARTIAL2,
   *         Mode.PARTIALS,
   *         Mode.FINAL        --> ProcessingMode.STREAMING
   *
   */
  public static ProcessingMode groupByDescModeToVectorProcessingMode(GroupByDesc.Mode mode,
      boolean hasKeys) {
    switch (mode) {
    case COMPLETE:
      return (hasKeys ? ProcessingMode.STREAMING : ProcessingMode.GLOBAL);
    case HASH:
      return ProcessingMode.HASH;
    case MERGEPARTIAL:
      return (hasKeys ? ProcessingMode.MERGE_PARTIAL : ProcessingMode.GLOBAL);
    case PARTIAL1:
    case PARTIAL2:
    case PARTIALS:
    case FINAL:
      return ProcessingMode.STREAMING;
    default:
      throw new RuntimeException("Unexpected GROUP BY mode " + mode.name());
    }
  }
}
