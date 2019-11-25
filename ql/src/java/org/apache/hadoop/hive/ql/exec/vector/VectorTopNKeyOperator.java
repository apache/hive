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
package org.apache.hadoop.hive.ql.exec.vector;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TopNKeyFilter;
import org.apache.hadoop.hive.ql.exec.TopNKeyOperator;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperBase;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.TopNKeyDesc;
import org.apache.hadoop.hive.ql.plan.VectorDesc;
import org.apache.hadoop.hive.ql.plan.VectorTopNKeyDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;

/**
 * VectorTopNKeyOperator passes rows that contains top N keys only.
 */
public class VectorTopNKeyOperator extends Operator<TopNKeyDesc> implements VectorizationOperator {

  private static final long serialVersionUID = 1L;

  private VectorTopNKeyDesc vectorDesc;
  private VectorizationContext vContext;

  // Batch processing
  private transient int[] temporarySelected;
  private transient VectorHashKeyWrapperBatch keyWrappersBatch;
  private transient TopNKeyFilter<VectorHashKeyWrapperBase> topNKeyFilter;

  public VectorTopNKeyOperator(CompilationOpContext ctx, OperatorDesc conf,
      VectorizationContext vContext, VectorDesc vectorDesc) {

    this(ctx);
    this.conf = (TopNKeyDesc) conf;
    this.vContext = vContext;
    this.vectorDesc = (VectorTopNKeyDesc) vectorDesc;
  }

  /** Kryo ctor. */
  @VisibleForTesting
  public VectorTopNKeyOperator() {
    super();
  }

  public VectorTopNKeyOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);

    VectorExpression[] keyExpressions = vectorDesc.getKeyExpressions();
    VectorExpression.doTransientInit(keyExpressions, hconf);
    for (VectorExpression keyExpression : keyExpressions) {
      keyExpression.init(hconf);
    }

    temporarySelected = new int [VectorizedRowBatch.DEFAULT_SIZE];

    keyWrappersBatch = VectorHashKeyWrapperBatch.compileKeyWrapperBatch(keyExpressions);
    this.topNKeyFilter = new TopNKeyFilter<>(conf.getTopN(), keyWrappersBatch.getComparator(
            conf.getColumnSortOrder(),
            conf.getNullOrder()));
  }

  @Override
  public void process(Object data, int tag) throws HiveException {
    VectorizedRowBatch batch = (VectorizedRowBatch) data;

    // The selected vector represents selected rows.
    // Clone the selected vector
    System.arraycopy(batch.selected, 0, temporarySelected, 0, batch.size);
    int [] selectedBackup = batch.selected;
    batch.selected = temporarySelected;
    int sizeBackup = batch.size;
    boolean selectedInUseBackup = batch.selectedInUse;

    for (VectorExpression keyExpression : vectorDesc.getKeyExpressions()) {
      keyExpression.evaluate(batch);
    }

    keyWrappersBatch.evaluateBatch(batch);
    VectorHashKeyWrapperBase[] keyWrappers = keyWrappersBatch.getVectorHashKeyWrappers();

    // Filter rows with top n keys
    int size = 0;
    int[] selected = new int[batch.selected.length];
    for (int i = 0; i < batch.size; i++) {
      int j;
      if (batch.selectedInUse) {
        j = batch.selected[i];
      } else {
        j = i;
      }

      // Select a row in the priority queue
      if (topNKeyFilter.canForward(keyWrappers[i])) {
        selected[size++] = j;
      }
    }

    // Apply selection to batch
    if (batch.size != size) {
      batch.selectedInUse = true;
      batch.selected = selected;
      batch.size = size;
    }

    // Forward the result
    if (size > 0) {
      vectorForward(batch);
    }

    // Restore the original selected vector
    batch.selected = selectedBackup;
    batch.size = sizeBackup;
    batch.selectedInUse = selectedInUseBackup;
  }

  @Override
  public VectorizationContext getInputVectorizationContext() {
    return vContext;
  }

  @Override
  public VectorDesc getVectorDesc() {
    return vectorDesc;
  }

  // Must send on to VectorPTFOperator...
  @Override
  public void setNextVectorBatchGroupStatus(boolean isLastGroupBatch) throws HiveException {
    for (Operator<? extends OperatorDesc> op : childOperators) {
      op.setNextVectorBatchGroupStatus(isLastGroupBatch);
    }
  }

  @Override
  public String getName() {
    return TopNKeyOperator.getOperatorName();
  }

  @Override
  public OperatorType getType() {
    return OperatorType.TOPNKEY;
  }

  @Override
  protected void closeOp(boolean abort) throws HiveException {
    topNKeyFilter.clear();
    super.closeOp(abort);
  }

  // Because a TopNKeyOperator works like a FilterOperator with top n key condition, its properties
  // for optimizers has same values. Following methods are same with FilterOperator;
  // supportSkewJoinOptimization, columnNamesRowResolvedCanBeObtained,
  // supportAutomaticSortMergeJoin, and supportUnionRemoveOptimization.
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
}
