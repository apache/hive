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
package org.apache.hadoop.hive.ql.exec.vector;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TopNKeyOperator;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.TopNKeyDesc;
import org.apache.hadoop.hive.ql.plan.VectorDesc;
import org.apache.hadoop.hive.ql.plan.VectorTopNKeyDesc;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * VectorTopNKeyOperator passes rows that contains top N keys only.
 */
public class VectorTopNKeyOperator extends TopNKeyOperator implements VectorizationOperator {

  private static final long serialVersionUID = 1L;

  private VectorTopNKeyDesc vectorDesc;
  private VectorizationContext vContext;

  // Extract row
  private transient Object[] extractedRow;
  private transient VectorExtractRow vectorExtractRow;

  // Batch processing
  private transient int[] temporarySelected;

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

    VectorExpression.doTransientInit(vectorDesc.getKeyExpressions());
    for (VectorExpression keyExpression : vectorDesc.getKeyExpressions()) {
      keyExpression.init(hconf);
    }

    vectorExtractRow = new VectorExtractRow();
    vectorExtractRow.init((StructObjectInspector) inputObjInspectors[0],
        vContext.getProjectedColumns());
    extractedRow = new Object[vectorExtractRow.getCount()];

    temporarySelected = new int [VectorizedRowBatch.DEFAULT_SIZE];
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

      // Get keys
      vectorExtractRow.extractRow(batch, j, extractedRow);

      // Select a row in the priority queue
      if (canProcess(extractedRow, tag)) {
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
      forward(batch, null, true);
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
}
