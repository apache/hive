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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.VectorDesc;
import org.apache.hadoop.hive.ql.plan.VectorReduceSinkDesc;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import com.google.common.annotations.VisibleForTesting;

public class VectorReduceSinkOperator extends ReduceSinkOperator
    implements VectorizationOperator {

  private static final long serialVersionUID = 1L;

  private VectorizationContext vContext;
  private VectorReduceSinkDesc vectorDesc;

  // The above members are initialized by the constructor and must not be
  // transient.
  //---------------------------------------------------------------------------

  private transient boolean firstBatch;

  private transient VectorExtractRow vectorExtractRow;

  protected transient Object[] singleRow;

  public VectorReduceSinkOperator(CompilationOpContext ctx,
      OperatorDesc conf, VectorizationContext vContext, VectorDesc vectorDesc)
          throws HiveException {
    this(ctx);
    ReduceSinkDesc desc = (ReduceSinkDesc) conf;
    this.conf = desc;
    this.vContext = vContext;
    this.vectorDesc = (VectorReduceSinkDesc) vectorDesc;
  }

  /** Kryo ctor. */
  @VisibleForTesting
  public VectorReduceSinkOperator() {
    super();
  }

  public VectorReduceSinkOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  @Override
  public VectorizationContext getInputVectorizationContext() {
    return vContext;
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {

    // We need a input object inspector that is for the row we will extract out of the
    // vectorized row batch, not for example, an original inspector for an ORC table, etc.
    inputObjInspectors[0] =
        VectorizedBatchUtil.convertToStandardStructObjectInspector((StructObjectInspector) inputObjInspectors[0]);

    // Call ReduceSinkOperator with new input inspector.
    super.initializeOp(hconf);

    firstBatch = true;
  }

  @Override
  public void process(Object data, int tag) throws HiveException {

    VectorizedRowBatch batch = (VectorizedRowBatch) data;
    if (firstBatch) {
      vectorExtractRow = new VectorExtractRow();
      vectorExtractRow.init((StructObjectInspector) inputObjInspectors[0], vContext.getProjectedColumns());

      singleRow = new Object[vectorExtractRow.getCount()];

      firstBatch = false;
    }

    // VectorizedBatchUtil.debugDisplayBatch( batch, "VectorReduceSinkOperator processOp ");

    if (batch.selectedInUse) {
      int selected[] = batch.selected;
      for (int logical = 0 ; logical < batch.size; logical++) {
        int batchIndex = selected[logical];
        vectorExtractRow.extractRow(batch, batchIndex, singleRow);
        super.process(singleRow, tag);
      }
    } else {
      for (int batchIndex = 0 ; batchIndex < batch.size; batchIndex++) {
        vectorExtractRow.extractRow(batch, batchIndex, singleRow);
        super.process(singleRow, tag);
      }
    }
  }

  @Override
  public VectorDesc getVectorDesc() {
    return vectorDesc;
  }
}
