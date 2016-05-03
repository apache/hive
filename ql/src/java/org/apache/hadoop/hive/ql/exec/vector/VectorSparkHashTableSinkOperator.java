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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.SparkHashTableSinkOperator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SparkHashTableSinkDesc;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import com.google.common.annotations.VisibleForTesting;

/**
 * Vectorized version of SparkHashTableSinkOperator
 * Currently the implementation just delegates all the work to super class
 *
 * Copied from VectorFileSinkOperator
 */
public class VectorSparkHashTableSinkOperator extends SparkHashTableSinkOperator {

  private static final long serialVersionUID = 1L;

  private VectorizationContext vContext;

  // The above members are initialized by the constructor and must not be
  // transient.
  //---------------------------------------------------------------------------

  private transient boolean firstBatch;

  private transient VectorExtractRow vectorExtractRow;

  protected transient Object[] singleRow;

  /** Kryo ctor. */
  @VisibleForTesting
  public VectorSparkHashTableSinkOperator() {
    super();
  }

  public VectorSparkHashTableSinkOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  public VectorSparkHashTableSinkOperator(
      CompilationOpContext ctx, VectorizationContext vContext, OperatorDesc conf) {
    this(ctx);
    this.vContext = vContext;
    this.conf = (SparkHashTableSinkDesc) conf;
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    inputObjInspectors[0] =
        VectorizedBatchUtil.convertToStandardStructObjectInspector((StructObjectInspector) inputObjInspectors[0]);

    super.initializeOp(hconf);

    firstBatch = true;
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    VectorizedRowBatch batch = (VectorizedRowBatch) row;

    if (firstBatch) {
      vectorExtractRow = new VectorExtractRow();
      vectorExtractRow.init((StructObjectInspector) inputObjInspectors[0], vContext.getProjectedColumns());

      singleRow = new Object[vectorExtractRow.getCount()];

      firstBatch = false;
    }

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
}
