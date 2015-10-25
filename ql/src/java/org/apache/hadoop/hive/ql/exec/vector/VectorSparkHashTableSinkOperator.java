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
import org.apache.hadoop.hive.ql.exec.SparkHashTableSinkOperator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SparkHashTableSinkDesc;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.util.Collection;
import java.util.concurrent.Future;

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

  private transient VectorExtractRowDynBatch vectorExtractRowDynBatch;

  protected transient Object[] singleRow;

  public VectorSparkHashTableSinkOperator() {
  }

  public VectorSparkHashTableSinkOperator(VectorizationContext vContext, OperatorDesc conf) {
    super();
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
      vectorExtractRowDynBatch = new VectorExtractRowDynBatch();
      vectorExtractRowDynBatch.init((StructObjectInspector) inputObjInspectors[0], vContext.getProjectedColumns());

      singleRow = new Object[vectorExtractRowDynBatch.getCount()];

      firstBatch = false;
    }
    vectorExtractRowDynBatch.setBatchOnEntry(batch);
    if (batch.selectedInUse) {
      int selected[] = batch.selected;
      for (int logical = 0 ; logical < batch.size; logical++) {
        int batchIndex = selected[logical];
        vectorExtractRowDynBatch.extractRow(batchIndex, singleRow);
        super.process(singleRow, tag);
      }
    } else {
      for (int batchIndex = 0 ; batchIndex < batch.size; batchIndex++) {
        vectorExtractRowDynBatch.extractRow(batchIndex, singleRow);
        super.process(singleRow, tag);
      }
    }

    vectorExtractRowDynBatch.forgetBatchOnExit();
  }
}
