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

import java.util.Collection;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.spark.SparkPartitionPruningSinkDesc;
import org.apache.hadoop.hive.ql.parse.spark.SparkPartitionPruningSinkOperator;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;

/**
 * Vectorized version for SparkPartitionPruningSinkOperator.
 * Forked from VectorAppMasterEventOperator.
 **/
public class VectorSparkPartitionPruningSinkOperator extends SparkPartitionPruningSinkOperator {

  private static final long serialVersionUID = 1L;

  private VectorizationContext vContext;

  protected transient boolean firstBatch;

  protected transient VectorExtractRowDynBatch vectorExtractRowDynBatch;

  protected transient Object[] singleRow;

  public VectorSparkPartitionPruningSinkOperator(CompilationOpContext ctx,
      VectorizationContext context, OperatorDesc conf) {
    this(ctx);
    this.conf = (SparkPartitionPruningSinkDesc) conf;
    this.vContext = context;
  }

  /** Kryo ctor. */
  protected VectorSparkPartitionPruningSinkOperator() {
    super();
  }

  public VectorSparkPartitionPruningSinkOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  @Override
  public void initializeOp(Configuration hconf) throws HiveException {
    inputObjInspectors[0] =
        VectorizedBatchUtil.convertToStandardStructObjectInspector(
            (StructObjectInspector) inputObjInspectors[0]);
    super.initializeOp(hconf);

    firstBatch = true;
  }

  @Override
  public void process(Object data, int tag) throws HiveException {
    VectorizedRowBatch batch = (VectorizedRowBatch) data;
    if (firstBatch) {
      vectorExtractRowDynBatch = new VectorExtractRowDynBatch();
      vectorExtractRowDynBatch.init((StructObjectInspector) inputObjInspectors[0],
          vContext.getProjectedColumns());
      singleRow = new Object[vectorExtractRowDynBatch.getCount()];
      firstBatch = false;
    }

    vectorExtractRowDynBatch.setBatchOnEntry(batch);
    ObjectInspector rowInspector = inputObjInspectors[0];
    try {
      Writable writableRow;
      for (int logical = 0; logical < batch.size; logical++) {
        int batchIndex = batch.selectedInUse ? batch.selected[logical] : logical;
        vectorExtractRowDynBatch.extractRow(batchIndex, singleRow);
        writableRow = serializer.serialize(singleRow, rowInspector);
        writableRow.write(buffer);
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }

    vectorExtractRowDynBatch.forgetBatchOnExit();
  }
}
