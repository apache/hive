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
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriterFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class VectorReduceSinkOperator extends ReduceSinkOperator {

  private static final long serialVersionUID = 1L;

  // Writer for producing row from input batch.
  private VectorExpressionWriter[] rowWriters;
  
  protected transient Object[] singleRow;

  public VectorReduceSinkOperator(VectorizationContext vContext, OperatorDesc conf)
      throws HiveException {
    this();
    ReduceSinkDesc desc = (ReduceSinkDesc) conf;
    this.conf = desc;
  }

  public VectorReduceSinkOperator() {
    super();
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    // We need a input object inspector that is for the row we will extract out of the
    // vectorized row batch, not for example, an original inspector for an ORC table, etc.
    VectorExpressionWriterFactory.processVectorInspector(
            (StructObjectInspector) inputObjInspectors[0],
            new VectorExpressionWriterFactory.SingleOIDClosure() {
              @Override
              public void assign(VectorExpressionWriter[] writers,
                  ObjectInspector objectInspector) {
                rowWriters = writers;
                inputObjInspectors[0] = objectInspector;
              }
            });
    singleRow = new Object[rowWriters.length];

    // Call ReduceSinkOperator with new input inspector.
    super.initializeOp(hconf);
  }

  @Override
  public void processOp(Object data, int tag) throws HiveException {
    VectorizedRowBatch vrg = (VectorizedRowBatch) data;

    for (int batchIndex = 0 ; batchIndex < vrg.size; ++batchIndex) {
      Object row = getRowObject(vrg, batchIndex);
      super.processOp(row, tag);
    }
  }

  private Object[] getRowObject(VectorizedRowBatch vrg, int rowIndex)
      throws HiveException {
    int batchIndex = rowIndex;
    if (vrg.selectedInUse) {
      batchIndex = vrg.selected[rowIndex];
    }
    for (int i = 0; i < vrg.projectionSize; i++) {
      ColumnVector vectorColumn = vrg.cols[vrg.projectedColumns[i]];
      if (vectorColumn != null) {
        singleRow[i] = rowWriters[i].writeValue(vectorColumn, batchIndex);
      } else {
        // Some columns from tables are not used.
        singleRow[i] = null;
      }
    }
    return singleRow;
  }
}
