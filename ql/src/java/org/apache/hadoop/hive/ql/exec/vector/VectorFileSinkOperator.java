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
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriterFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * File Sink operator implementation.
 **/
public class VectorFileSinkOperator extends FileSinkOperator {

  private static final long serialVersionUID = 1L;

  protected transient Object[] singleRow;

  protected transient VectorExpressionWriter[] valueWriters;

  public VectorFileSinkOperator(VectorizationContext context,
      OperatorDesc conf) {
    super();
    this.conf = (FileSinkDesc) conf;
  }

  public VectorFileSinkOperator() {

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
                valueWriters = writers;
                inputObjInspectors[0] = objectInspector;
              }
            });
    singleRow = new Object[valueWriters.length];

    // Call FileSinkOperator with new input inspector.
    super.initializeOp(hconf);
  }

  @Override
  public void processOp(Object data, int tag) throws HiveException {
    VectorizedRowBatch vrg = (VectorizedRowBatch)data;
    for (int i = 0; i < vrg.size; i++) {
      Object[] row = getRowObject(vrg, i);
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
      singleRow[i] = valueWriters[i].writeValue(vectorColumn, batchIndex);
    }
    return singleRow;
  }
}
