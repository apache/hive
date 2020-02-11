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

package org.apache.hadoop.hive.ql.exec.util.collectoroperator;

import org.apache.hadoop.hive.ql.exec.util.rowobjects.RowTestObjects;
import org.apache.hadoop.hive.ql.exec.vector.VectorExtractRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public abstract class RowVectorCollectorTestOperator extends RowCollectorTestOperatorBase {

  private final ObjectInspector[] outputObjectInspectors;
  private final VectorExtractRow vectorExtractRow;

  public RowVectorCollectorTestOperator(TypeInfo[] outputTypeInfos,
      ObjectInspector[] outputObjectInspectors) throws HiveException {
    super();
    this.outputObjectInspectors = outputObjectInspectors;
    vectorExtractRow = new VectorExtractRow();
    vectorExtractRow.init(outputTypeInfos);
  }

  public RowVectorCollectorTestOperator(
      int[] outputProjectionColumnNums,
      TypeInfo[] outputTypeInfos,
      ObjectInspector[] outputObjectInspectors) throws HiveException {
    super();
    this.outputObjectInspectors = outputObjectInspectors;
    vectorExtractRow = new VectorExtractRow();
    vectorExtractRow.init(outputTypeInfos, outputProjectionColumnNums);
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    VectorizedRowBatch batch = (VectorizedRowBatch) row;
    rowCount += batch.size;
    boolean selectedInUse = batch.selectedInUse;
    int[] selected = batch.selected;
    for (int logical = 0; logical < batch.size; logical++) {
      int batchIndex = (selectedInUse ? selected[logical] : logical);
      Object[] rowObjects = new Object[outputObjectInspectors.length];
      vectorExtractRow.extractRow(batch, batchIndex, rowObjects);
      for (int c = 0; c < rowObjects.length; c++) {
        rowObjects[c] = ((PrimitiveObjectInspector) outputObjectInspectors[c]).copyObject(rowObjects[c]);
      }
      nextTestRow(new RowTestObjects(rowObjects));
    }
  }

  @Override
  public String getName() {
    return RowVectorCollectorTestOperator.class.getSimpleName();
  }
}