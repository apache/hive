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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.Descriptor;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;

/**
 * Evaluate an IN filter on a batch for a vector of structs.
 * This is optimized so that no objects have to be created in
 * the inner loop, and there is a hash table implemented
 * with Cuckoo hashing that has fast lookup to do the IN test.
 */
public class FilterStructColumnInList extends FilterStringColumnInList implements IStructInExpr {
  private static final long serialVersionUID = 1L;
  private VectorExpression[] structExpressions;
  private ColumnVector.Type[] fieldVectorColumnTypes;
  private int[] structColumnMap;
  private int scratchBytesColumn;

  private transient Output buffer;
  private transient BinarySortableSerializeWrite binarySortableSerializeWrite;

  /**
   * After construction you must call setInListValues() to add the values to the IN set
   * (on the IStringInExpr interface).
   *
   * And, call a and b on the IStructInExpr interface.
   */
  public FilterStructColumnInList() {
    super(-1);
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    final int logicalSize = batch.size;
    if (logicalSize == 0) {
      return;
    }

    if (buffer == null) {
      buffer = new Output();
      binarySortableSerializeWrite = new BinarySortableSerializeWrite(structColumnMap.length);
    }

    for (VectorExpression ve : structExpressions) {
      ve.evaluate(batch);
    }

    BytesColumnVector scratchBytesColumnVector = (BytesColumnVector) batch.cols[scratchBytesColumn];

    try {
    boolean selectedInUse = batch.selectedInUse;
    int[] selected = batch.selected;
    for (int logical = 0; logical < logicalSize; logical++) {
      int batchIndex = (selectedInUse ? selected[logical] : logical);

      binarySortableSerializeWrite.set(buffer);
      for (int f = 0; f < structColumnMap.length; f++) {
        int fieldColumn = structColumnMap[f];
        ColumnVector colVec = batch.cols[fieldColumn];
        int adjustedIndex = (colVec.isRepeating ? 0 : batchIndex);
        if (colVec.noNulls || !colVec.isNull[adjustedIndex]) {
          switch (fieldVectorColumnTypes[f]) {
          case BYTES:
            {
              BytesColumnVector bytesColVec = (BytesColumnVector) colVec;
              byte[] bytes = bytesColVec.vector[adjustedIndex];
              int start = bytesColVec.start[adjustedIndex];
              int length = bytesColVec.length[adjustedIndex];
              binarySortableSerializeWrite.writeString(bytes, start, length);
            }
            break;

          case LONG:
            binarySortableSerializeWrite.writeLong(((LongColumnVector) colVec).vector[adjustedIndex]);
            break;

          case DOUBLE:
            binarySortableSerializeWrite.writeDouble(((DoubleColumnVector) colVec).vector[adjustedIndex]);
            break;

          case DECIMAL:
            DecimalColumnVector decColVector = ((DecimalColumnVector) colVec);
            binarySortableSerializeWrite.writeHiveDecimal(
                decColVector.vector[adjustedIndex], decColVector.scale);
            break;

          default:
            throw new RuntimeException("Unexpected vector column type " +
                fieldVectorColumnTypes[f].name());
          } 
        } else {
          binarySortableSerializeWrite.writeNull();
        }
      }
      scratchBytesColumnVector.setVal(batchIndex, buffer.getData(), 0, buffer.getLength());
    }

    // Now, take the serialized keys we just wrote into our scratch column and look them
    // up in the IN list.
    super.evaluate(batch);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
 
  }


  @Override
  public String getOutputType() {
    return "boolean";
  }

  @Override
  public int getOutputColumn() {
    return -1;
  }

  @Override
  public Descriptor getDescriptor() {

    // This VectorExpression (IN) is a special case, so don't return a descriptor.
    return null;
  }

  @Override
  public void setScratchBytesColumn(int scratchBytesColumn) {

    // Tell our super class FilterStringColumnInList it will be evaluating our scratch
    // BytesColumnVector.
    super.setInputColumn(scratchBytesColumn);
    this.scratchBytesColumn = scratchBytesColumn;
  }

  @Override
  public void setStructColumnExprs(VectorizationContext vContext,
      List<ExprNodeDesc> structColumnExprs, ColumnVector.Type[] fieldVectorColumnTypes)
          throws HiveException {

    structExpressions = vContext.getVectorExpressions(structColumnExprs);
    structColumnMap = new int[structExpressions.length];
    for (int i = 0; i < structColumnMap.length; i++) {
      VectorExpression ve = structExpressions[i];
      structColumnMap[i] = ve.getOutputColumn();
    }
    this.fieldVectorColumnTypes = fieldVectorColumnTypes;
  }

  @Override
  public String vectorExpressionParameters() {
    return "structExpressions " + Arrays.toString(structExpressions) +
        ", fieldVectorColumnTypes " + Arrays.toString(fieldVectorColumnTypes) +
        ", structColumnMap " + Arrays.toString(structColumnMap);
  }

}
