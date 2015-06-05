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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * This class copies specified columns of a row from one VectorizedRowBatch to another.
 */
public class VectorCopyRow {

  protected static transient final Log LOG = LogFactory.getLog(VectorCopyRow.class);

  private abstract class CopyRow {
    protected int inColumnIndex;
    protected int outColumnIndex;

    CopyRow(int inColumnIndex, int outColumnIndex) {
      this.inColumnIndex = inColumnIndex;
      this.outColumnIndex = outColumnIndex;
    }

    abstract void copy(VectorizedRowBatch inBatch, int inBatchIndex, VectorizedRowBatch outBatch, int outBatchIndex);
  }

  private class LongCopyRow extends CopyRow {

    LongCopyRow(int inColumnIndex, int outColumnIndex) {
      super(inColumnIndex, outColumnIndex);
    }

    @Override
    void copy(VectorizedRowBatch inBatch, int inBatchIndex, VectorizedRowBatch outBatch, int outBatchIndex) {
      LongColumnVector inColVector = (LongColumnVector) inBatch.cols[inColumnIndex];
      LongColumnVector outColVector = (LongColumnVector) outBatch.cols[outColumnIndex];

      if (inColVector.isRepeating) {
        if (inColVector.noNulls || !inColVector.isNull[0]) {
          outColVector.vector[outBatchIndex] = inColVector.vector[0];
        } else {
          VectorizedBatchUtil.setNullColIsNullValue(outColVector, outBatchIndex);
        }
      } else {
        if (inColVector.noNulls || !inColVector.isNull[inBatchIndex]) {
          outColVector.vector[outBatchIndex] = inColVector.vector[inBatchIndex];
        } else {
          VectorizedBatchUtil.setNullColIsNullValue(outColVector, outBatchIndex);
        }
      }
    }
  }

  private class DoubleCopyRow extends CopyRow {

    DoubleCopyRow(int inColumnIndex, int outColumnIndex) {
      super(inColumnIndex, outColumnIndex);
    }

    @Override
    void copy(VectorizedRowBatch inBatch, int inBatchIndex, VectorizedRowBatch outBatch, int outBatchIndex) {
      DoubleColumnVector inColVector = (DoubleColumnVector) inBatch.cols[inColumnIndex];
      DoubleColumnVector outColVector = (DoubleColumnVector) outBatch.cols[outColumnIndex];

      if (inColVector.isRepeating) {
        if (inColVector.noNulls || !inColVector.isNull[0]) {
          outColVector.vector[outBatchIndex] = inColVector.vector[0];
        } else {
          VectorizedBatchUtil.setNullColIsNullValue(outColVector, outBatchIndex);
        }
      } else {
        if (inColVector.noNulls || !inColVector.isNull[inBatchIndex]) {
          outColVector.vector[outBatchIndex] = inColVector.vector[inBatchIndex];
        } else {
          VectorizedBatchUtil.setNullColIsNullValue(outColVector, outBatchIndex);
        }
      }
    }
  }

  private abstract class AbstractBytesCopyRow extends CopyRow {
 
    AbstractBytesCopyRow(int inColumnIndex, int outColumnIndex) {
      super(inColumnIndex, outColumnIndex);
    }

  }

  private class BytesCopyRowByValue extends AbstractBytesCopyRow {

    BytesCopyRowByValue(int inColumnIndex, int outColumnIndex) {
      super(inColumnIndex, outColumnIndex);
    }

    @Override
    void copy(VectorizedRowBatch inBatch, int inBatchIndex, VectorizedRowBatch outBatch, int outBatchIndex) {
      BytesColumnVector inColVector = (BytesColumnVector) inBatch.cols[inColumnIndex];
      BytesColumnVector outColVector = (BytesColumnVector) outBatch.cols[outColumnIndex];

      if (inColVector.isRepeating) {
        if (inColVector.noNulls || !inColVector.isNull[0]) {
          outColVector.setVal(outBatchIndex, inColVector.vector[0], inColVector.start[0], inColVector.length[0]);
        } else {
          VectorizedBatchUtil.setNullColIsNullValue(outColVector, outBatchIndex);
        }
      } else {
        if (inColVector.noNulls || !inColVector.isNull[inBatchIndex]) {
          outColVector.setVal(outBatchIndex, inColVector.vector[inBatchIndex], inColVector.start[inBatchIndex], inColVector.length[inBatchIndex]);
        } else {
          VectorizedBatchUtil.setNullColIsNullValue(outColVector, outBatchIndex);
        }
      }
    }
  }

  private class BytesCopyRowByReference extends AbstractBytesCopyRow {

    BytesCopyRowByReference(int inColumnIndex, int outColumnIndex) {
      super(inColumnIndex, outColumnIndex);
    }

    @Override
    void copy(VectorizedRowBatch inBatch, int inBatchIndex, VectorizedRowBatch outBatch, int outBatchIndex) {
      BytesColumnVector inColVector = (BytesColumnVector) inBatch.cols[inColumnIndex];
      BytesColumnVector outColVector = (BytesColumnVector) outBatch.cols[outColumnIndex];

      if (inColVector.isRepeating) {
        if (inColVector.noNulls || !inColVector.isNull[0]) {
          outColVector.setRef(outBatchIndex, inColVector.vector[0], inColVector.start[0], inColVector.length[0]);
        } else {
          VectorizedBatchUtil.setNullColIsNullValue(outColVector, outBatchIndex);
        }
      } else {
        if (inColVector.noNulls || !inColVector.isNull[inBatchIndex]) {
          outColVector.setRef(outBatchIndex, inColVector.vector[inBatchIndex], inColVector.start[inBatchIndex], inColVector.length[inBatchIndex]);
        } else {
          VectorizedBatchUtil.setNullColIsNullValue(outColVector, outBatchIndex);
        }
      }
    }
  }

  private class DecimalCopyRow extends CopyRow {

    DecimalCopyRow(int inColumnIndex, int outColumnIndex) {
      super(inColumnIndex, outColumnIndex);
    }

    @Override
    void copy(VectorizedRowBatch inBatch, int inBatchIndex, VectorizedRowBatch outBatch, int outBatchIndex) {
      DecimalColumnVector inColVector = (DecimalColumnVector) inBatch.cols[inColumnIndex];
      DecimalColumnVector outColVector = (DecimalColumnVector) outBatch.cols[outColumnIndex];

      if (inColVector.isRepeating) {
        if (inColVector.noNulls || !inColVector.isNull[0]) {
          outColVector.set(outBatchIndex, inColVector.vector[0]);
        } else {
          VectorizedBatchUtil.setNullColIsNullValue(outColVector, outBatchIndex);
        }
      } else {
        if (inColVector.noNulls || !inColVector.isNull[inBatchIndex]) {
          outColVector.set(outBatchIndex, inColVector.vector[inBatchIndex]);
        } else {
          VectorizedBatchUtil.setNullColIsNullValue(outColVector, outBatchIndex);
        }
      }
    }
  }

  private CopyRow[] subRowToBatchCopiersByValue;
  private CopyRow[] subRowToBatchCopiersByReference;

  public void init(VectorColumnMapping columnMapping) throws HiveException {
    int count = columnMapping.getCount();
    subRowToBatchCopiersByValue = new CopyRow[count];
    subRowToBatchCopiersByReference = new CopyRow[count];

    for (int i = 0; i < count; i++) {
      int inputColumn = columnMapping.getInputColumns()[i];
      int outputColumn = columnMapping.getOutputColumns()[i];
      String typeName = columnMapping.getTypeNames()[i].toLowerCase();
      TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeName);
      Type columnVectorType = VectorizationContext.getColumnVectorTypeFromTypeInfo(typeInfo);

      CopyRow copyRowByValue = null;
      CopyRow copyRowByReference = null;

      switch (columnVectorType) {
      case LONG:
        copyRowByValue = new LongCopyRow(inputColumn, outputColumn);
        break;

      case DOUBLE:
        copyRowByValue = new DoubleCopyRow(inputColumn, outputColumn);
        break;

      case BYTES:
        copyRowByValue = new BytesCopyRowByValue(inputColumn, outputColumn);
        copyRowByReference = new BytesCopyRowByReference(inputColumn, outputColumn);
        break;

      case DECIMAL:
        copyRowByValue = new DecimalCopyRow(inputColumn, outputColumn);
        break;

      default:
        throw new HiveException("Unexpected column vector type " + columnVectorType);
      }

      subRowToBatchCopiersByValue[i] = copyRowByValue;
      if (copyRowByReference == null) {
        subRowToBatchCopiersByReference[i] = copyRowByValue;
      } else {
        subRowToBatchCopiersByReference[i] = copyRowByReference;
      }
    }
  }

  /*
   * Use this copy method when the source batch may get reused before the target batch is finished.
   * Any bytes column vector values will be copied to the target by value into the column's
   * data buffer.
   */
  public void copyByValue(VectorizedRowBatch inBatch, int inBatchIndex, VectorizedRowBatch outBatch, int outBatchIndex) {
    for (CopyRow copyRow : subRowToBatchCopiersByValue) {
      copyRow.copy(inBatch, inBatchIndex, outBatch, outBatchIndex);
    }
  }

  /*
   * Use this copy method when the source batch is safe and will remain around until the target
   * batch is finished.
   *
   * Any bytes column vector values will be referenced by the target column instead of copying.
   */
  public void copyByReference(VectorizedRowBatch inBatch, int inBatchIndex, VectorizedRowBatch outBatch, int outBatchIndex) {
    for (CopyRow copyRow : subRowToBatchCopiersByReference) {
      copyRow.copy(inBatch, inBatchIndex, outBatch, outBatchIndex);
    }
  }
}