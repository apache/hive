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

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * This class is used as a static factory for VectorColumnAssign.
 * Is capable of building assigners from expression nodes or from object inspectors.
 */
public class VectorColumnAssignFactory {

  private static abstract class VectorColumnAssignVectorBase<T extends ColumnVector>
    implements VectorColumnAssign {
    protected VectorizedRowBatch outBatch;
    protected T outCol;

    protected void copyValue(T in, int srcIndex, int destIndex) throws HiveException {
      throw new HiveException("Internal error: should not reach here");
    }

    @SuppressWarnings("unchecked")
    @Override
    public void assignVectorValue(VectorizedRowBatch inBatch, int batchIndex,
        int valueColumnIndex, int destIndex) throws HiveException {
      T in = (T) inBatch.cols[valueColumnIndex];
      if (in.isRepeating) {
        if (in.noNulls) {
          copyValue(in, 0, destIndex);
        }
        else {
          assignNull(destIndex);
        }
      }
      else {
        int srcIndex  = inBatch.selectedInUse ? inBatch.selected[batchIndex] : batchIndex;
        if (in.noNulls || !in.isNull[srcIndex]) {
          copyValue(in, srcIndex, destIndex);
        }
        else {
          assignNull(destIndex);
        }
      }
    }

    public VectorColumnAssign init(VectorizedRowBatch out, T cv) {
      this.outBatch = out;
      this.outCol = cv;
      return this;
    }

    protected void assignNull(int index) {
      VectorizedBatchUtil.SetNullColIsNullValue(outCol, index);
    }

    @Override
    public void reset() {
    }

    @Override
    public void assignObjectValue(Object value, int destIndex) throws HiveException {
      throw new HiveException("Internal error: should not reach here");
    }
  }

  private static abstract class VectorLongColumnAssign
    extends VectorColumnAssignVectorBase<LongColumnVector> {
    protected void assignLong(long value, int destIndex) {
      outCol.vector[destIndex] = value;
    }
  }

  private static abstract class VectorDoubleColumnAssign
    extends VectorColumnAssignVectorBase<DoubleColumnVector> {

    protected void assignDouble(double value, int destIndex) {
      outCol.vector[destIndex] = value;
    }
  }

  private static abstract class VectorBytesColumnAssign
    extends VectorColumnAssignVectorBase<BytesColumnVector> {
    byte[] pad = new byte[BytesColumnVector.DEFAULT_BUFFER_SIZE];
    int padUsed = 0;

    protected void assignBytes(byte[] buffer, int start, int length, int destIndex) {
      if (padUsed + length <= pad.length) {
        System.arraycopy(buffer, start,
            pad, padUsed, length);
        outCol.vector[destIndex] = pad;
        outCol.start[destIndex] = padUsed;
        outCol.length[destIndex] = length;
        padUsed += length;
      }
      else {
        outCol.vector[destIndex] = Arrays.copyOfRange(buffer,
            start, length);
        outCol.start[destIndex] = 0;
        outCol.length[destIndex] = length;
      }
    }

    @Override
    public void reset() {
      super.reset();
      padUsed = 0;
    }
  }


  public static VectorColumnAssign[] buildAssigners(VectorizedRowBatch outputBatch)
      throws HiveException {
    VectorColumnAssign[] vca = new VectorColumnAssign[outputBatch.cols.length];
    for(int i=0; i<vca.length; ++i) {
      ColumnVector cv = outputBatch.cols[i];
      if (cv == null) {
        continue;
      }
      else if (cv instanceof LongColumnVector) {
        vca[i] = new VectorLongColumnAssign() {
          @Override
          protected void copyValue(LongColumnVector in, int srcIndex, int destIndex) {
            assignLong(in.vector[srcIndex], destIndex);
          }
        }.init(outputBatch, (LongColumnVector) cv);
      }
      else if (cv instanceof DoubleColumnVector) {
        vca[i] = new VectorDoubleColumnAssign() {

          @Override
          protected void copyValue(DoubleColumnVector in, int srcIndex, int destIndex) {
            assignDouble(in.vector[srcIndex], destIndex);
          }
        }.init(outputBatch, (DoubleColumnVector) cv);
      }
      else if (cv instanceof BytesColumnVector) {
        vca[i] = new VectorBytesColumnAssign() {
          @Override
          protected void copyValue(BytesColumnVector src, int srcIndex, int destIndex) {
            assignBytes(src.vector[srcIndex], src.start[srcIndex], src.length[srcIndex], destIndex);
          }
        }.init(outputBatch,  (BytesColumnVector) cv);
      }
      else {
        throw new HiveException("Unimplemented vector column type: " + cv.getClass().getName());
      }
    }
    return vca;
  }

  public static VectorColumnAssign buildObjectAssign(VectorizedRowBatch outputBatch,
      int outColIndex, ObjectInspector objInspector) throws HiveException {
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector) objInspector;
    VectorColumnAssign outVCA = null;
    ColumnVector destCol = outputBatch.cols[outColIndex];
    if (destCol instanceof LongColumnVector) {
      switch(poi.getPrimitiveCategory()) {
      case BOOLEAN:
        outVCA = new VectorLongColumnAssign() {
          @Override
          public void assignObjectValue(Object val, int destIndex) throws HiveException {
            if (val == null) {
              assignNull(destIndex);
            }
            else {
              BooleanWritable bw = (BooleanWritable) val;
              assignLong(bw.get() ? 1:0, destIndex);
            }
          }
        }.init(outputBatch, (LongColumnVector) destCol);
        break;
      case BYTE:
        outVCA = new VectorLongColumnAssign() {
          @Override
          public void assignObjectValue(Object val, int destIndex) throws HiveException {
            if (val == null) {
              assignNull(destIndex);
            }
            else {
              ByteWritable bw = (ByteWritable) val;
              assignLong(bw.get(), destIndex);
            }
          }
        }.init(outputBatch, (LongColumnVector) destCol);
        break;
      case SHORT:
        outVCA = new VectorLongColumnAssign() {
          @Override
          public void assignObjectValue(Object val, int destIndex) throws HiveException {
            if (val == null) {
              assignNull(destIndex);
            }
            else {
              ShortWritable bw = (ShortWritable) val;
              assignLong(bw.get(), destIndex);
            }
          }
        }.init(outputBatch, (LongColumnVector) destCol);
        break;
      case INT:
        outVCA = new VectorLongColumnAssign() {
          @Override
          public void assignObjectValue(Object val, int destIndex) throws HiveException {
            if (val == null) {
              assignNull(destIndex);
            }
            else {
              IntWritable bw = (IntWritable) val;
              assignLong(bw.get(), destIndex);
            }
          }
        }.init(outputBatch, (LongColumnVector) destCol);
        break;
      case LONG:
        outVCA = new VectorLongColumnAssign() {
          @Override
          public void assignObjectValue(Object val, int destIndex) throws HiveException {
            if (val == null) {
              assignNull(destIndex);
            }
            else {
              LongWritable bw = (LongWritable) val;
              assignLong(bw.get(), destIndex);
            }
          }
        }.init(outputBatch, (LongColumnVector) destCol);
        break;
      case TIMESTAMP:
        outVCA = new VectorLongColumnAssign() {
          @Override
          public void assignObjectValue(Object val, int destIndex) throws HiveException {
            if (val == null) {
              assignNull(destIndex);
            }
            else {
              TimestampWritable bw = (TimestampWritable) val;
              Timestamp t = bw.getTimestamp();
              assignLong(TimestampUtils.getTimeNanoSec(t), destIndex);
            }
          }
        }.init(outputBatch, (LongColumnVector) destCol);
        break;
      default:
        throw new HiveException("Incompatible Long vector column and primitive category " +
            poi.getPrimitiveCategory());
      }
    }
    else if (destCol instanceof DoubleColumnVector) {
      switch(poi.getPrimitiveCategory()) {
      case DOUBLE:
        outVCA = new VectorDoubleColumnAssign() {
          @Override
          public void assignObjectValue(Object val, int destIndex) throws HiveException {
            if (val == null) {
              assignNull(destIndex);
            }
            else {
              DoubleWritable bw = (DoubleWritable) val;
              assignDouble(bw.get(), destIndex);
            }
          }
        }.init(outputBatch, (DoubleColumnVector) destCol);
        break;
      case FLOAT:
        outVCA = new VectorDoubleColumnAssign() {
          @Override
          public void assignObjectValue(Object val, int destIndex) throws HiveException {
            if (val == null) {
              assignNull(destIndex);
            }
            else {
              FloatWritable bw = (FloatWritable) val;
              assignDouble(bw.get(), destIndex);
            }
          }
        }.init(outputBatch, (DoubleColumnVector) destCol);
        break;
      default:
        throw new HiveException("Incompatible Double vector column and primitive category " +
            poi.getPrimitiveCategory());
      }
    }
    else if (destCol instanceof BytesColumnVector) {
      switch(poi.getPrimitiveCategory()) {
      case STRING:
        outVCA = new VectorBytesColumnAssign() {
          @Override
          public void assignObjectValue(Object val, int destIndex) throws HiveException {
            if (val == null) {
              assignNull(destIndex);
            }
            else {
              Text bw = (Text) val;
              byte[] bytes = bw.getBytes();
              assignBytes(bytes, 0, bytes.length, destIndex);
            }
          }
        }.init(outputBatch, (BytesColumnVector) destCol);
        break;
      default:
        throw new HiveException("Incompatible Bytes vector column and primitive category " +
            poi.getPrimitiveCategory());
      }
    }
    else {
      throw new HiveException("Unknown vector column type " + destCol.getClass().getName());
    }
    return outVCA;
  }

  /**
   * Builds the assigners from an object inspector and from a list of columns.
   * @param outputBatch The batch to which the assigners are bound
   * @param outputOI  The row object inspector
   * @param columnMap Vector column map
   * @param outputColumnNames Column names, used both to find the vector columns and the
   * @return
   * @throws HiveException
   */
  public static VectorColumnAssign[] buildAssigners(VectorizedRowBatch outputBatch,
      ObjectInspector outputOI,
      Map<String, Integer> columnMap,
      List<String> outputColumnNames) throws HiveException {
    StructObjectInspector soi = (StructObjectInspector) outputOI;
    VectorColumnAssign[] vcas = new VectorColumnAssign[outputColumnNames.size()];
    for (int i=0; i<outputColumnNames.size(); ++i) {
      String columnName = outputColumnNames.get(i);
      Integer columnIndex = columnMap.get(columnName);
      StructField columnRef = soi.getStructFieldRef(columnName);
      ObjectInspector valueOI = columnRef.getFieldObjectInspector();
      vcas[i] = buildObjectAssign(outputBatch, columnIndex, valueOI);
    }
    return vcas;
  }
}