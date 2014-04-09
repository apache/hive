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

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class VectorizedBatchUtil {

  /**
   * Sets the IsNull value for ColumnVector at specified index
   * @param cv
   * @param rowIndex
   */
  public static void setNullColIsNullValue(ColumnVector cv, int rowIndex) {
    cv.isNull[rowIndex] = true;
    if (cv.noNulls) {
      cv.noNulls = false;
    }
  }

  /**
   * Iterates thru all the column vectors and sets noNull to
   * specified value.
   *
   * @param batch
   *          Batch on which noNull is set
   */
  public static void setNoNullFields(VectorizedRowBatch batch) {
    for (int i = 0; i < batch.numCols; i++) {
      batch.cols[i].noNulls = true;
    }
  }

  /**
   * Iterates thru all the columns in a given row and populates the batch
   * @param row Deserialized row object
   * @param oi Object insepector for that row
   * @param rowIndex index to which the row should be added to batch
   * @param batch Vectorized batch to which the row is added at rowIndex
   * @throws HiveException
   */
  public static void addRowToBatch(Object row, StructObjectInspector oi,
                                   int rowIndex,
                                   VectorizedRowBatch batch,
                                   DataOutputBuffer buffer
                                   ) throws HiveException {
    List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
    // Iterate thru the cols and load the batch
    for (int i = 0; i < fieldRefs.size(); i++) {
      Object fieldData = oi.getStructFieldData(row, fieldRefs.get(i));
      ObjectInspector foi = fieldRefs.get(i).getFieldObjectInspector();

      // Vectorization only supports PRIMITIVE data types. Assert the same
      assert (foi.getCategory() == Category.PRIMITIVE);

      // Get writable object
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) foi;
      Object writableCol = poi.getPrimitiveWritableObject(fieldData);

      // NOTE: The default value for null fields in vectorization is 1 for int types, NaN for
      // float/double. String types have no default value for null.
      switch (poi.getPrimitiveCategory()) {
      case BOOLEAN: {
        LongColumnVector lcv = (LongColumnVector) batch.cols[i];
        if (writableCol != null) {
          lcv.vector[rowIndex] = ((BooleanWritable) writableCol).get() ? 1 : 0;
          lcv.isNull[rowIndex] = false;
        } else {
          lcv.vector[rowIndex] = 1;
          setNullColIsNullValue(lcv, rowIndex);
        }
      }
        break;
      case BYTE: {
        LongColumnVector lcv = (LongColumnVector) batch.cols[i];
        if (writableCol != null) {
          lcv.vector[rowIndex] = ((ByteWritable) writableCol).get();
          lcv.isNull[rowIndex] = false;
        } else {
          lcv.vector[rowIndex] = 1;
          setNullColIsNullValue(lcv, rowIndex);
        }
      }
        break;
      case SHORT: {
        LongColumnVector lcv = (LongColumnVector) batch.cols[i];
        if (writableCol != null) {
          lcv.vector[rowIndex] = ((ShortWritable) writableCol).get();
          lcv.isNull[rowIndex] = false;
        } else {
          lcv.vector[rowIndex] = 1;
          setNullColIsNullValue(lcv, rowIndex);
        }
      }
        break;
      case INT: {
        LongColumnVector lcv = (LongColumnVector) batch.cols[i];
        if (writableCol != null) {
          lcv.vector[rowIndex] = ((IntWritable) writableCol).get();
          lcv.isNull[rowIndex] = false;
        } else {
          lcv.vector[rowIndex] = 1;
          setNullColIsNullValue(lcv, rowIndex);
        }
      }
        break;
      case LONG: {
        LongColumnVector lcv = (LongColumnVector) batch.cols[i];
        if (writableCol != null) {
          lcv.vector[rowIndex] = ((LongWritable) writableCol).get();
          lcv.isNull[rowIndex] = false;
        } else {
          lcv.vector[rowIndex] = 1;
          setNullColIsNullValue(lcv, rowIndex);
        }
      }
        break;
      case DATE: {
        LongColumnVector lcv = (LongColumnVector) batch.cols[i];
        if (writableCol != null) {
          lcv.vector[rowIndex] = ((DateWritable) writableCol).getDays();
          lcv.isNull[rowIndex] = false;
        } else {
          lcv.vector[rowIndex] = 1;
          setNullColIsNullValue(lcv, rowIndex);
        }
      }
        break;
      case FLOAT: {
        DoubleColumnVector dcv = (DoubleColumnVector) batch.cols[i];
        if (writableCol != null) {
          dcv.vector[rowIndex] = ((FloatWritable) writableCol).get();
          dcv.isNull[rowIndex] = false;
        } else {
          dcv.vector[rowIndex] = Double.NaN;
          setNullColIsNullValue(dcv, rowIndex);
        }
      }
        break;
      case DOUBLE: {
        DoubleColumnVector dcv = (DoubleColumnVector) batch.cols[i];
        if (writableCol != null) {
          dcv.vector[rowIndex] = ((DoubleWritable) writableCol).get();
          dcv.isNull[rowIndex] = false;
        } else {
          dcv.vector[rowIndex] = Double.NaN;
          setNullColIsNullValue(dcv, rowIndex);
        }
      }
        break;
      case TIMESTAMP: {
        LongColumnVector lcv = (LongColumnVector) batch.cols[i];
        if (writableCol != null) {
          Timestamp t = ((TimestampWritable) writableCol).getTimestamp();
          lcv.vector[rowIndex] = TimestampUtils.getTimeNanoSec(t);
          lcv.isNull[rowIndex] = false;
        } else {
          lcv.vector[rowIndex] = 1;
          setNullColIsNullValue(lcv, rowIndex);
        }
      }
        break;
      case STRING: {
        BytesColumnVector bcv = (BytesColumnVector) batch.cols[i];
        if (writableCol != null) {
          bcv.isNull[rowIndex] = false;
          Text colText = (Text) writableCol;
          int start = buffer.getLength();
          int length = colText.getLength();
          try {
            buffer.write(colText.getBytes(), 0, length);
          } catch (IOException ioe) {
            throw new IllegalStateException("bad write", ioe);
          }
          bcv.setRef(rowIndex, buffer.getData(), start, length);
        } else {
          setNullColIsNullValue(bcv, rowIndex);
        }
      }
        break;
      case DECIMAL:
        DecimalColumnVector dcv = (DecimalColumnVector) batch.cols[i];
        if (writableCol != null) {
          dcv.isNull[rowIndex] = false;
          HiveDecimalWritable wobj = (HiveDecimalWritable) writableCol;
          dcv.vector[rowIndex].update(wobj.getHiveDecimal().unscaledValue(),
              (short) wobj.getScale());
        } else {
          setNullColIsNullValue(dcv, rowIndex);
        }
        break;
      default:
        throw new HiveException("Vectorizaton is not supported for datatype:"
            + poi.getPrimitiveCategory());
      }
    }
  }

}
