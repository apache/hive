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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hive.common.util.DateUtils;

public class VectorizedBatchUtil {
  private static final Log LOG = LogFactory.getLog(VectorizedBatchUtil.class);

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
   * Iterates thru all the column vectors and sets repeating to
   * specified column.
   *
   */
  public static void setRepeatingColumn(VectorizedRowBatch batch, int column) {
    ColumnVector cv = batch.cols[column];
    cv.isRepeating = true;
  }

  /**
   * Reduce the batch size for a vectorized row batch
   */
  public static void setBatchSize(VectorizedRowBatch batch, int size) {
    assert (size <= batch.getMaxSize());
    batch.size = size;
  }

  /**
   * Walk through the object inspector and add column vectors
   *
   * @param oi
   * @param cvList
   *          ColumnVectors are populated in this list
   */
  private static void allocateColumnVector(StructObjectInspector oi,
      List<ColumnVector> cvList) throws HiveException {
    if (cvList == null) {
      throw new HiveException("Null columnvector list");
    }
    if (oi == null) {
      return;
    }
    final List<? extends StructField> fields = oi.getAllStructFieldRefs();
    for(StructField field : fields) {
      ObjectInspector fieldObjectInspector = field.getFieldObjectInspector();
      switch(fieldObjectInspector.getCategory()) {
      case PRIMITIVE:
        PrimitiveObjectInspector poi = (PrimitiveObjectInspector) fieldObjectInspector;
        switch(poi.getPrimitiveCategory()) {
        case BOOLEAN:
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
        case TIMESTAMP:
        case DATE:
        case INTERVAL_YEAR_MONTH:
        case INTERVAL_DAY_TIME:
          cvList.add(new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE));
          break;
        case FLOAT:
        case DOUBLE:
          cvList.add(new DoubleColumnVector(VectorizedRowBatch.DEFAULT_SIZE));
          break;
        case BINARY:
        case STRING:
        case CHAR:
        case VARCHAR:
          cvList.add(new BytesColumnVector(VectorizedRowBatch.DEFAULT_SIZE));
          break;
        case DECIMAL:
          DecimalTypeInfo tInfo = (DecimalTypeInfo) poi.getTypeInfo();
          cvList.add(new DecimalColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
              tInfo.precision(), tInfo.scale()));
          break;
        default:
          throw new HiveException("Vectorizaton is not supported for datatype:"
              + poi.getPrimitiveCategory());
        }
        break;
      case STRUCT:
        throw new HiveException("Struct not supported");
      default:
        throw new HiveException("Flattening is not supported for datatype:"
            + fieldObjectInspector.getCategory());
      }
    }
  }


  /**
   * Create VectorizedRowBatch from ObjectInspector
   *
   * @param oi
   * @return
   * @throws HiveException
   */
  public static VectorizedRowBatch constructVectorizedRowBatch(
      StructObjectInspector oi) throws HiveException {
    final List<ColumnVector> cvList = new LinkedList<ColumnVector>();
    allocateColumnVector(oi, cvList);
    final VectorizedRowBatch result = new VectorizedRowBatch(cvList.size());
    int i = 0;
    for(ColumnVector cv : cvList) {
      result.cols[i++] = cv;
    }
    return result;
  }

  /**
   * Create VectorizedRowBatch from key and value object inspectors
   *
   * @param keyInspector
   * @param valueInspector
   * @return VectorizedRowBatch
   * @throws HiveException
   */
  public static VectorizedRowBatch constructVectorizedRowBatch(
      StructObjectInspector keyInspector, StructObjectInspector valueInspector)
          throws HiveException {
    final List<ColumnVector> cvList = new LinkedList<ColumnVector>();
    allocateColumnVector(keyInspector, cvList);
    allocateColumnVector(valueInspector, cvList);
    final VectorizedRowBatch result = new VectorizedRowBatch(cvList.size());
    result.cols = cvList.toArray(result.cols);
    return result;
  }

  /**
   * Iterates through all columns in a given row and populates the batch
   *
   * @param row
   * @param oi
   * @param rowIndex
   * @param batch
   * @param buffer
   * @throws HiveException
   */
  public static void addRowToBatch(Object row, StructObjectInspector oi,
          int rowIndex,
          VectorizedRowBatch batch,
          DataOutputBuffer buffer
          ) throws HiveException {
    addRowToBatchFrom(row, oi, rowIndex, 0, batch, buffer);
  }

  /**
   * Iterates thru all the columns in a given row and populates the batch
   * from a given offset
   *
   * @param row Deserialized row object
   * @param oi Object insepector for that row
   * @param rowIndex index to which the row should be added to batch
   * @param colOffset offset from where the column begins
   * @param batch Vectorized batch to which the row is added at rowIndex
   * @throws HiveException
   */
  public static void addRowToBatchFrom(Object row, StructObjectInspector oi,
                                   int rowIndex,
                                   int colOffset,
                                   VectorizedRowBatch batch,
                                   DataOutputBuffer buffer
                                   ) throws HiveException {
    List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
    final int off = colOffset;
    // Iterate thru the cols and load the batch
    for (int i = 0; i < fieldRefs.size(); i++) {
      setVector(row, oi, fieldRefs.get(i), batch, buffer, rowIndex, i, off);
    }
  }

  /**
   * Add only the projected column of a regular row to the specified vectorized row batch
   * @param row the regular row
   * @param oi object inspector for the row
   * @param rowIndex the offset to add in the batch
   * @param batch vectorized row batch
   * @param buffer data output buffer
   * @throws HiveException
   */
  public static void addProjectedRowToBatchFrom(Object row, StructObjectInspector oi,
      int rowIndex, VectorizedRowBatch batch, DataOutputBuffer buffer) throws HiveException {
    List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
    for (int i = 0; i < fieldRefs.size(); i++) {
      int projectedOutputCol = batch.projectedColumns[i];
      if (batch.cols[projectedOutputCol] == null) {
        continue;
      }
      setVector(row, oi, fieldRefs.get(i), batch, buffer, rowIndex, projectedOutputCol, 0);
    }
  }
  /**
   * Iterates thru all the columns in a given row and populates the batch
   * from a given offset
   *
   * @param row Deserialized row object
   * @param oi Object insepector for that row
   * @param rowIndex index to which the row should be added to batch
   * @param batch Vectorized batch to which the row is added at rowIndex
   * @param context context object for this vectorized batch
   * @param buffer
   * @throws HiveException
   */
  public static void acidAddRowToBatch(Object row,
                                       StructObjectInspector oi,
                                       int rowIndex,
                                       VectorizedRowBatch batch,
                                       VectorizedRowBatchCtx context,
                                       DataOutputBuffer buffer) throws HiveException {
    List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
    // Iterate thru the cols and load the batch
    for (int i = 0; i < fieldRefs.size(); i++) {
      if (batch.cols[i] == null) {
        // This means the column was not included in the projection from the underlying read
        continue;
      }
      if (context.isPartitionCol(i)) {
        // The value will have already been set before we're called, so don't overwrite it
        continue;
      }
      setVector(row, oi, fieldRefs.get(i), batch, buffer, rowIndex, i, 0);
    }
  }

  private static void setVector(Object row,
                                StructObjectInspector oi,
                                StructField field,
                                VectorizedRowBatch batch,
                                DataOutputBuffer buffer,
                                int rowIndex,
                                int colIndex,
                                int offset) throws HiveException {

    Object fieldData = oi.getStructFieldData(row, field);
    ObjectInspector foi = field.getFieldObjectInspector();

    // Vectorization only supports PRIMITIVE data types. Assert the same
    assert (foi.getCategory() == Category.PRIMITIVE);

    // Get writable object
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector) foi;
    Object writableCol = poi.getPrimitiveWritableObject(fieldData);

    // NOTE: The default value for null fields in vectorization is 1 for int types, NaN for
    // float/double. String types have no default value for null.
    switch (poi.getPrimitiveCategory()) {
    case BOOLEAN: {
      LongColumnVector lcv = (LongColumnVector) batch.cols[offset + colIndex];
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
      LongColumnVector lcv = (LongColumnVector) batch.cols[offset + colIndex];
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
      LongColumnVector lcv = (LongColumnVector) batch.cols[offset + colIndex];
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
      LongColumnVector lcv = (LongColumnVector) batch.cols[offset + colIndex];
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
      LongColumnVector lcv = (LongColumnVector) batch.cols[offset + colIndex];
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
      LongColumnVector lcv = (LongColumnVector) batch.cols[offset + colIndex];
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
      DoubleColumnVector dcv = (DoubleColumnVector) batch.cols[offset + colIndex];
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
      DoubleColumnVector dcv = (DoubleColumnVector) batch.cols[offset + colIndex];
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
      LongColumnVector lcv = (LongColumnVector) batch.cols[offset + colIndex];
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
    case INTERVAL_YEAR_MONTH: {
      LongColumnVector lcv = (LongColumnVector) batch.cols[offset + colIndex];
      if (writableCol != null) {
        HiveIntervalYearMonth i = ((HiveIntervalYearMonthWritable) writableCol).getHiveIntervalYearMonth();
        lcv.vector[rowIndex] = i.getTotalMonths();
        lcv.isNull[rowIndex] = false;
      } else {
        lcv.vector[rowIndex] = 1;
        setNullColIsNullValue(lcv, rowIndex);
      }
    }
      break;
    case INTERVAL_DAY_TIME: {
      LongColumnVector lcv = (LongColumnVector) batch.cols[offset + colIndex];
      if (writableCol != null) {
        HiveIntervalDayTime i = ((HiveIntervalDayTimeWritable) writableCol).getHiveIntervalDayTime();
        lcv.vector[rowIndex] = DateUtils.getIntervalDayTimeTotalNanos(i);
        lcv.isNull[rowIndex] = false;
      } else {
        lcv.vector[rowIndex] = 1;
        setNullColIsNullValue(lcv, rowIndex);
      }
    }
      break;
    case BINARY: {
      BytesColumnVector bcv = (BytesColumnVector) batch.cols[offset + colIndex];
      if (writableCol != null) {
          bcv.isNull[rowIndex] = false;
          BytesWritable bw = (BytesWritable) writableCol;
          byte[] bytes = bw.getBytes();
          int start = buffer.getLength();
          int length = bw.getLength();
          try {
            buffer.write(bytes, 0, length);
          } catch (IOException ioe) {
            throw new IllegalStateException("bad write", ioe);
          }
          bcv.setRef(rowIndex, buffer.getData(), start, length);
      } else {
        setNullColIsNullValue(bcv, rowIndex);
      }
    }
      break;
    case STRING: {
      BytesColumnVector bcv = (BytesColumnVector) batch.cols[offset + colIndex];
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
    case CHAR: {
      BytesColumnVector bcv = (BytesColumnVector) batch.cols[offset + colIndex];
      if (writableCol != null) {
        bcv.isNull[rowIndex] = false;
        HiveChar colHiveChar = ((HiveCharWritable) writableCol).getHiveChar();
        byte[] bytes = colHiveChar.getStrippedValue().getBytes();

        // We assume the CHAR maximum length was enforced when the object was created.
        int length = bytes.length;

        int start = buffer.getLength();
        try {
          // In vector mode, we store CHAR as unpadded.
          buffer.write(bytes, 0, length);
        } catch (IOException ioe) {
          throw new IllegalStateException("bad write", ioe);
        }
        bcv.setRef(rowIndex, buffer.getData(), start, length);
      } else {
        setNullColIsNullValue(bcv, rowIndex);
      }
    }
      break;
    case VARCHAR: {
        BytesColumnVector bcv = (BytesColumnVector) batch.cols[offset + colIndex];
        if (writableCol != null) {
          bcv.isNull[rowIndex] = false;
          HiveVarchar colHiveVarchar = ((HiveVarcharWritable) writableCol).getHiveVarchar();
          byte[] bytes = colHiveVarchar.getValue().getBytes();

          // We assume the VARCHAR maximum length was enforced when the object was created.
          int length = bytes.length;

          int start = buffer.getLength();
          try {
            buffer.write(bytes, 0, length);
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
      DecimalColumnVector dcv = (DecimalColumnVector) batch.cols[offset + colIndex];
      if (writableCol != null) {
        dcv.isNull[rowIndex] = false;
        HiveDecimalWritable wobj = (HiveDecimalWritable) writableCol;
        dcv.set(rowIndex, wobj);
      } else {
        setNullColIsNullValue(dcv, rowIndex);
      }
      break;
    default:
      throw new HiveException("Vectorizaton is not supported for datatype:" +
          poi.getPrimitiveCategory());
    }
  }

  public static StandardStructObjectInspector convertToStandardStructObjectInspector(
      StructObjectInspector structObjectInspector) throws HiveException {

    List<? extends StructField> fields = structObjectInspector.getAllStructFieldRefs();
    List<ObjectInspector> oids = new ArrayList<ObjectInspector>();
    ArrayList<String> columnNames = new ArrayList<String>();

    for(StructField field : fields) {
      TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(
          field.getFieldObjectInspector().getTypeName());
      ObjectInspector standardWritableObjectInspector = 
              TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(typeInfo);
      oids.add(standardWritableObjectInspector);
      columnNames.add(field.getFieldName());
    }
    return ObjectInspectorFactory.getStandardStructObjectInspector(columnNames,oids);
  }

  public static PrimitiveTypeInfo[] primitiveTypeInfosFromStructObjectInspector(
      StructObjectInspector structObjectInspector) throws HiveException {

    List<? extends StructField> fields = structObjectInspector.getAllStructFieldRefs();
    PrimitiveTypeInfo[] result = new PrimitiveTypeInfo[fields.size()];

    int i = 0;
    for(StructField field : fields) {
      TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(
          field.getFieldObjectInspector().getTypeName());
      result[i++] =  (PrimitiveTypeInfo) typeInfo;
    }
    return result;
  }

  public static PrimitiveTypeInfo[] primitiveTypeInfosFromTypeNames(
      String[] typeNames) throws HiveException {

    PrimitiveTypeInfo[] result = new PrimitiveTypeInfo[typeNames.length];

    for(int i = 0; i < typeNames.length; i++) {
      TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeNames[i]);
      result[i] =  (PrimitiveTypeInfo) typeInfo;
    }
    return result;
  }

  /**
   * Make a new (scratch) batch, which is exactly "like" the batch provided, except that it's empty
   * @param batch the batch to imitate
   * @return the new batch
   * @throws HiveException
   */
  public static VectorizedRowBatch makeLike(VectorizedRowBatch batch) throws HiveException {
    VectorizedRowBatch newBatch = new VectorizedRowBatch(batch.numCols);
    for (int i = 0; i < batch.numCols; i++) {
      ColumnVector colVector = batch.cols[i];
      if (colVector != null) {
        ColumnVector newColVector;
        if (colVector instanceof LongColumnVector) {
          newColVector = new LongColumnVector();
        } else if (colVector instanceof DoubleColumnVector) {
          newColVector = new DoubleColumnVector();
        } else if (colVector instanceof BytesColumnVector) {
          newColVector = new BytesColumnVector();
        } else if (colVector instanceof DecimalColumnVector) {
          DecimalColumnVector decColVector = (DecimalColumnVector) colVector;
          newColVector = new DecimalColumnVector(decColVector.precision, decColVector.scale);
        } else {
          throw new HiveException("Column vector class " + colVector.getClass().getName() +
          " is not supported!");
        }
        newBatch.cols[i] = newColVector;
        newBatch.cols[i].init();
      }
    }
    newBatch.projectedColumns = Arrays.copyOf(batch.projectedColumns, batch.projectedColumns.length);
    newBatch.projectionSize = batch.projectionSize;
    newBatch.reset();
    return newBatch;
  }

  public static String displayBytes(byte[] bytes, int start, int length) {
    StringBuilder sb = new StringBuilder();
    for (int i = start; i < start + length; i++) {
      char ch = (char) bytes[i];
      if (ch < ' ' || ch > '~') {
        sb.append(String.format("\\%03d", (int) (bytes[i] & 0xff)));
      } else {
        sb.append(ch);
      }
    }
    return sb.toString();
  }

  public static void debugDisplayOneRow(VectorizedRowBatch batch, int index, String prefix) {
    StringBuffer sb = new StringBuffer();
    sb.append(prefix + " row " + index + " ");
    for (int column = 0; column < batch.cols.length; column++) {
      ColumnVector colVector = batch.cols[column];
      if (colVector == null) {
        sb.append("(null colVector " + column + ")");
      } else {
        boolean isRepeating = colVector.isRepeating;
        index = (isRepeating ? 0 : index);
        if (colVector.noNulls || !colVector.isNull[index]) {
          if (colVector instanceof LongColumnVector) {
            sb.append(((LongColumnVector) colVector).vector[index]);
          } else if (colVector instanceof DoubleColumnVector) {
            sb.append(((DoubleColumnVector) colVector).vector[index]);
          } else if (colVector instanceof BytesColumnVector) {
            BytesColumnVector bytesColumnVector = (BytesColumnVector) colVector;
            byte[] bytes = bytesColumnVector.vector[index];
            int start = bytesColumnVector.start[index];
            int length = bytesColumnVector.length[index];
            if (bytes == null) {
              sb.append("(Unexpected null bytes with start " + start + " length " + length + ")");
            } else {
              sb.append("bytes: '" + displayBytes(bytes, start, length) + "'");
            }
          } else if (colVector instanceof DecimalColumnVector) {
            sb.append(((DecimalColumnVector) colVector).vector[index].toString());
          } else {
            sb.append("Unknown");
          }
        } else {
          sb.append("NULL");
        }
      }
      sb.append(" ");
    }
    LOG.info(sb.toString());
  }

  public static void debugDisplayBatch(VectorizedRowBatch batch, String prefix) throws HiveException {
    for (int i = 0; i < batch.size; i++) {
      int index = (batch.selectedInUse ? batch.selected[i] : i);
      debugDisplayOneRow(batch, index, prefix);
    }
  }
}
