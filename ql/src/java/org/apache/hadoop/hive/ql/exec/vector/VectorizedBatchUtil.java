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

package org.apache.hadoop.hive.ql.exec.vector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toList;

public class VectorizedBatchUtil {
  private static final Logger LOG = LoggerFactory.getLogger(VectorizedBatchUtil.class);

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

  public static ColumnVector createColumnVector(String typeName) {
    return createColumnVector(typeName, DataTypePhysicalVariation.NONE);
  }

  public static ColumnVector createColumnVector(String typeName,
      DataTypePhysicalVariation dataTypePhysicalVariation) {

    typeName = typeName.toLowerCase();

    // Allow undecorated CHAR and VARCHAR to support scratch column type names.
    if (typeName.equals("char") || typeName.equals("varchar")) {
      return new BytesColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
    }

    TypeInfo typeInfo = (TypeInfo) TypeInfoUtils.getTypeInfoFromTypeString(typeName);
    return createColumnVector(typeInfo, dataTypePhysicalVariation);
  }

  public static ColumnVector createColumnVector(TypeInfo typeInfo) {
    return createColumnVector(typeInfo, DataTypePhysicalVariation.NONE);
  }

  public static ColumnVector createColumnVector(TypeInfo typeInfo,
      DataTypePhysicalVariation dataTypePhysicalVariation) {
    switch(typeInfo.getCategory()) {
    case PRIMITIVE:
      {
        PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
        switch(primitiveTypeInfo.getPrimitiveCategory()) {
        case BOOLEAN:
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
        case INTERVAL_YEAR_MONTH:
          return new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
        case DATE:
          return new DateColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
        case TIMESTAMP:
          return new TimestampColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
        case INTERVAL_DAY_TIME:
          return new IntervalDayTimeColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
        case FLOAT:
        case DOUBLE:
          return new DoubleColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
        case BINARY:
        case STRING:
        case CHAR:
        case VARCHAR:
          return new BytesColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
        case DECIMAL:
          DecimalTypeInfo tInfo = (DecimalTypeInfo) primitiveTypeInfo;
          if (dataTypePhysicalVariation == DataTypePhysicalVariation.DECIMAL_64) {
            return new Decimal64ColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
                tInfo.precision(), tInfo.scale());
          } else {
            return new DecimalColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
                tInfo.precision(), tInfo.scale());
          }
        case VOID:
          return new VoidColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
        default:
          throw new RuntimeException("Vectorizaton is not supported for datatype:"
              + primitiveTypeInfo.getPrimitiveCategory());
        }
      }
    case STRUCT:
      {
        StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
        List<TypeInfo> typeInfoList = structTypeInfo.getAllStructFieldTypeInfos();
        ColumnVector[] children = new ColumnVector[typeInfoList.size()];
        for(int i=0; i < children.length; ++i) {
          children[i] =
              createColumnVector(typeInfoList.get(i));
        }
        return new StructColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
            children);
      }
    case UNION:
      {
        UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
        List<TypeInfo> typeInfoList = unionTypeInfo.getAllUnionObjectTypeInfos();
        ColumnVector[] children = new ColumnVector[typeInfoList.size()];
        for(int i=0; i < children.length; ++i) {
          children[i] = createColumnVector(typeInfoList.get(i));
        }
        return new UnionColumnVector(VectorizedRowBatch.DEFAULT_SIZE, children);
      }
    case LIST:
      {
        ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
        return new ListColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
            createColumnVector(listTypeInfo.getListElementTypeInfo()));
      }
    case MAP:
      {
        MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
        return new MapColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
            createColumnVector(mapTypeInfo.getMapKeyTypeInfo()),
            createColumnVector(mapTypeInfo.getMapValueTypeInfo()));
      }
    default:
      throw new RuntimeException("Vectorization is not supported for datatype:"
          + typeInfo.getCategory());
    }
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
        lcv.vector[rowIndex] = ((DateWritableV2) writableCol).getDays();
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
      TimestampColumnVector lcv = (TimestampColumnVector) batch.cols[offset + colIndex];
      if (writableCol != null) {
        lcv.set(rowIndex, ((TimestampWritableV2) writableCol).getTimestamp().toSqlTimestamp());
        lcv.isNull[rowIndex] = false;
      } else {
        lcv.setNullValue(rowIndex);
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
      IntervalDayTimeColumnVector icv = (IntervalDayTimeColumnVector) batch.cols[offset + colIndex];
      if (writableCol != null) {
        HiveIntervalDayTime idt = ((HiveIntervalDayTimeWritable) writableCol).getHiveIntervalDayTime();
        icv.set(rowIndex, idt);
        icv.isNull[rowIndex] = false;
      } else {
        icv.setNullValue(rowIndex);
        setNullColIsNullValue(icv, rowIndex);
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

  public static String[] columnNamesFromStructObjectInspector(
      StructObjectInspector structObjectInspector) throws HiveException {

    List<? extends StructField> fields = structObjectInspector.getAllStructFieldRefs();
    String[] result = new String[fields.size()];

    int i = 0;
    for(StructField field : fields) {
      result[i++] =  field.getFieldName();
    }
    return result;
  }

  public static TypeInfo[] typeInfosFromTypeNames(String[] typeNames) throws HiveException {
    ArrayList<TypeInfo> typeInfoList =
        TypeInfoUtils.typeInfosFromTypeNames(Arrays.asList(typeNames));
    return typeInfoList.toArray(new TypeInfo[0]);
  }

  public static TypeInfo[] typeInfosFromStructObjectInspector(
      StructObjectInspector structObjectInspector) {
    ArrayList<TypeInfo> typeInfoList =
        TypeInfoUtils.typeInfosFromStructObjectInspector(structObjectInspector);
    return typeInfoList.toArray(new TypeInfo[0]);
  }

  public static ColumnVector makeLikeColumnVector(ColumnVector source) throws HiveException{
    if (source instanceof Decimal64ColumnVector) {
      Decimal64ColumnVector dec64ColVector = (Decimal64ColumnVector) source;
      return new Decimal64ColumnVector(dec64ColVector.vector.length,
          dec64ColVector.precision,
          dec64ColVector.scale);
    } else if (source instanceof DateColumnVector) {
      return new DateColumnVector(((DateColumnVector) source).vector.length);
    } else if (source instanceof LongColumnVector) {
      return new LongColumnVector(((LongColumnVector) source).vector.length);
    } else if (source instanceof DoubleColumnVector) {
      return new DoubleColumnVector(((DoubleColumnVector) source).vector.length);
    } else if (source instanceof BytesColumnVector) {
      return new BytesColumnVector(((BytesColumnVector) source).vector.length);
    } else if (source instanceof DecimalColumnVector) {
      DecimalColumnVector decColVector = (DecimalColumnVector) source;
      return new DecimalColumnVector(decColVector.vector.length,
          decColVector.precision,
          decColVector.scale);
    } else if (source instanceof TimestampColumnVector) {
      return new TimestampColumnVector(((TimestampColumnVector) source).getLength());
    } else if (source instanceof IntervalDayTimeColumnVector) {
      return new IntervalDayTimeColumnVector(((IntervalDayTimeColumnVector) source).getLength());
    } else if (source instanceof ListColumnVector) {
      ListColumnVector src = (ListColumnVector) source;
      ColumnVector child = makeLikeColumnVector(src.child);
      return new ListColumnVector(src.offsets.length, child);
    } else if (source instanceof MapColumnVector) {
      MapColumnVector src = (MapColumnVector) source;
      ColumnVector keys = makeLikeColumnVector(src.keys);
      ColumnVector values = makeLikeColumnVector(src.values);
      return new MapColumnVector(src.offsets.length, keys, values);
    } else if (source instanceof StructColumnVector) {
      StructColumnVector src = (StructColumnVector) source;
      ColumnVector[] copy = new ColumnVector[src.fields.length];
      for(int i=0; i < copy.length; ++i) {
        copy[i] = makeLikeColumnVector(src.fields[i]);
      }
      return new StructColumnVector(VectorizedRowBatch.DEFAULT_SIZE, copy);
    } else if (source instanceof UnionColumnVector) {
      UnionColumnVector src = (UnionColumnVector) source;
      ColumnVector[] copy = new ColumnVector[src.fields.length];
      for(int i=0; i < copy.length; ++i) {
        copy[i] = makeLikeColumnVector(src.fields[i]);
      }
      return new UnionColumnVector(src.tags.length, copy);
    } else if (source instanceof VoidColumnVector) {
      return new VoidColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
    } else
      throw new HiveException("Column vector class " +
          source.getClass().getName() +
          " is not supported!");
  }

  private static final byte[] EMPTY_BYTES = new byte[0];
  private static final HiveIntervalDayTime emptyIntervalDayTime = new HiveIntervalDayTime(0, 0);

  public static void copyNonSelectedColumnVector(VectorizedRowBatch sourceBatch,
      int sourceColumnNum, VectorizedRowBatch targetBatch, int targetColumnNum, int size) {
    copyNonSelectedColumnVector(sourceBatch, sourceColumnNum, targetBatch, targetColumnNum, size, 0);
  }

  public static void copyNonSelectedColumnVector(
      VectorizedRowBatch sourceBatch, int sourceColumnNum,
      VectorizedRowBatch targetBatch, int targetColumnNum,
      int size, int startIndex) {

    ColumnVector sourceColVector = sourceBatch.cols[sourceColumnNum];
    ColumnVector targetColVector = targetBatch.cols[targetColumnNum];
    if (sourceColVector.noNulls && targetColVector.noNulls) {
      // No isNull copying necessary.
    } else if (sourceColVector.noNulls) {

      // Clear out isNull array.
      targetColVector.reset();
    } else {
      System.arraycopy(sourceColVector.isNull, 0, targetColVector.isNull, 0, size);
      targetColVector.noNulls = false;
    }
    if (sourceColVector.isRepeating) {
      size = 1;
      startIndex = 0; // we must copy the first item in case of isRepeating
      targetColVector.isRepeating = true;
    } else {
      targetColVector.isRepeating = false;
    }

    // Primitive column types ignore nulls and just copy all values.
    switch (sourceColVector.type) {
    case LONG:
      {
        long[] sourceVector = ((LongColumnVector) sourceColVector).vector;
        long[] targetVector = ((LongColumnVector) targetColVector).vector;
        System.arraycopy(sourceVector, startIndex, targetVector, 0, size);
      }
      break;
    case DOUBLE:
      {
        double[] sourceVector = ((DoubleColumnVector) sourceColVector).vector;
        double[] targetVector = ((DoubleColumnVector) targetColVector).vector;
        System.arraycopy(sourceVector, startIndex, targetVector, 0, size);
      }
      break;
    case BYTES:
      {
        BytesColumnVector sourceBytesColVector = ((BytesColumnVector) sourceColVector);
        byte[][] sourceVector = sourceBytesColVector.vector;
        int[] sourceStart = sourceBytesColVector.start;
        int[] sourceLength = sourceBytesColVector.length;
  
        BytesColumnVector targetBytesColVector = ((BytesColumnVector) targetColVector);
  
        if (sourceColVector.noNulls) {
          for (int i = startIndex; i < size; i++) {
            targetBytesColVector.setVal(i, sourceVector[i], sourceStart[i], sourceLength[i]);
          }
        } else {
          boolean[] sourceIsNull = sourceColVector.isNull;
  
          // Target isNull was copied at beginning of method.
          for (int i = startIndex; i < size; i++) {
            if (!sourceIsNull[i]) {
              targetBytesColVector.setVal(i, sourceVector[i], sourceStart[i], sourceLength[i]);
            } else {
              targetBytesColVector.setRef(i, EMPTY_BYTES, 0, 0);
            }
          }
        }
      }
      break;
    case DECIMAL:
      {
        DecimalColumnVector sourceDecimalColVector = ((DecimalColumnVector) sourceColVector);
        HiveDecimalWritable[] sourceVector = sourceDecimalColVector.vector;

        DecimalColumnVector targetDecimalColVector = ((DecimalColumnVector) targetColVector);

        if (sourceColVector.noNulls) {
          for (int i = startIndex; i < size; i++) {
            targetDecimalColVector.set(i, sourceVector[i]);
          }
        } else {
          boolean[] sourceIsNull = sourceColVector.isNull;

          // Target isNull was copied at beginning of method.
          for (int i = startIndex; i < size; i++) {
            if (!sourceIsNull[i]) {
              targetDecimalColVector.set(i, sourceVector[i]);
            } else {
              targetDecimalColVector.vector[i].setFromLong(0);
            }
          }
        }
      }
      break;
    case TIMESTAMP:
      {
        TimestampColumnVector sourceTimestampColVector = ((TimestampColumnVector) sourceColVector);
        long[] sourceTime = sourceTimestampColVector.time;
        int[] sourceNanos = sourceTimestampColVector.nanos;

        TimestampColumnVector targetTimestampColVector = ((TimestampColumnVector) targetColVector);
        long[] targetTime = targetTimestampColVector.time;
        int[] targetNanos = targetTimestampColVector.nanos;

        if (sourceColVector.noNulls) {
          for (int i = startIndex; i < size; i++) {
            targetTime[i] = sourceTime[i];
            targetNanos[i] = sourceNanos[i];
          }
        } else {
          boolean[] sourceIsNull = sourceColVector.isNull;
  
          // Target isNull was copied at beginning of method.
          for (int i = startIndex; i < size; i++) {
            if (!sourceIsNull[i]) {
              targetTime[i] = sourceTime[i];
              targetNanos[i] = sourceNanos[i];
            } else {
              targetTime[i] = 0;
              targetNanos[i] = 0;
            }
          }
        }
      }
      break;
    case INTERVAL_DAY_TIME:
      {
        IntervalDayTimeColumnVector sourceIntervalDayTimeColVector = ((IntervalDayTimeColumnVector) sourceColVector);

        IntervalDayTimeColumnVector targetIntervalDayTimeColVector = ((IntervalDayTimeColumnVector) targetColVector);

        if (sourceColVector.noNulls) {
          for (int i = startIndex; i < size; i++) {
            targetIntervalDayTimeColVector.set(
                i, targetIntervalDayTimeColVector.asScratchIntervalDayTime(i));
          }
        } else {
          boolean[] sourceIsNull = sourceColVector.isNull;

          // Target isNull was copied at beginning of method.
          for (int i = startIndex; i < size; i++) {
            if (!sourceIsNull[i]) {
              targetIntervalDayTimeColVector.set(
                  i, targetIntervalDayTimeColVector.asScratchIntervalDayTime(i));
            } else {
              targetIntervalDayTimeColVector.set(
                  i, emptyIntervalDayTime);
            }
          }
        }
      }
      break;
    case STRUCT:
    case LIST:
    case MAP:
    case UNION:
      throw new RuntimeException("No complex type support: " + sourceColVector.type);
    case VOID:
      break;
    default:
      throw new RuntimeException("Unexpected column vector type " + sourceColVector.type);
    }
 
  }

  public static void copyRepeatingColumn(VectorizedRowBatch sourceBatch, int sourceColumnNum,
      VectorizedRowBatch targetBatch, int targetColumnNum, boolean setByValue) {
    ColumnVector sourceColVector = sourceBatch.cols[sourceColumnNum];
    ColumnVector targetColVector = targetBatch.cols[targetColumnNum];

    targetColVector.isRepeating = true;

    if (!sourceColVector.noNulls) {
      targetColVector.noNulls = false;
      targetColVector.isNull[0] = true;
      return;
    }

    switch (sourceColVector.type) {
    case LONG:
      ((LongColumnVector) targetColVector).vector[0] =
          ((LongColumnVector) sourceColVector).vector[0];
      break;
    case DOUBLE:
      ((DoubleColumnVector) targetColVector).vector[0] =
          ((DoubleColumnVector) sourceColVector).vector[0];
      break;
    case BYTES:
      {
        BytesColumnVector bytesColVector = (BytesColumnVector) sourceColVector;
        byte[] bytes = bytesColVector.vector[0];
        final int start = bytesColVector.start[0];
        final int length = bytesColVector.length[0];
        if (setByValue) {
          ((BytesColumnVector) targetColVector).setVal(0, bytes, start, length);
        } else {
          ((BytesColumnVector) targetColVector).setRef(0, bytes, start, length);
        }
      }
      break;
    case DECIMAL:
      ((DecimalColumnVector) targetColVector).set(
          0, ((DecimalColumnVector) sourceColVector).vector[0]);
      break;
    case TIMESTAMP:
      ((TimestampColumnVector) targetColVector).set(
          0, ((TimestampColumnVector) sourceColVector).asScratchTimestamp(0));
      break;
    case INTERVAL_DAY_TIME:
      ((IntervalDayTimeColumnVector) targetColVector).set(
          0, ((IntervalDayTimeColumnVector) sourceColVector).asScratchIntervalDayTime(0));
      break;
    case STRUCT:
    case LIST:
    case MAP:
    case UNION:
      // No complex type support for now.
    default:
      throw new RuntimeException("Unexpected column vector type " + sourceColVector.type);
    }
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
      if (batch.cols[i] != null) {
        newBatch.cols[i] = makeLikeColumnVector(batch.cols[i]);
        newBatch.cols[i].init();
      }
    }
    newBatch.projectedColumns = Arrays.copyOf(batch.projectedColumns,
        batch.projectedColumns.length);
    newBatch.projectionSize = batch.projectionSize;
    newBatch.reset();
    return newBatch;
  }

  public static Writable getPrimitiveWritable(TypeInfo typeInfo) {
    return getPrimitiveWritable(((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory());
  }

  public static Writable getPrimitiveWritable(PrimitiveCategory primitiveCategory) {
    switch (primitiveCategory) {
    case VOID:
      return null;
    case BOOLEAN:
      return new BooleanWritable(false);
    case BYTE:
      return new ByteWritable((byte) 0);
    case SHORT:
      return new ShortWritable((short) 0);
    case INT:
      return new IntWritable(0);
    case LONG:
      return new LongWritable(0);
    case TIMESTAMP:
      return new TimestampWritableV2(new Timestamp());
    case DATE:
      return new DateWritableV2(new Date());
    case FLOAT:
      return new FloatWritable(0);
    case DOUBLE:
      return new DoubleWritable(0);
    case BINARY:
      return new BytesWritable(ArrayUtils.EMPTY_BYTE_ARRAY);
    case STRING:
      return new Text(ArrayUtils.EMPTY_BYTE_ARRAY);
    case VARCHAR:
      return new HiveVarcharWritable(new HiveVarchar(StringUtils.EMPTY, -1));
    case CHAR:
      return new HiveCharWritable(new HiveChar(StringUtils.EMPTY, -1));
    case DECIMAL:
      return new HiveDecimalWritable();
    case INTERVAL_YEAR_MONTH:
      return new HiveIntervalYearMonthWritable();
    case INTERVAL_DAY_TIME:
      return new HiveIntervalDayTimeWritable();
    default:
      throw new RuntimeException("Primitive category " + primitiveCategory.name() + " not supported");
    }
  }

  public static String displayBytes(byte[] bytes, int start, int length) {
    StringBuilder sb = new StringBuilder();
    for (int i = start; i < start + length; i++) {
      char ch = (char) bytes[i];
      if (ch < ' ' || ch > '~') {
        sb.append(String.format("\\%03d", bytes[i] & 0xff));
      } else {
        sb.append(ch);
      }
    }
    return sb.toString();
  }

  public static void debugDisplayOneRow(VectorizedRowBatch batch, int index, String prefix) {
    StringBuilder sb = new StringBuilder();
    LOG.info(debugFormatOneRow(batch, index, prefix, sb).toString());
  }

  public static StringBuilder debugFormatOneRow(VectorizedRowBatch batch,
      int index, String prefix, StringBuilder sb) {
    sb.append(prefix + " row " + index + " ");
    for (int p = 0; p < batch.projectionSize; p++) {
      int column = batch.projectedColumns[p];
      sb.append("(" + p + "," + column + ") ");
      ColumnVector colVector = batch.cols[column];
      if (colVector == null) {
        sb.append("(null ColumnVector)");
      } else {
        boolean isRepeating = colVector.isRepeating;
        if (isRepeating) {
          sb.append("(repeating)");
        }
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
          } else if (colVector instanceof TimestampColumnVector) {
            java.sql.Timestamp timestamp = new java.sql.Timestamp(0);
            ((TimestampColumnVector) colVector).timestampUpdate(timestamp, index);
            sb.append(Timestamp.ofEpochMilli(timestamp.getTime(), timestamp.getNanos()).toString());
          } else if (colVector instanceof IntervalDayTimeColumnVector) {
            HiveIntervalDayTime intervalDayTime = ((IntervalDayTimeColumnVector) colVector).asScratchIntervalDayTime(index);
            sb.append(intervalDayTime.toString());
          } else {
            sb.append("Unknown");
          }
        } else {
          sb.append("NULL");
        }
      }
      sb.append(" ");
    }
    return sb;
  }

  public static void debugDisplayBatch(VectorizedRowBatch batch, String prefix) {
    for (int i = 0; i < batch.size; i++) {
      int index = (batch.selectedInUse ? batch.selected[i] : i);
      debugDisplayOneRow(batch, index, prefix);
    }
  }

  /**
   * Reset all columns other than Partition columns
   * @param rowBatch VectorizedRowBatch to reset
   */
  public static void resetNonPartitionColumns(VectorizedRowBatch rowBatch) {
    List<Integer> columnsToReset = IntStream.concat(
            IntStream.range(0, rowBatch.getDataColumnCount()),
            IntStream.range(rowBatch.getDataColumnCount() + rowBatch.getPartitionColumnCount(), rowBatch.cols.length)
    ).boxed().collect(toList());
    rowBatch.reset(columnsToReset);
  }
}
