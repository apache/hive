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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;

/**
 * This class extracts specified VectorizedRowBatch row columns into writables.
 *
 * The caller provides the data types and projection column numbers of a subset of the columns
 * to extract.
 */
public class VectorExtractRow {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(VectorExtractRow.class);

  /*
   * These members have information for extracting a row column objects from VectorizedRowBatch
   * columns.
   */
  int[] projectionColumnNums;
              // Extraction can be a subset of columns, so this is the projection --
              // the batch column numbers.

  TypeInfo[] typeInfos;
  ObjectInspector[] objectInspectors;

  private static final byte[] EMPTY_BYTES = new byte[0];
  private static final String EMPTY_STRING = "";

  /*
   * Allocate the various arrays.
   */
  private void allocateArrays(int count) {
    projectionColumnNums = new int[count];
    typeInfos = new TypeInfo[count];
    objectInspectors = new ObjectInspector[count];
  }

  /*
   * Initialize one column's array entries.
   */
  private void initEntry(int logicalColumnIndex, int projectionColumnNum, TypeInfo typeInfo) {
    projectionColumnNums[logicalColumnIndex] = projectionColumnNum;
    typeInfos[logicalColumnIndex] = typeInfo;
    objectInspectors[logicalColumnIndex] = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
  }

  /*
   * Initialize using an StructObjectInspector and a column projection list.
   */
  public void init(StructObjectInspector structObjectInspector, List<Integer> projectedColumns)
      throws HiveException {
    List<? extends StructField> fields = structObjectInspector.getAllStructFieldRefs();
    final int count = fields.size();
    allocateArrays(count);

    for (int i = 0; i < count; i++) {

      int projectionColumnNum = projectedColumns.get(i);

      StructField field = fields.get(i);
      ObjectInspector fieldInspector = field.getFieldObjectInspector();
      TypeInfo typeInfo =
          TypeInfoUtils.getTypeInfoFromTypeString(fieldInspector.getTypeName());
      initEntry(i, projectionColumnNum, typeInfo);
    }
  }

  /*
   * Initialize using an ObjectInspector array and a column projection array.
   */
  public void init(TypeInfo[] typeInfos, int[] projectedColumns)
      throws HiveException {

    final int count = typeInfos.length;
    allocateArrays(count);

    for (int i = 0; i < count; i++) {
      initEntry(i, projectedColumns[i], typeInfos[i]);
    }
  }

  /*
   * Initialize using data type names.
   * No projection -- the column range 0 .. types.size()-1
   */
  @VisibleForTesting
  void init(List<String> typeNames) throws HiveException {

    final int count = typeNames.size();
    allocateArrays(count);

    for (int i = 0; i < count; i++) {
      initEntry(i, i, TypeInfoUtils.getTypeInfoFromTypeString(typeNames.get(i)));
    }
  }

  public void init(TypeInfo[] typeInfos) throws HiveException {

    final int count = typeInfos.length;
    allocateArrays(count);

    for (int i = 0; i < count; i++) {
      initEntry(i, i, typeInfos[i]);
    }
  }

  public int getCount() {
    return projectionColumnNums.length;
  }

  /**
   * Extract a row's column object from the ColumnVector at batchIndex in the VectorizedRowBatch.
   *
   * @param batch
   * @param batchIndex
   * @param logicalColumnIndex
   * @return
   */
  private Object extractRowColumn(VectorizedRowBatch batch, int batchIndex,
      int logicalColumnIndex) {

    final int projectionColumnNum = projectionColumnNums[logicalColumnIndex];
    final ColumnVector colVector = batch.cols[projectionColumnNum];
    final TypeInfo typeInfo = typeInfos[logicalColumnIndex];
    // try {
      return extractRowColumn(
          colVector, typeInfo, objectInspectors[logicalColumnIndex], batchIndex);
    // } catch (Exception e){
    //   throw new RuntimeException("Error evaluating column number " + projectionColumnNum +
    //       ", typeInfo " + typeInfo.toString() + ", batchIndex " + batchIndex);
    // }
  }

  public Object extractRowColumn(
      ColumnVector colVector, TypeInfo typeInfo, ObjectInspector objectInspector, int batchIndex) {

    if (colVector == null) {
      // The planner will not include unneeded columns for reading but other parts of execution
      // may ask for them..
      return null;
    }
    final int adjustedIndex = (colVector.isRepeating ? 0 : batchIndex);
    if (!colVector.noNulls && colVector.isNull[adjustedIndex]) {
      return null;
    }

    final Category category = typeInfo.getCategory();
    switch (category) {
    case PRIMITIVE:
      {
        final PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
        final PrimitiveCategory primitiveCategory = primitiveTypeInfo.getPrimitiveCategory();
        final Writable primitiveWritable =
            VectorizedBatchUtil.getPrimitiveWritable(primitiveCategory);
        switch (primitiveCategory) {
        case VOID:
          return null;
        case BOOLEAN:
          ((BooleanWritable) primitiveWritable).set(
              ((LongColumnVector) colVector).vector[adjustedIndex] == 0 ?
                  false : true);
          return primitiveWritable;
        case BYTE:
          ((ByteWritable) primitiveWritable).set(
              (byte) ((LongColumnVector) colVector).vector[adjustedIndex]);
          return primitiveWritable;
        case SHORT:
          ((ShortWritable) primitiveWritable).set(
              (short) ((LongColumnVector) colVector).vector[adjustedIndex]);
          return primitiveWritable;
        case INT:
          ((IntWritable) primitiveWritable).set(
              (int) ((LongColumnVector) colVector).vector[adjustedIndex]);
          return primitiveWritable;
        case LONG:
          ((LongWritable) primitiveWritable).set(
              ((LongColumnVector) colVector).vector[adjustedIndex]);
          return primitiveWritable;
        case TIMESTAMP:
          // From java.sql.Timestamp used by vectorization to serializable org.apache.hadoop.hive.common.type.Timestamp
          java.sql.Timestamp ts =
              ((TimestampColumnVector) colVector).asScratchTimestamp(adjustedIndex);
          Timestamp serializableTS = Timestamp.ofEpochMilli(ts.getTime(), ts.getNanos());
          ((TimestampWritableV2) primitiveWritable).set(serializableTS);
          return primitiveWritable;
        case DATE:
          ((DateWritableV2) primitiveWritable).set(
              (int) ((LongColumnVector) colVector).vector[adjustedIndex]);
          return primitiveWritable;
        case FLOAT:
          ((FloatWritable) primitiveWritable).set(
              (float) ((DoubleColumnVector) colVector).vector[adjustedIndex]);
          return primitiveWritable;
        case DOUBLE:
          ((DoubleWritable) primitiveWritable).set(
              ((DoubleColumnVector) colVector).vector[adjustedIndex]);
          return primitiveWritable;
        case BINARY:
          {
            final BytesColumnVector bytesColVector =
                ((BytesColumnVector) colVector);
            final byte[] bytes = bytesColVector.vector[adjustedIndex];
            final int start = bytesColVector.start[adjustedIndex];
            final int length = bytesColVector.length[adjustedIndex];

            BytesWritable bytesWritable = (BytesWritable) primitiveWritable;
            if (bytes == null || length == 0) {
              if (length > 0) {
                nullBytesReadError(primitiveCategory, batchIndex);
              }
              bytesWritable.set(EMPTY_BYTES, 0, 0);
            } else {
              bytesWritable.set(bytes, start, length);
            }
            return primitiveWritable;
          }
        case STRING:
          {
            final BytesColumnVector bytesColVector =
                ((BytesColumnVector) colVector);
            final byte[] bytes = bytesColVector.vector[adjustedIndex];
            final int start = bytesColVector.start[adjustedIndex];
            final int length = bytesColVector.length[adjustedIndex];

            if (bytes == null || length == 0) {
              if (length > 0) {
                nullBytesReadError(primitiveCategory, batchIndex);
              }
              ((Text) primitiveWritable).set(EMPTY_BYTES, 0, 0);
            } else {

              // Use org.apache.hadoop.io.Text as our helper to go from byte[] to String.
              ((Text) primitiveWritable).set(bytes, start, length);
            }
            return primitiveWritable;
          }
        case VARCHAR:
          {
            final BytesColumnVector bytesColVector =
                ((BytesColumnVector) colVector);
            final byte[] bytes = bytesColVector.vector[adjustedIndex];
            final int start = bytesColVector.start[adjustedIndex];
            final int length = bytesColVector.length[adjustedIndex];

            final HiveVarcharWritable hiveVarcharWritable = (HiveVarcharWritable) primitiveWritable;
            if (bytes == null || length == 0) {
              if (length > 0) {
                nullBytesReadError(primitiveCategory, batchIndex);
              }
              hiveVarcharWritable.set(EMPTY_STRING, -1);
            } else {
              final int adjustedLength =
                  StringExpr.truncate(
                      bytes, start, length, ((VarcharTypeInfo) primitiveTypeInfo).getLength());
              if (adjustedLength == 0) {
                hiveVarcharWritable.set(EMPTY_STRING, -1);
              } else {
                hiveVarcharWritable.set(
                    new String(bytes, start, adjustedLength, Charsets.UTF_8), -1);
              }
            }
            return primitiveWritable;
          }
        case CHAR:
          {
            final BytesColumnVector bytesColVector =
                ((BytesColumnVector) colVector);
            final byte[] bytes = bytesColVector.vector[adjustedIndex];
            final int start = bytesColVector.start[adjustedIndex];
            final int length = bytesColVector.length[adjustedIndex];

            final HiveCharWritable hiveCharWritable = (HiveCharWritable) primitiveWritable;
            final int maxLength = ((CharTypeInfo) primitiveTypeInfo).getLength();
            if (bytes == null || length == 0) {
              if (length > 0) {
                nullBytesReadError(primitiveCategory, batchIndex);
              }
              hiveCharWritable.set(EMPTY_STRING, maxLength);
            } else {
              final int adjustedLength = StringExpr.rightTrimAndTruncate(bytes, start, length,
                  ((CharTypeInfo) primitiveTypeInfo).getLength());

              if (adjustedLength == 0) {
                hiveCharWritable.set(EMPTY_STRING, maxLength);
              } else {
                hiveCharWritable.set(
                    new String(bytes, start, adjustedLength, Charsets.UTF_8), maxLength);
              }
            }
            return primitiveWritable;
          }
        case DECIMAL:
          if (colVector instanceof Decimal64ColumnVector) {
            Decimal64ColumnVector dec32ColVector = (Decimal64ColumnVector) colVector;
            ((HiveDecimalWritable) primitiveWritable).deserialize64(
                dec32ColVector.vector[adjustedIndex], dec32ColVector.scale);
          } else {
            // The HiveDecimalWritable set method will quickly copy the deserialized decimal writable fields.
            ((HiveDecimalWritable) primitiveWritable).set(
                ((DecimalColumnVector) colVector).vector[adjustedIndex]);
          }
          return primitiveWritable;
        case INTERVAL_YEAR_MONTH:
          ((HiveIntervalYearMonthWritable) primitiveWritable).set(
              (int) ((LongColumnVector) colVector).vector[adjustedIndex]);
          return primitiveWritable;
        case INTERVAL_DAY_TIME:
          ((HiveIntervalDayTimeWritable) primitiveWritable).set(
              ((IntervalDayTimeColumnVector) colVector).asScratchIntervalDayTime(adjustedIndex));
          return primitiveWritable;
        default:
          throw new RuntimeException("Primitive category " + primitiveCategory.name() +
              " not supported");
        }
      }
    case LIST:
      {
        final ListColumnVector listColumnVector = (ListColumnVector) colVector;
        final ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
        final ListObjectInspector listObjectInspector = (ListObjectInspector) objectInspector;
        final int offset = (int) listColumnVector.offsets[adjustedIndex];
        final int size = (int) listColumnVector.lengths[adjustedIndex];

        final List list = new ArrayList();
        for (int i = 0; i < size; i++) {
          list.add(
              extractRowColumn(
                  listColumnVector.child,
                  listTypeInfo.getListElementTypeInfo(),
                  listObjectInspector.getListElementObjectInspector(),
                  offset + i));
        }
        return list;
      }
    case MAP:
      {
        final MapColumnVector mapColumnVector = (MapColumnVector) colVector;
        final MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
        final MapObjectInspector mapObjectInspector = (MapObjectInspector) objectInspector;
        final int offset = (int) mapColumnVector.offsets[adjustedIndex];
        final int size = (int) mapColumnVector.lengths[adjustedIndex];

        final Map<Object, Object> map = new LinkedHashMap<Object, Object>();
        for (int i = 0; i < size; i++) {
          final Object key = extractRowColumn(
              mapColumnVector.keys,
              mapTypeInfo.getMapKeyTypeInfo(),
              mapObjectInspector.getMapKeyObjectInspector(),
              offset + i);
          final Object value = extractRowColumn(
              mapColumnVector.values,
              mapTypeInfo.getMapValueTypeInfo(),
              mapObjectInspector.getMapValueObjectInspector(),
              offset + i);
          map.put(key, value);
        }
        return map;
      }
    case STRUCT:
      {
        final StructColumnVector structColumnVector = (StructColumnVector) colVector;
        final StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
        final StandardStructObjectInspector structInspector =
            (StandardStructObjectInspector) objectInspector;
        final List<TypeInfo> fieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
        final int size = fieldTypeInfos.size();
        final List<? extends StructField> structFields =
            structInspector.getAllStructFieldRefs();

        final Object struct = structInspector.create();
        for (int i = 0; i < size; i++) {
          final StructField structField = structFields.get(i);
          final TypeInfo fieldTypeInfo = fieldTypeInfos.get(i);
          final Object value = extractRowColumn(
              structColumnVector.fields[i],
              fieldTypeInfo,
              structField.getFieldObjectInspector(),
              adjustedIndex);
          structInspector.setStructFieldData(struct, structField, value);
        }
        return struct;
      }
    case UNION:
      {
        final UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
        final List<TypeInfo> objectTypeInfos = unionTypeInfo.getAllUnionObjectTypeInfos();
        final UnionObjectInspector unionInspector = (UnionObjectInspector) objectInspector;
        final List<ObjectInspector> unionInspectors = unionInspector.getObjectInspectors();
        final UnionColumnVector unionColumnVector = (UnionColumnVector) colVector;
        final byte tag = (byte) unionColumnVector.tags[adjustedIndex];
        final Object object = extractRowColumn(
            unionColumnVector.fields[tag],
            objectTypeInfos.get(tag),
            unionInspectors.get(tag),
            adjustedIndex);

        final StandardUnion standardUnion = new StandardUnion();
        standardUnion.setTag(tag);
        standardUnion.setObject(object);
        return standardUnion;
      }
    default:
      throw new RuntimeException("Category " + category.name() + " not supported");
    }
  }

  /**
   * Extract an row object from a VectorizedRowBatch at batchIndex.
   *
   * @param batch
   * @param batchIndex
   * @param objects
   */
  public void extractRow(VectorizedRowBatch batch, int batchIndex, Object[] objects) {
    for (int i = 0; i < projectionColumnNums.length; i++) {
      objects[i] = extractRowColumn(batch, batchIndex, i);
    }
  }

  private void nullBytesReadError(PrimitiveCategory primitiveCategory, int batchIndex) {
    throw new RuntimeException("null " + primitiveCategory.name() +
        " entry: batchIndex " + batchIndex);
  }
}
