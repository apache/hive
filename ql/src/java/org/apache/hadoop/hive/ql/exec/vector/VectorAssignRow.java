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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.VectorPartitionConversion;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
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
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
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

import com.google.common.base.Preconditions;


/**
 * This class assigns specified columns of a row from a Writable row objects.
 *
 * The caller provides the data types and projection column numbers of a subset of the columns
 * to assign.
 */
public class VectorAssignRow {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(VectorAssignRow.class);

  /*
   * These members have information for assigning a row column objects into the VectorizedRowBatch
   * columns.
   *
   * We say "target" because when there is conversion the data type being converted is the source.
   */
  boolean[] isConvert;
                // For each column, are we converting the row column object?

  int[] projectionColumnNums;
                // Assigning can be a subset of columns, so this is the projection --
                // the batch column numbers.

  TypeInfo[] targetTypeInfos;
                // The type info of each column being assigned.

  int[] maxLengths;
                // For the CHAR and VARCHAR data types, the maximum character length of
                // the columns.  Otherwise, 0.

  /*
   * These members have information for data type conversion.
   * Not defined if there is no conversion.
   */
  ObjectInspector[] convertSourceOI;
                // The primitive object inspector of the source data type for any column being
                // converted.  Otherwise, null.

  Writable[] convertTargetWritables;
                // Conversion to the target data type requires a "helper" target writable in a
                // few cases.

  /*
   * Allocate the target related arrays.
   */
  private void allocateArrays(int count) {
    isConvert = new boolean[count];
    projectionColumnNums = new int[count];
    targetTypeInfos = new TypeInfo[count];
    maxLengths = new int[count];
  }

  /*
   * Allocate the source conversion related arrays (optional).
   */
  private void allocateConvertArrays(int count) {
    convertSourceOI = new ObjectInspector[count];
    convertTargetWritables = new Writable[count];
  }

  /*
   * Initialize one column's target related arrays.
   */
  private void initTargetEntry(int logicalColumnIndex, int projectionColumnNum, TypeInfo typeInfo) {
    isConvert[logicalColumnIndex] = false;
    projectionColumnNums[logicalColumnIndex] = projectionColumnNum;
    targetTypeInfos[logicalColumnIndex] = typeInfo;
    if (typeInfo.getCategory() == Category.PRIMITIVE) {
      final PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
      final PrimitiveCategory primitiveCategory = primitiveTypeInfo.getPrimitiveCategory();
      switch (primitiveCategory) {
      case CHAR:
        maxLengths[logicalColumnIndex] = ((CharTypeInfo) primitiveTypeInfo).getLength();
        break;
      case VARCHAR:
        maxLengths[logicalColumnIndex] = ((VarcharTypeInfo) primitiveTypeInfo).getLength();
        break;
      default:
        // No additional data type specific setting.
        break;
      }
    }
  }

  /*
   * Initialize one column's source conversion related arrays.
   * Assumes initTargetEntry has already been called.
   */
  private void initConvertSourceEntry(int logicalColumnIndex, TypeInfo convertSourceTypeInfo) {
    isConvert[logicalColumnIndex] = true;
    final Category convertSourceCategory = convertSourceTypeInfo.getCategory();
    convertSourceOI[logicalColumnIndex] =
        TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(convertSourceTypeInfo);

    if (convertSourceCategory == Category.PRIMITIVE) {
      // These need to be based on the target.
      final PrimitiveCategory targetPrimitiveCategory =
          ((PrimitiveTypeInfo) targetTypeInfos[logicalColumnIndex]).getPrimitiveCategory();
      switch (targetPrimitiveCategory) {
      case DATE:
        convertTargetWritables[logicalColumnIndex] = new DateWritableV2();
        break;
      case STRING:
        convertTargetWritables[logicalColumnIndex] = new Text();
        break;
      default:
        // No additional data type specific setting.
        break;
      }
    }
  }

  /*
   * Initialize using an StructObjectInspector and a column projection list.
   */
  public void init(StructObjectInspector structObjectInspector, List<Integer> projectedColumns)
      throws HiveException {

    final List<? extends StructField> fields = structObjectInspector.getAllStructFieldRefs();
    final int count = fields.size();
    allocateArrays(count);

    for (int i = 0; i < count; i++) {

      final int projectionColumnNum = projectedColumns.get(i);

      final StructField field = fields.get(i);
      final ObjectInspector fieldInspector = field.getFieldObjectInspector();
      final TypeInfo typeInfo =
          TypeInfoUtils.getTypeInfoFromTypeString(fieldInspector.getTypeName());

      initTargetEntry(i, projectionColumnNum, typeInfo);
    }
  }

  /*
   * Initialize using an StructObjectInspector.
   * No projection -- the column range 0 .. fields.size()-1
   */
  public void init(StructObjectInspector structObjectInspector) throws HiveException {

    final List<? extends StructField> fields = structObjectInspector.getAllStructFieldRefs();
    final int count = fields.size();
    allocateArrays(count);

    for (int i = 0; i < count; i++) {

      final StructField field = fields.get(i);
      final ObjectInspector fieldInspector = field.getFieldObjectInspector();
      final TypeInfo typeInfo =
          TypeInfoUtils.getTypeInfoFromTypeString(fieldInspector.getTypeName());

      initTargetEntry(i, i, typeInfo);
    }
  }

  /*
   * Initialize using target data type names.
   * No projection -- the column range 0 .. types.size()-1
   */
  public void init(List<String> typeNames) throws HiveException {

    final int count = typeNames.size();
    allocateArrays(count);

    for (int i = 0; i < count; i++) {

      final TypeInfo typeInfo =
          TypeInfoUtils.getTypeInfoFromTypeString(typeNames.get(i));

      initTargetEntry(i, i, typeInfo);
    }
  }

  /*
   * Initialize using one target data type info.
   */
  public void init(TypeInfo typeInfo, int outputColumnNum) throws HiveException {

    allocateArrays(1);
    initTargetEntry(0, outputColumnNum, typeInfo);
  }

  /**
   * Initialize for conversion from a provided (source) data types to the target data types
   * desired in the VectorizedRowBatch.
   *
   * No projection -- the column range 0 .. count-1
   *
   *    where count is the minimum of the target data type array size, included array size,
   *       and source data type array size.
   *
   * @param sourceTypeInfos
   * @param targetTypeInfos
   * @param columnsToIncludeTruncated
   *                Flag array indicating which columns are to be included.
   *                "Truncated" because all false entries on the end of the array have been
   *                eliminated.
   * @return the minimum count described above is returned.  That is, the number of columns
   *         that will be processed by assign.
   */
  public int initConversion(TypeInfo[] sourceTypeInfos, TypeInfo[] targetTypeInfos,
      boolean[] columnsToIncludeTruncated) {

    final int targetColumnCount;
    if (columnsToIncludeTruncated == null) {
      targetColumnCount = targetTypeInfos.length;
    } else {
      targetColumnCount = Math.min(targetTypeInfos.length, columnsToIncludeTruncated.length);
    }

    final int sourceColumnCount = Math.min(sourceTypeInfos.length, targetColumnCount);

    allocateArrays(sourceColumnCount);
    allocateConvertArrays(sourceColumnCount);

    for (int i = 0; i < sourceColumnCount; i++) {

      if (columnsToIncludeTruncated != null && !columnsToIncludeTruncated[i]) {

        // Field not included in query.

      } else {
        final TypeInfo targetTypeInfo = targetTypeInfos[i];
        final TypeInfo sourceTypeInfo = sourceTypeInfos[i];

        if (!sourceTypeInfo.equals(targetTypeInfo)) {

          if (VectorPartitionConversion.isImplicitVectorColumnConversion(
              sourceTypeInfo, targetTypeInfo)) {

            // Do implicit conversion accepting the source type and putting it in the same
            // target type ColumnVector type.
            initTargetEntry(i, i, sourceTypeInfo);

          } else {

            // Do formal conversion...
            initTargetEntry(i, i, targetTypeInfo);
            initConvertSourceEntry(i, sourceTypeInfo);

          }
        } else {

          // No conversion.
          initTargetEntry(i, i, targetTypeInfo);

        }
      }
    }

    return sourceColumnCount;
  }

  /**
   * Assign a row's column object to the ColumnVector at batchIndex in the VectorizedRowBatch.
   *
   * @param batch
   * @param batchIndex
   * @param logicalColumnIndex
   * @param object    The row column object whose type is the target data type.
   */
  public void assignRowColumn(
      VectorizedRowBatch batch, int batchIndex, int logicalColumnIndex, Object object) {

    final int projectionColumnNum = projectionColumnNums[logicalColumnIndex];
    final TypeInfo targetTypeInfo = targetTypeInfos[logicalColumnIndex];
    if (targetTypeInfo == null || targetTypeInfo.getCategory() == null) {
      /*
       * This is a column that we don't want (i.e. not included) -- we are done.
       */
      return;
    }
    assignRowColumn(batch.cols[projectionColumnNum], batchIndex, targetTypeInfo, object);
  }

  private void assignRowColumn(
      ColumnVector columnVector, int batchIndex, TypeInfo targetTypeInfo, Object object) {

    if (object == null) {
      assignNullRowColumn(columnVector, batchIndex, targetTypeInfo);
      return;
    }
    switch (targetTypeInfo.getCategory()) {
    case PRIMITIVE:
      {
        final PrimitiveCategory targetPrimitiveCategory =
            ((PrimitiveTypeInfo) targetTypeInfo).getPrimitiveCategory();
        switch (targetPrimitiveCategory) {
        case VOID:
          VectorizedBatchUtil.setNullColIsNullValue(columnVector, batchIndex);
          return;
        case BOOLEAN:
          if (object instanceof Boolean) {
            ((LongColumnVector) columnVector).vector[batchIndex] =
                (((Boolean) object) ? 1 : 0);
          } else {
            ((LongColumnVector) columnVector).vector[batchIndex] =
                (((BooleanWritable) object).get() ? 1 : 0);
          }
          break;
        case BYTE:
          if (object instanceof Byte) {
            ((LongColumnVector) columnVector).vector[batchIndex] =
                ((Byte) object);
          } else {
            ((LongColumnVector) columnVector).vector[batchIndex] =
               ((ByteWritable) object).get();
          }
          break;
        case SHORT:
          if (object instanceof Short) {
            ((LongColumnVector) columnVector).vector[batchIndex] =
                ((Short) object);
          } else {
            ((LongColumnVector) columnVector).vector[batchIndex] =
                ((ShortWritable) object).get();
          }
          break;
        case INT:
          if (object instanceof Integer) {
            ((LongColumnVector) columnVector).vector[batchIndex] =
                ((Integer) object);
          } else {
            ((LongColumnVector) columnVector).vector[batchIndex] =
                ((IntWritable) object).get();
          }
          break;
        case LONG:
          if (object instanceof Long) {
            ((LongColumnVector) columnVector).vector[batchIndex] =
                ((Long) object);
          } else {
            ((LongColumnVector) columnVector).vector[batchIndex] =
                ((LongWritable) object).get();
          }
          break;
        case TIMESTAMP:
          if (object instanceof Timestamp) {
            ((TimestampColumnVector) columnVector).set(
                batchIndex, ((Timestamp) object).toSqlTimestamp());
          } else {
            ((TimestampColumnVector) columnVector).set(
                batchIndex, ((TimestampWritableV2) object).getTimestamp().toSqlTimestamp());
          }
          break;
        case DATE:
          if (object instanceof Date) {
            ((LongColumnVector) columnVector).vector[batchIndex] =
                DateWritableV2.dateToDays((Date) object);
          } else {
            ((LongColumnVector) columnVector).vector[batchIndex] =
               ((DateWritableV2) object).getDays();
          }
          break;
        case FLOAT:
          if (object instanceof Float) {
            ((DoubleColumnVector) columnVector).vector[batchIndex] =
                ((Float) object);
          } else {
            ((DoubleColumnVector) columnVector).vector[batchIndex] =
                ((FloatWritable) object).get();
          }
          break;
        case DOUBLE:
          if (object instanceof Double) {
            ((DoubleColumnVector) columnVector).vector[batchIndex] =
                ((Double) object);
          } else {
            ((DoubleColumnVector) columnVector).vector[batchIndex] =
                ((DoubleWritable) object).get();
          }
          break;
        case BINARY:
          {
            if (object instanceof byte[]) {
              byte[] bytes = (byte[]) object;
              ((BytesColumnVector) columnVector).setVal(
                  batchIndex, bytes, 0, bytes.length);
            } else {
              BytesWritable bw = (BytesWritable) object;
              ((BytesColumnVector) columnVector).setVal(
                  batchIndex, bw.getBytes(), 0, bw.getLength());
            }
          }
          break;
        case STRING:
          {
            if (object instanceof String) {
              String string = (String) object;
              byte[] bytes = string.getBytes();
              ((BytesColumnVector) columnVector).setVal(
                  batchIndex, bytes, 0, bytes.length);
            } else {
              Text tw = (Text) object;
              ((BytesColumnVector) columnVector).setVal(
                  batchIndex, tw.getBytes(), 0, tw.getLength());
            }
          }
          break;
        case VARCHAR:
          {
            // UNDONE: Performance problem with conversion to String, then bytes...

            // We store VARCHAR type stripped of pads.
            HiveVarchar hiveVarchar;
            if (object instanceof HiveVarchar) {
              hiveVarchar = (HiveVarchar) object;
            } else {
              hiveVarchar = ((HiveVarcharWritable) object).getHiveVarchar();
            }

            // TODO: HIVE-13624 Do we need maxLength checking?

            byte[] bytes = hiveVarchar.getValue().getBytes();
            ((BytesColumnVector) columnVector).setVal(
                batchIndex, bytes, 0, bytes.length);
          }
          break;
        case CHAR:
          {
            // UNDONE: Performance problem with conversion to String, then bytes...

            // We store CHAR type stripped of pads.
            HiveChar hiveChar;
            if (object instanceof HiveChar) {
              hiveChar = (HiveChar) object;
            } else {
              hiveChar = ((HiveCharWritable) object).getHiveChar();
            }

            // TODO: HIVE-13624 Do we need maxLength checking?

            // We store CHAR in vector row batch with padding stripped.
            byte[] bytes = hiveChar.getStrippedValue().getBytes();
            ((BytesColumnVector) columnVector).setVal(
                batchIndex, bytes, 0, bytes.length);
          }
          break;
        case DECIMAL:
          if (columnVector instanceof DecimalColumnVector) {
            if (object instanceof HiveDecimal) {
              ((DecimalColumnVector) columnVector).set(
                  batchIndex, (HiveDecimal) object);
            } else {
              ((DecimalColumnVector) columnVector).set(
                  batchIndex, (HiveDecimalWritable) object);
            }
          } else {
            if (object instanceof HiveDecimal) {
              ((Decimal64ColumnVector) columnVector).set(
                  batchIndex, (HiveDecimal) object);
            } else {
              ((Decimal64ColumnVector) columnVector).set(
                  batchIndex, (HiveDecimalWritable) object);
            }
          }
          break;
        case INTERVAL_YEAR_MONTH:
          if (object instanceof HiveIntervalYearMonth) {
            ((LongColumnVector) columnVector).vector[batchIndex] =
                ((HiveIntervalYearMonth) object).getTotalMonths();
          } else {
            ((LongColumnVector) columnVector).vector[batchIndex] =
                ((HiveIntervalYearMonthWritable) object).getHiveIntervalYearMonth().getTotalMonths();
          }
          break;
        case INTERVAL_DAY_TIME:
          if (object instanceof HiveIntervalDayTime) {
            ((IntervalDayTimeColumnVector) columnVector).set(
                batchIndex, (HiveIntervalDayTime) object);
          } else {
            ((IntervalDayTimeColumnVector) columnVector).set(
                batchIndex, ((HiveIntervalDayTimeWritable) object).getHiveIntervalDayTime());
          }
          break;
        default:
          throw new RuntimeException("Primitive category " + targetPrimitiveCategory.name() +
              " not supported");
        }
      }
      break;
    case LIST:
      {
        final ListColumnVector listColumnVector = (ListColumnVector) columnVector;
        final ListTypeInfo listTypeInfo = (ListTypeInfo) targetTypeInfo;
        final TypeInfo elementTypeInfo = listTypeInfo.getListElementTypeInfo();
        final List list = (List) object;
        final int size = list.size();
        final int childCount = listColumnVector.childCount;
        listColumnVector.offsets[batchIndex] = childCount;
        listColumnVector.lengths[batchIndex] = size;
        listColumnVector.childCount = childCount + size;
        listColumnVector.child.ensureSize(childCount + size, true);

        for (int i = 0; i < size; i++) {
          assignRowColumn(listColumnVector.child, childCount + i, elementTypeInfo, list.get(i));
        }
      }
      break;
    case MAP:
      {
        final MapColumnVector mapColumnVector = (MapColumnVector) columnVector;
        final MapTypeInfo mapTypeInfo = (MapTypeInfo) targetTypeInfo;
        final Map<Object, Object> map = (Map<Object, Object>) object;
        final int size = map.size();
        int childCount = mapColumnVector.childCount;
        mapColumnVector.offsets[batchIndex] = childCount;
        mapColumnVector.lengths[batchIndex] = size;
        mapColumnVector.keys.ensureSize(childCount + size, true);
        mapColumnVector.values.ensureSize(childCount + size, true);

        for (Map.Entry<Object, Object> entry : map.entrySet()) {
          assignRowColumn(mapColumnVector.keys, childCount, mapTypeInfo.getMapKeyTypeInfo(), entry.getKey());
          assignRowColumn(mapColumnVector.values, childCount, mapTypeInfo.getMapValueTypeInfo(), entry.getValue());
          childCount++;
        }
        mapColumnVector.childCount = childCount;
      }
      break;
    case STRUCT:
      {
        final StructColumnVector structColumnVector = (StructColumnVector) columnVector;
        final StructTypeInfo targetStructTypeInfo = (StructTypeInfo) targetTypeInfo;
        final List<TypeInfo> targetFieldTypeInfos = targetStructTypeInfo.getAllStructFieldTypeInfos();
        final int size = targetFieldTypeInfos.size();
        if (object instanceof List) {
          final List struct = (List) object;
          for (int i = 0; i < size; i++) {
            assignRowColumn(structColumnVector.fields[i], batchIndex, targetFieldTypeInfos.get(i), struct.get(i));
          }
        } else {
          final Object[] array = (Object[]) object;
          for (int i = 0; i < size; i++) {
            assignRowColumn(structColumnVector.fields[i], batchIndex, targetFieldTypeInfos.get(i), array[i]);
          }
        }
      }
      break;
    case UNION:
      {
        final StandardUnion union = (StandardUnion) object;
        final UnionColumnVector unionColumnVector = (UnionColumnVector) columnVector;
        final UnionTypeInfo unionTypeInfo = (UnionTypeInfo) targetTypeInfo;
        final List<TypeInfo> objectTypeInfos = unionTypeInfo.getAllUnionObjectTypeInfos();
        final byte tag = union.getTag();
        unionColumnVector.tags[batchIndex] = tag;
        assignRowColumn(unionColumnVector.fields[tag], batchIndex, objectTypeInfos.get(tag), union.getObject());
      }
      break;
    default:
      throw new RuntimeException("Category " + targetTypeInfo.getCategory().name() + " not supported");
    }

    /*
     * We always set the null flag to false when there is a value.
     */
    columnVector.isNull[batchIndex] = false;
  }

  private void assignNullRowColumn(
    ColumnVector columnVector, int batchIndex, TypeInfo targetTypeInfo) {

    VectorizedBatchUtil.setNullColIsNullValue(columnVector, batchIndex);     
    
    if (targetTypeInfo.getCategory() == Category.STRUCT) {
      final StructColumnVector structColumnVector = (StructColumnVector) columnVector;
      final StructTypeInfo targetStructTypeInfo = (StructTypeInfo) targetTypeInfo;
      final List<TypeInfo> targetFieldTypeInfos = targetStructTypeInfo.getAllStructFieldTypeInfos();
      final int size = targetFieldTypeInfos.size();
      
      for (int i = 0; i < size; i++) {
        assignRowColumn(structColumnVector.fields[i], batchIndex, targetFieldTypeInfos.get(i), null);
      }
    }
  }

    /**
   * Convert row's column object and then assign it the ColumnVector at batchIndex
   * in the VectorizedRowBatch.
   *
   * Public so VectorDeserializeRow can use this method to convert a row's column object.
   *
   * @param batch
   * @param batchIndex
   * @param logicalColumnIndex
   * @param object    The row column object whose type is the VectorAssignRow.initConversion
   *                  source data type.
   *
   */
  public void assignConvertRowColumn(VectorizedRowBatch batch, int batchIndex,
      int logicalColumnIndex, Object object) {
    Preconditions.checkState(isConvert[logicalColumnIndex]);
    final int projectionColumnNum = projectionColumnNums[logicalColumnIndex];
    assignConvertRowColumn(
        batch.cols[projectionColumnNum],
        batchIndex,
        targetTypeInfos[logicalColumnIndex],
        convertSourceOI[logicalColumnIndex],
        convertTargetWritables[logicalColumnIndex],
        object);
  }

  private void assignConvertRowColumn(ColumnVector columnVector, int batchIndex,
      TypeInfo targetTypeInfo, ObjectInspector sourceObjectInspector,
      Writable convertTargetWritable, Object object) {

    final Category targetCategory = targetTypeInfo.getCategory();
    if (targetCategory == null) {
      /*
       * This is a column that we don't want (i.e. not included) -- we are done.
       */
      return;
    }
    if (object == null) {
      VectorizedBatchUtil.setNullColIsNullValue(columnVector, batchIndex);
      return;
    }
    try {
      switch (targetCategory) {
      case PRIMITIVE:
        final PrimitiveObjectInspector sourcePrimitiveOI =
            (PrimitiveObjectInspector) sourceObjectInspector;
        final PrimitiveCategory targetPrimitiveCategory =
            ((PrimitiveTypeInfo) targetTypeInfo).getPrimitiveCategory();
        switch (targetPrimitiveCategory) {
        case VOID:
          VectorizedBatchUtil.setNullColIsNullValue(columnVector, batchIndex);
          return;
        case BOOLEAN:
          ((LongColumnVector) columnVector).vector[batchIndex] =
              (PrimitiveObjectInspectorUtils.getBoolean(
                  object, sourcePrimitiveOI) ? 1 : 0);
          break;
        case BYTE:
          ((LongColumnVector) columnVector).vector[batchIndex] =
              PrimitiveObjectInspectorUtils.getByte(
                  object, sourcePrimitiveOI);
          break;
        case SHORT:
          ((LongColumnVector) columnVector).vector[batchIndex] =
              PrimitiveObjectInspectorUtils.getShort(
                  object, sourcePrimitiveOI);
          break;
        case INT:
          ((LongColumnVector) columnVector).vector[batchIndex] =
              PrimitiveObjectInspectorUtils.getInt(
                  object, sourcePrimitiveOI);
          break;
        case LONG:
          ((LongColumnVector) columnVector).vector[batchIndex] =
              PrimitiveObjectInspectorUtils.getLong(
                  object, sourcePrimitiveOI);
          break;
        case TIMESTAMP:
          {
            final Timestamp timestamp =
              PrimitiveObjectInspectorUtils.getTimestamp(
                  object, sourcePrimitiveOI);
            if (timestamp == null) {
              VectorizedBatchUtil.setNullColIsNullValue(columnVector, batchIndex);
              return;
            }
            ((TimestampColumnVector) columnVector).set(
                batchIndex, timestamp.toSqlTimestamp());
          }
          break;
        case DATE:
          {
            final Date date = PrimitiveObjectInspectorUtils.getDate(
                object, sourcePrimitiveOI);
            if (date == null) {
              VectorizedBatchUtil.setNullColIsNullValue(columnVector, batchIndex);
              return;
            }
            DateWritableV2 dateWritable = (DateWritableV2) convertTargetWritable;
            if (dateWritable == null) {
              dateWritable = new DateWritableV2();
            }
            dateWritable.set(date);
            ((LongColumnVector) columnVector).vector[batchIndex] =
                dateWritable.getDays();
          }
          break;
        case FLOAT:
          ((DoubleColumnVector) columnVector).vector[batchIndex] =
              PrimitiveObjectInspectorUtils.getFloat(
                  object, sourcePrimitiveOI);
          break;
        case DOUBLE:
          ((DoubleColumnVector) columnVector).vector[batchIndex] =
              PrimitiveObjectInspectorUtils.getDouble(
                  object, sourcePrimitiveOI);
          break;
        case BINARY:
          {
            final BytesWritable bytesWritable =
                PrimitiveObjectInspectorUtils.getBinary(
                    object, sourcePrimitiveOI);
            if (bytesWritable == null) {
              VectorizedBatchUtil.setNullColIsNullValue(columnVector, batchIndex);
              return;
            }
            ((BytesColumnVector) columnVector).setVal(
                batchIndex, bytesWritable.getBytes(), 0, bytesWritable.getLength());
          }
          break;
        case STRING:
          {
            final String string = PrimitiveObjectInspectorUtils.getString(
                object, sourcePrimitiveOI);
            if (string == null) {
              VectorizedBatchUtil.setNullColIsNullValue(columnVector, batchIndex);
              return;
            }
            Text text = (Text) convertTargetWritable;
            if (text == null) {
              text = new Text();
            }
            text.set(string);
            ((BytesColumnVector) columnVector).setVal(
                batchIndex, text.getBytes(), 0, text.getLength());
          }
          break;
        case VARCHAR:
          {
            // UNDONE: Performance problem with conversion to String, then bytes...

            final HiveVarchar hiveVarchar =
                PrimitiveObjectInspectorUtils.getHiveVarchar(
                    object, sourcePrimitiveOI);
            if (hiveVarchar == null) {
              VectorizedBatchUtil.setNullColIsNullValue(columnVector, batchIndex);
              return;
            }

            // TODO: Do we need maxLength checking?

            byte[] bytes = hiveVarchar.getValue().getBytes();
            ((BytesColumnVector) columnVector).setVal(
                batchIndex, bytes, 0, bytes.length);
          }
          break;
        case CHAR:
          {
            // UNDONE: Performance problem with conversion to String, then bytes...

            final HiveChar hiveChar =
                PrimitiveObjectInspectorUtils.getHiveChar(
                    object, sourcePrimitiveOI);
            if (hiveChar == null) {
              VectorizedBatchUtil.setNullColIsNullValue(columnVector, batchIndex);
              return;
            }
            // We store CHAR in vector row batch with padding stripped.

            // TODO: Do we need maxLength checking?

            final byte[] bytes = hiveChar.getStrippedValue().getBytes();
            ((BytesColumnVector) columnVector).setVal(
                batchIndex, bytes, 0, bytes.length);
          }
          break;
        case DECIMAL:
          {
            final HiveDecimal hiveDecimal =
                PrimitiveObjectInspectorUtils.getHiveDecimal(
                    object, sourcePrimitiveOI);
            if (hiveDecimal == null) {
              VectorizedBatchUtil.setNullColIsNullValue(columnVector, batchIndex);
              return;
            }
            if (columnVector instanceof Decimal64ColumnVector) {
              Decimal64ColumnVector dec64ColVector = (Decimal64ColumnVector) columnVector;
              dec64ColVector.set(batchIndex, hiveDecimal);
              if (dec64ColVector.isNull[batchIndex]) {
                return;
              }
            } else {
              ((DecimalColumnVector) columnVector).set(
                  batchIndex, hiveDecimal);
            }
          }
          break;
        case INTERVAL_YEAR_MONTH:
          {
            final HiveIntervalYearMonth intervalYearMonth =
                PrimitiveObjectInspectorUtils.getHiveIntervalYearMonth(
                    object, sourcePrimitiveOI);
            if (intervalYearMonth == null) {
              VectorizedBatchUtil.setNullColIsNullValue(columnVector, batchIndex);
              return;
            }
            ((LongColumnVector) columnVector).vector[batchIndex] =
                intervalYearMonth.getTotalMonths();
          }
          break;
        case INTERVAL_DAY_TIME:
          {
            final HiveIntervalDayTime intervalDayTime =
                PrimitiveObjectInspectorUtils.getHiveIntervalDayTime(
                    object, sourcePrimitiveOI);
            if (intervalDayTime == null) {
              VectorizedBatchUtil.setNullColIsNullValue(columnVector, batchIndex);
              return;
            }
            ((IntervalDayTimeColumnVector) columnVector).set(
                batchIndex, intervalDayTime);
          }
          break;
        default:
          throw new RuntimeException("Primitive category " + targetPrimitiveCategory.name() +
              " not supported");
        }
        break;
      case LIST:
        {
          final ListColumnVector listColumnVector = (ListColumnVector) columnVector;
          final ListObjectInspector sourceListOI = (ListObjectInspector) sourceObjectInspector;
          final ObjectInspector sourceElementOI = sourceListOI.getListElementObjectInspector();
          final int size = sourceListOI.getListLength(object);
          final TypeInfo targetElementTypeInfo = ((ListTypeInfo) targetTypeInfo).getListElementTypeInfo();

          listColumnVector.offsets[batchIndex] = listColumnVector.childCount;
          listColumnVector.childCount += size;
          listColumnVector.ensureSize(listColumnVector.childCount, true);
          listColumnVector.lengths[batchIndex] = size;

          for (int i = 0; i < size; i++) {
            final Object element = sourceListOI.getListElement(object, i);
            final int offset = (int) (listColumnVector.offsets[batchIndex] + i);
            assignConvertRowColumn(
                listColumnVector.child,
                offset,
                targetElementTypeInfo,
                sourceElementOI,
                null,
                element);
          }
        }
        break;
      case MAP:
        {
          final MapColumnVector mapColumnVector = (MapColumnVector) columnVector;
          final MapObjectInspector mapObjectInspector = (MapObjectInspector) sourceObjectInspector;
          final MapTypeInfo mapTypeInfo = (MapTypeInfo) targetTypeInfo;

          final Map<?, ?> map = mapObjectInspector.getMap(object);
          for (Map.Entry<?, ?> entry : map.entrySet()) {
            assignConvertRowColumn(
                mapColumnVector.keys,
                batchIndex,
                mapTypeInfo.getMapKeyTypeInfo(),
                mapObjectInspector.getMapKeyObjectInspector(),
                null,
                entry.getKey());
            assignConvertRowColumn(
                mapColumnVector.values,
                batchIndex,
                mapTypeInfo.getMapValueTypeInfo(),
                mapObjectInspector.getMapValueObjectInspector(),
                null,
                entry.getValue());
          }
        }
        break;
      case STRUCT:
        {
          final StructColumnVector structColumnVector = (StructColumnVector) columnVector;
          final StructObjectInspector sourceStructOI = (StructObjectInspector) sourceObjectInspector;
          final List<? extends StructField> sourceFields = sourceStructOI.getAllStructFieldRefs();
          final StructTypeInfo targetStructTypeInfo = (StructTypeInfo) targetTypeInfo;
          final List<TypeInfo> targetTypeInfos = targetStructTypeInfo.getAllStructFieldTypeInfos();
          final int size = targetTypeInfos.size();

          for (int i = 0; i < size; i++) {
            if (i < sourceFields.size()) {
              final StructField sourceStructField = sourceFields.get(i);
              final ObjectInspector sourceFieldOI = sourceStructField.getFieldObjectInspector();
              final Object sourceData = sourceStructOI.getStructFieldData(object, sourceStructField);
              assignConvertRowColumn(
                  structColumnVector.fields[i],
                  batchIndex,
                  targetTypeInfos.get(i),
                  sourceFieldOI,
                  null,
                  sourceData);
            } else {
              final ColumnVector fieldColumnVector = structColumnVector.fields[i];
              VectorizedBatchUtil.setNullColIsNullValue(fieldColumnVector, batchIndex);
            }
          }
        }
        break;
      case UNION:
        {
          final UnionColumnVector unionColumnVector = (UnionColumnVector) columnVector;
          final UnionObjectInspector unionObjectInspector = (UnionObjectInspector) sourceObjectInspector;
          final UnionTypeInfo unionTypeInfo = (UnionTypeInfo) targetTypeInfo;
          final int tag = unionObjectInspector.getTag(object);

          assignConvertRowColumn(
              unionColumnVector.fields[tag],
              batchIndex,
              unionTypeInfo.getAllUnionObjectTypeInfos().get(tag),
              unionObjectInspector.getObjectInspectors().get(tag),
              null,
              unionObjectInspector.getField(tag));
        }
        break;
      default:
        throw new RuntimeException("Category " + targetCategory.name() + " not supported");
      }
    } catch (NumberFormatException e) {

      // Some of the conversion methods throw this exception on numeric parsing errors.
      VectorizedBatchUtil.setNullColIsNullValue(columnVector, batchIndex);
      return;
    }

    // We always set the null flag to false when there is a value.
    columnVector.isNull[batchIndex] = false;
  }

  /*
   * Assign a row from an array of objects.
   */
  public void assignRow(VectorizedRowBatch batch, int batchIndex, Object[] objects) {
    final int count = isConvert.length;
    for (int i = 0; i < count; i++) {
      if (isConvert[i]) {
        assignConvertRowColumn(batch, batchIndex, i, objects[i]);
      } else {
        assignRowColumn(batch, batchIndex, i, objects[i]);
      }
    }
  }

  public void assignRow(VectorizedRowBatch batch, int batchIndex, ArrayList<Object> objectList) {
    final int count = isConvert.length;
    for (int i = 0; i < count; i++) {
      if (isConvert[i]) {
        assignConvertRowColumn(batch, batchIndex, i, objectList.get(i));
      } else {
        assignRowColumn(batch, batchIndex, i, objectList.get(i));
      }
    }
  }

  /*
   * Assign a row from a list of standard objects up to a count
   */
  public void assignRow(VectorizedRowBatch batch, int batchIndex,
      List<Object> standardObjects, int columnCount) {

    for (int i = 0; i < columnCount; i++) {
      if (isConvert[i]) {
        assignConvertRowColumn(batch, batchIndex, i, standardObjects.get(i));
      } else {
        assignRowColumn(batch, batchIndex, i, standardObjects.get(i));
      }
    }
  }
}