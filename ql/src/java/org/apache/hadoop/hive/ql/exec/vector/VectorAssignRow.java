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

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.VectorPartitionConversion;
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
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
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

  Category[] targetCategories;
                // The data type category of each column being assigned.

  PrimitiveCategory[] targetPrimitiveCategories;
                // The data type primitive category of each column being assigned.

  int[] maxLengths;
                // For the CHAR and VARCHAR data types, the maximum character length of
                // the columns.  Otherwise, 0.

  /*
   * These members have information for data type conversion.
   * Not defined if there is no conversion.
   */
  PrimitiveObjectInspector[] convertSourcePrimitiveObjectInspectors;
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
    targetCategories = new Category[count];
    targetPrimitiveCategories = new PrimitiveCategory[count];
    maxLengths = new int[count];
  }

  /*
   * Allocate the source conversion related arrays (optional).
   */
  private void allocateConvertArrays(int count) {
    convertSourcePrimitiveObjectInspectors = new PrimitiveObjectInspector[count];
    convertTargetWritables = new Writable[count];
  }

  /*
   * Initialize one column's target related arrays.
   */
  private void initTargetEntry(int logicalColumnIndex, int projectionColumnNum, TypeInfo typeInfo) {
    isConvert[logicalColumnIndex] = false;
    projectionColumnNums[logicalColumnIndex] = projectionColumnNum;
    Category category = typeInfo.getCategory();
    targetCategories[logicalColumnIndex] = category;
    if (category == Category.PRIMITIVE) {
      PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
      PrimitiveCategory primitiveCategory = primitiveTypeInfo.getPrimitiveCategory();
      targetPrimitiveCategories[logicalColumnIndex] = primitiveCategory;
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
    Category convertSourceCategory = convertSourceTypeInfo.getCategory();
    if (convertSourceCategory == Category.PRIMITIVE) {
      PrimitiveTypeInfo convertSourcePrimitiveTypeInfo = (PrimitiveTypeInfo) convertSourceTypeInfo;
      convertSourcePrimitiveObjectInspectors[logicalColumnIndex] =
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
            convertSourcePrimitiveTypeInfo);

      // These need to be based on the target.
      PrimitiveCategory targetPrimitiveCategory = targetPrimitiveCategories[logicalColumnIndex];
      switch (targetPrimitiveCategory) {
      case DATE:
        convertTargetWritables[logicalColumnIndex] = new DateWritable();
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

    List<? extends StructField> fields = structObjectInspector.getAllStructFieldRefs();
    final int count = fields.size();
    allocateArrays(count);

    for (int i = 0; i < count; i++) {

      int projectionColumnNum = projectedColumns.get(i);

      StructField field = fields.get(i);
      ObjectInspector fieldInspector = field.getFieldObjectInspector();
      TypeInfo typeInfo =
          TypeInfoUtils.getTypeInfoFromTypeString(fieldInspector.getTypeName());

      initTargetEntry(i, projectionColumnNum, typeInfo);
    }
  }

  /*
   * Initialize using an StructObjectInspector.
   * No projection -- the column range 0 .. fields.size()-1
   */
  public void init(StructObjectInspector structObjectInspector) throws HiveException {

    List<? extends StructField> fields = structObjectInspector.getAllStructFieldRefs();
    final int count = fields.size();
    allocateArrays(count);

    for (int i = 0; i < count; i++) {

      StructField field = fields.get(i);
      ObjectInspector fieldInspector = field.getFieldObjectInspector();
      TypeInfo typeInfo =
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

      TypeInfo typeInfo =
          TypeInfoUtils.getTypeInfoFromTypeString(typeNames.get(i));

      initTargetEntry(i, i, typeInfo);
    }
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

    int targetColumnCount;
    if (columnsToIncludeTruncated == null) {
      targetColumnCount = targetTypeInfos.length;
    } else {
      targetColumnCount = Math.min(targetTypeInfos.length, columnsToIncludeTruncated.length);
    }

    int sourceColumnCount = Math.min(sourceTypeInfos.length, targetColumnCount);

    allocateArrays(sourceColumnCount);
    allocateConvertArrays(sourceColumnCount);

    for (int i = 0; i < sourceColumnCount; i++) {

      if (columnsToIncludeTruncated != null && !columnsToIncludeTruncated[i]) {

        // Field not included in query.

      } else {
        TypeInfo targetTypeInfo = targetTypeInfos[i];

        if (targetTypeInfo.getCategory() != ObjectInspector.Category.PRIMITIVE) {

          // For now, we don't have an assigner for complex types...

        } else {
          TypeInfo sourceTypeInfo = sourceTypeInfos[i];

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
  public void assignRowColumn(VectorizedRowBatch batch, int batchIndex, int logicalColumnIndex,
      Object object) {
    Category targetCategory = targetCategories[logicalColumnIndex];
    if (targetCategory == null) {
      /*
       * This is a column that we don't want (i.e. not included) -- we are done.
       */
      return;
    }
    final int projectionColumnNum = projectionColumnNums[logicalColumnIndex];
    if (object == null) {
      VectorizedBatchUtil.setNullColIsNullValue(batch.cols[projectionColumnNum], batchIndex);
      return;
    }
    switch (targetCategory) {
    case PRIMITIVE:
      {
        PrimitiveCategory targetPrimitiveCategory = targetPrimitiveCategories[logicalColumnIndex];
        switch (targetPrimitiveCategory) {
        case VOID:
          VectorizedBatchUtil.setNullColIsNullValue(batch.cols[projectionColumnNum], batchIndex);
          return;
        case BOOLEAN:
          ((LongColumnVector) batch.cols[projectionColumnNum]).vector[batchIndex] =
              (((BooleanWritable) object).get() ? 1 : 0);
          break;
        case BYTE:
          ((LongColumnVector) batch.cols[projectionColumnNum]).vector[batchIndex] =
             ((ByteWritable) object).get();
          break;
        case SHORT:
          ((LongColumnVector) batch.cols[projectionColumnNum]).vector[batchIndex] =
              ((ShortWritable) object).get();
          break;
        case INT:
          ((LongColumnVector) batch.cols[projectionColumnNum]).vector[batchIndex] =
              ((IntWritable) object).get();
          break;
        case LONG:
          ((LongColumnVector) batch.cols[projectionColumnNum]).vector[batchIndex] =
              ((LongWritable) object).get();
          break;
        case TIMESTAMP:
          ((TimestampColumnVector) batch.cols[projectionColumnNum]).set(
              batchIndex, ((TimestampWritable) object).getTimestamp());
          break;
        case DATE:
          ((LongColumnVector) batch.cols[projectionColumnNum]).vector[batchIndex] =
             ((DateWritable) object).getDays();
          break;
        case FLOAT:
          ((DoubleColumnVector) batch.cols[projectionColumnNum]).vector[batchIndex] =
              ((FloatWritable) object).get();
          break;
        case DOUBLE:
          ((DoubleColumnVector) batch.cols[projectionColumnNum]).vector[batchIndex] =
              ((DoubleWritable) object).get();
          break;
        case BINARY:
          {
            BytesWritable bw = (BytesWritable) object;
            ((BytesColumnVector) batch.cols[projectionColumnNum]).setVal(
                batchIndex, bw.getBytes(), 0, bw.getLength());
          }
          break;
        case STRING:
          {
            Text tw = (Text) object;
            ((BytesColumnVector) batch.cols[projectionColumnNum]).setVal(
                batchIndex, tw.getBytes(), 0, tw.getLength());
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
            ((BytesColumnVector) batch.cols[projectionColumnNum]).setVal(
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
            ((BytesColumnVector) batch.cols[projectionColumnNum]).setVal(
                batchIndex, bytes, 0, bytes.length);
          }
          break;
        case DECIMAL:
          if (object instanceof HiveDecimal) {
            ((DecimalColumnVector) batch.cols[projectionColumnNum]).set(
                batchIndex, (HiveDecimal) object);
          } else {
            ((DecimalColumnVector) batch.cols[projectionColumnNum]).set(
                batchIndex, (HiveDecimalWritable) object);
          }
          break;
        case INTERVAL_YEAR_MONTH:
          ((LongColumnVector) batch.cols[projectionColumnNum]).vector[batchIndex] =
              ((HiveIntervalYearMonthWritable) object).getHiveIntervalYearMonth().getTotalMonths();
          break;
        case INTERVAL_DAY_TIME:
          ((IntervalDayTimeColumnVector) batch.cols[projectionColumnNum]).set(
              batchIndex, ((HiveIntervalDayTimeWritable) object).getHiveIntervalDayTime());
          break;
        default:
          throw new RuntimeException("Primitive category " + targetPrimitiveCategory.name() +
              " not supported");
        }
      }
      break;
    default:
      throw new RuntimeException("Category " + targetCategory.name() + " not supported");
    }

    /*
     * We always set the null flag to false when there is a value.
     */
    batch.cols[projectionColumnNum].isNull[batchIndex] = false;
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
    Category targetCategory = targetCategories[logicalColumnIndex];
    if (targetCategory == null) {
      /*
       * This is a column that we don't want (i.e. not included) -- we are done.
       */
      return;
    }
    final int projectionColumnNum = projectionColumnNums[logicalColumnIndex];
    if (object == null) {
      VectorizedBatchUtil.setNullColIsNullValue(batch.cols[projectionColumnNum], batchIndex);
      return;
    }
    try {
      switch (targetCategory) {
      case PRIMITIVE:
        PrimitiveCategory targetPrimitiveCategory = targetPrimitiveCategories[logicalColumnIndex];
        switch (targetPrimitiveCategory) {
        case VOID:
          VectorizedBatchUtil.setNullColIsNullValue(batch.cols[projectionColumnNum], batchIndex);
          return;
        case BOOLEAN:
          ((LongColumnVector) batch.cols[projectionColumnNum]).vector[batchIndex] =
              (PrimitiveObjectInspectorUtils.getBoolean(
                  object, convertSourcePrimitiveObjectInspectors[logicalColumnIndex]) ? 1 : 0);
          break;
        case BYTE:
          ((LongColumnVector) batch.cols[projectionColumnNum]).vector[batchIndex] =
              PrimitiveObjectInspectorUtils.getByte(
                  object, convertSourcePrimitiveObjectInspectors[logicalColumnIndex]);
          break;
        case SHORT:
          ((LongColumnVector) batch.cols[projectionColumnNum]).vector[batchIndex] =
              PrimitiveObjectInspectorUtils.getShort(
                  object, convertSourcePrimitiveObjectInspectors[logicalColumnIndex]);
          break;
        case INT:
          ((LongColumnVector) batch.cols[projectionColumnNum]).vector[batchIndex] =
              PrimitiveObjectInspectorUtils.getInt(
                  object, convertSourcePrimitiveObjectInspectors[logicalColumnIndex]);
          break;
        case LONG:
          ((LongColumnVector) batch.cols[projectionColumnNum]).vector[batchIndex] =
              PrimitiveObjectInspectorUtils.getLong(
                  object, convertSourcePrimitiveObjectInspectors[logicalColumnIndex]);
          break;
        case TIMESTAMP:
          {
            Timestamp timestamp =
              PrimitiveObjectInspectorUtils.getTimestamp(
                  object, convertSourcePrimitiveObjectInspectors[logicalColumnIndex]);
            if (timestamp == null) {
              VectorizedBatchUtil.setNullColIsNullValue(batch.cols[projectionColumnNum], batchIndex);
              return;
            }
            ((TimestampColumnVector) batch.cols[projectionColumnNum]).set(
                batchIndex, timestamp);
          }
          break;
        case DATE:
          {
            Date date = PrimitiveObjectInspectorUtils.getDate(
                object, convertSourcePrimitiveObjectInspectors[logicalColumnIndex]);
            if (date == null) {
              VectorizedBatchUtil.setNullColIsNullValue(batch.cols[projectionColumnNum], batchIndex);
              return;
            }
            DateWritable dateWritable = (DateWritable) convertTargetWritables[logicalColumnIndex];
            dateWritable.set(date);
            ((LongColumnVector) batch.cols[projectionColumnNum]).vector[batchIndex] =
                dateWritable.getDays();
          }
          break;
        case FLOAT:
          ((DoubleColumnVector) batch.cols[projectionColumnNum]).vector[batchIndex] =
              PrimitiveObjectInspectorUtils.getFloat(
                  object, convertSourcePrimitiveObjectInspectors[logicalColumnIndex]);
          break;
        case DOUBLE:
          ((DoubleColumnVector) batch.cols[projectionColumnNum]).vector[batchIndex] =
              PrimitiveObjectInspectorUtils.getDouble(
                  object, convertSourcePrimitiveObjectInspectors[logicalColumnIndex]);
          break;
        case BINARY:
          {
            BytesWritable bytesWritable =
                PrimitiveObjectInspectorUtils.getBinary(
                    object, convertSourcePrimitiveObjectInspectors[logicalColumnIndex]);
            if (bytesWritable == null) {
              VectorizedBatchUtil.setNullColIsNullValue(batch.cols[projectionColumnNum], batchIndex);
              return;
            }
            ((BytesColumnVector) batch.cols[projectionColumnNum]).setVal(
                batchIndex, bytesWritable.getBytes(), 0, bytesWritable.getLength());
          }
          break;
        case STRING:
          {
            String string = PrimitiveObjectInspectorUtils.getString(
                object, convertSourcePrimitiveObjectInspectors[logicalColumnIndex]);
            if (string == null) {
              VectorizedBatchUtil.setNullColIsNullValue(batch.cols[projectionColumnNum], batchIndex);
              return;
            }
            Text text = (Text) convertTargetWritables[logicalColumnIndex];
            text.set(string);
            ((BytesColumnVector) batch.cols[projectionColumnNum]).setVal(
                batchIndex, text.getBytes(), 0, text.getLength());
          }
          break;
        case VARCHAR:
          {
            // UNDONE: Performance problem with conversion to String, then bytes...

            HiveVarchar hiveVarchar =
                PrimitiveObjectInspectorUtils.getHiveVarchar(
                    object, convertSourcePrimitiveObjectInspectors[logicalColumnIndex]);
            if (hiveVarchar == null) {
              VectorizedBatchUtil.setNullColIsNullValue(batch.cols[projectionColumnNum], batchIndex);
              return;
            }

            // TODO: Do we need maxLength checking?

            byte[] bytes = hiveVarchar.getValue().getBytes();
            ((BytesColumnVector) batch.cols[projectionColumnNum]).setVal(
                batchIndex, bytes, 0, bytes.length);
          }
          break;
        case CHAR:
          {
            // UNDONE: Performance problem with conversion to String, then bytes...

            HiveChar hiveChar =
                PrimitiveObjectInspectorUtils.getHiveChar(
                    object, convertSourcePrimitiveObjectInspectors[logicalColumnIndex]);
            if (hiveChar == null) {
              VectorizedBatchUtil.setNullColIsNullValue(batch.cols[projectionColumnNum], batchIndex);
              return;
            }
            // We store CHAR in vector row batch with padding stripped.

            // TODO: Do we need maxLength checking?

            byte[] bytes = hiveChar.getStrippedValue().getBytes();
            ((BytesColumnVector) batch.cols[projectionColumnNum]).setVal(
                batchIndex, bytes, 0, bytes.length);
          }
          break;
        case DECIMAL:
          {
            HiveDecimal hiveDecimal =
                PrimitiveObjectInspectorUtils.getHiveDecimal(
                    object, convertSourcePrimitiveObjectInspectors[logicalColumnIndex]);
            if (hiveDecimal == null) {
              VectorizedBatchUtil.setNullColIsNullValue(batch.cols[projectionColumnNum], batchIndex);
              return;
            }
            ((DecimalColumnVector) batch.cols[projectionColumnNum]).set(
                batchIndex, hiveDecimal);
          }
          break;
        case INTERVAL_YEAR_MONTH:
          {
            HiveIntervalYearMonth intervalYearMonth =
                PrimitiveObjectInspectorUtils.getHiveIntervalYearMonth(
                    object, convertSourcePrimitiveObjectInspectors[logicalColumnIndex]);
            if (intervalYearMonth == null) {
              VectorizedBatchUtil.setNullColIsNullValue(batch.cols[projectionColumnNum], batchIndex);
              return;
            }
            ((LongColumnVector) batch.cols[projectionColumnNum]).vector[batchIndex] =
                intervalYearMonth.getTotalMonths();
          }
          break;
        case INTERVAL_DAY_TIME:
          {
            HiveIntervalDayTime intervalDayTime =
                PrimitiveObjectInspectorUtils.getHiveIntervalDayTime(
                    object, convertSourcePrimitiveObjectInspectors[logicalColumnIndex]);
            if (intervalDayTime == null) {
              VectorizedBatchUtil.setNullColIsNullValue(batch.cols[projectionColumnNum], batchIndex);
              return;
            }
            ((IntervalDayTimeColumnVector) batch.cols[projectionColumnNum]).set(
                batchIndex, intervalDayTime);
          }
          break;
        default:
          throw new RuntimeException("Primitive category " + targetPrimitiveCategory.name() +
              " not supported");
        }
        break;
      default:
        throw new RuntimeException("Category " + targetCategory.name() + " not supported");
      }
    } catch (NumberFormatException e) {

      // Some of the conversion methods throw this exception on numeric parsing errors.
      VectorizedBatchUtil.setNullColIsNullValue(batch.cols[projectionColumnNum], batchIndex);
      return;
    }

    // We always set the null flag to false when there is a value.
    batch.cols[projectionColumnNum].isNull[batchIndex] = false;
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