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

import java.io.EOFException;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.VectorPartitionConversion;
import org.apache.hadoop.hive.serde2.fast.DeserializeRead;
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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;

/**
 * This class deserializes a serialization format into a row of a VectorizedRowBatch.
 *
 * The caller provides the hive type names and output column numbers in the order desired to
 * deserialize.
 *
 * This class uses an provided DeserializeRead object to directly deserialize by reading
 * field-by-field from a serialization format into the primitive values of the VectorizedRowBatch.
 */

public final class VectorDeserializeRow<T extends DeserializeRead> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(VectorDeserializeRow.class);

  private T deserializeRead;

  private TypeInfo[] sourceTypeInfos;

  public VectorDeserializeRow(T deserializeRead) {
    this();
    this.deserializeRead = deserializeRead;
    sourceTypeInfos = deserializeRead.typeInfos();
  }

  // Not public since we must have the deserialize read object.
  private VectorDeserializeRow() {
  }

  /*
   * These members have information for deserializing a row into the VectorizedRowBatch
   * columns.
   *
   * We say "source" because when there is conversion we are converting th deserialized source into
   * a target data type.
   */
  boolean[] isConvert;
                // For each column, are we converting the row column?

  int[] projectionColumnNums;
                // Assigning can be a subset of columns, so this is the projection --
                // the batch column numbers.

  Category[] sourceCategories;
                // The data type category of each column being deserialized.

  PrimitiveCategory[] sourcePrimitiveCategories;
                //The data type primitive category of each column being deserialized.

  int[] maxLengths;
                // For the CHAR and VARCHAR data types, the maximum character length of
                // the columns.  Otherwise, 0.

  /*
   * These members have information for data type conversion.
   * Not defined if there is no conversion.
   */
  Writable[] convertSourceWritables;
                // Conversion requires source be placed in writable so we can call upon
                // VectorAssignRow to convert and assign the row column.

  VectorAssignRow convertVectorAssignRow;
                // Use its conversion ability.

  /*
   * Allocate the source deserialization related arrays.
   */
  private void allocateArrays(int count) {
    isConvert = new boolean[count];
    projectionColumnNums = new int[count];
    sourceCategories = new Category[count];
    sourcePrimitiveCategories = new PrimitiveCategory[count];
    maxLengths = new int[count];
  }

  /*
   * Allocate the conversion related arrays (optional).
   */
  private void allocateConvertArrays(int count) {
    convertSourceWritables = new Writable[count];
  }

  /*
   * Initialize one column's source deserializtion related arrays.
   */
  private void initSourceEntry(int logicalColumnIndex, int projectionColumnNum, TypeInfo sourceTypeInfo) {
    isConvert[logicalColumnIndex] = false;
    projectionColumnNums[logicalColumnIndex] = projectionColumnNum;
    Category sourceCategory = sourceTypeInfo.getCategory();
    sourceCategories[logicalColumnIndex] = sourceCategory;
    if (sourceCategory == Category.PRIMITIVE) {
      PrimitiveTypeInfo sourcePrimitiveTypeInfo = (PrimitiveTypeInfo) sourceTypeInfo;
      PrimitiveCategory sourcePrimitiveCategory = sourcePrimitiveTypeInfo.getPrimitiveCategory();
      sourcePrimitiveCategories[logicalColumnIndex] = sourcePrimitiveCategory;
      switch (sourcePrimitiveCategory) {
      case CHAR:
        maxLengths[logicalColumnIndex] = ((CharTypeInfo) sourcePrimitiveTypeInfo).getLength();
        break;
      case VARCHAR:
        maxLengths[logicalColumnIndex] = ((VarcharTypeInfo) sourcePrimitiveTypeInfo).getLength();
        break;
      default:
        // No additional data type specific setting.
        break;
      }
    } else {
      // We don't currently support complex types.
      Preconditions.checkState(false);
    }
  }

  /*
   * Initialize the conversion related arrays.  Assumes initSourceEntry has already been called.
   */
  private void initConvertTargetEntry(int logicalColumnIndex) {
    isConvert[logicalColumnIndex] = true;

    if (sourceCategories[logicalColumnIndex] == Category.PRIMITIVE) {
      convertSourceWritables[logicalColumnIndex] =
          VectorizedBatchUtil.getPrimitiveWritable(sourcePrimitiveCategories[logicalColumnIndex]);
    } else {
      // We don't currently support complex types.
      Preconditions.checkState(false);
    }
  }

  /*
   * Specify the columns to deserialize into as an array.
   */
  public void init(int[] outputColumns) throws HiveException {

    final int count = sourceTypeInfos.length;
    allocateArrays(count);

    for (int i = 0; i < count; i++) {
      int outputColumn = outputColumns[i];
      initSourceEntry(i, outputColumn, sourceTypeInfos[i]);
    }
  }

  /*
   * Specify the columns to deserialize into as a list.
   */
  public void init(List<Integer> outputColumns) throws HiveException {

    final int count = sourceTypeInfos.length;
    allocateArrays(count);

    for (int i = 0; i < count; i++) {
      int outputColumn = outputColumns.get(i);
      initSourceEntry(i, outputColumn, sourceTypeInfos[i]);
    }
  }

  /*
   * Specify the columns to deserialize into a range starting at a column number.
   */
  public void init(int startColumn) throws HiveException {

    final int count = sourceTypeInfos.length;
    allocateArrays(count);

    for (int i = 0; i < count; i++) {
      int outputColumn = startColumn + i;
      initSourceEntry(i, outputColumn, sourceTypeInfos[i]);
    }
  }

  public void init(boolean[] columnsToIncludeTruncated) throws HiveException {

    if (columnsToIncludeTruncated != null) {
      deserializeRead.setColumnsToInclude(columnsToIncludeTruncated);
    }

    final int columnCount = (columnsToIncludeTruncated == null ?
        sourceTypeInfos.length : columnsToIncludeTruncated.length);
    allocateArrays(columnCount);

    for (int i = 0; i < columnCount; i++) {

      if (columnsToIncludeTruncated != null && !columnsToIncludeTruncated[i]) {

        // Field not included in query.

      } else {

        initSourceEntry(i, i, sourceTypeInfos[i]);

      }
    }
  }

  /**
   * Initialize for converting the source data type that are going to be read with the
   * DeserializedRead interface passed to the constructor to the target data types desired in
   * the VectorizedRowBatch.
   *
   * No projection -- the column range 0 .. count-1
   *
   *    where count is the minimum of the target data type array size, included array size,
   *       and source data type array size.
   *
   * @param targetTypeInfos
   * @param columnsToIncludeTruncated
   * @return the minimum count described above is returned.  That is, the number of columns
   *         that will be processed by deserialize.
   * @throws HiveException
   */
  public int initConversion(TypeInfo[] targetTypeInfos,
      boolean[] columnsToIncludeTruncated) throws HiveException {

    if (columnsToIncludeTruncated != null) {
      deserializeRead.setColumnsToInclude(columnsToIncludeTruncated);
    }

    int targetColumnCount;
    if (columnsToIncludeTruncated == null) {
      targetColumnCount = targetTypeInfos.length;
    } else {
      targetColumnCount = Math.min(targetTypeInfos.length, columnsToIncludeTruncated.length);
    }

    int sourceColumnCount = Math.min(sourceTypeInfos.length, targetColumnCount);
    allocateArrays(sourceColumnCount);
    allocateConvertArrays(sourceColumnCount);

    boolean atLeastOneConvert = false;
    for (int i = 0; i < sourceColumnCount; i++) {

      if (columnsToIncludeTruncated != null && !columnsToIncludeTruncated[i]) {

        // Field not included in query.

      } else {

        TypeInfo sourceTypeInfo = sourceTypeInfos[i];
        TypeInfo targetTypeInfo = targetTypeInfos[i];

        if (!sourceTypeInfo.equals(targetTypeInfo)) {

          if (VectorPartitionConversion.isImplicitVectorColumnConversion(sourceTypeInfo, targetTypeInfo)) {

            // Do implicit conversion from source type to target type.
            initSourceEntry(i, i, sourceTypeInfo);

          } else {

            // Do formal conversion...
            initSourceEntry(i, i, sourceTypeInfo);
            initConvertTargetEntry(i);
            atLeastOneConvert = true;

          }
        } else {

          // No conversion.
          initSourceEntry(i, i, sourceTypeInfo);

        }
      }
    }

    if (atLeastOneConvert) {

      // Let the VectorAssignRow class do the conversion.
      convertVectorAssignRow = new VectorAssignRow();
      convertVectorAssignRow.initConversion(sourceTypeInfos, targetTypeInfos,
          columnsToIncludeTruncated);
    }

    return sourceColumnCount;
  }

  public void init() throws HiveException {
    init(0);
  }

  /**
   * Deserialize one row column value.
   *
   * @param batch
   * @param batchIndex
   * @param logicalColumnIndex
   * @throws IOException
   */
  private void deserializeRowColumn(VectorizedRowBatch batch, int batchIndex,
      int logicalColumnIndex) throws IOException {
    final int projectionColumnNum = projectionColumnNums[logicalColumnIndex];
    if (deserializeRead.readCheckNull()) {
      VectorizedBatchUtil.setNullColIsNullValue(batch.cols[projectionColumnNum], batchIndex);
      return;
    }

    // We have a value for the row column.
    Category sourceCategory = sourceCategories[logicalColumnIndex];
    if (sourceCategory == null) {
      /*
       * This is a column that we don't want (i.e. not included).
       * The deserializeRead.readCheckNull() has read the field, so we are done.
       */
      return;
    }
    switch (sourceCategory) {
    case PRIMITIVE:
      {
        PrimitiveCategory sourcePrimitiveCategory = sourcePrimitiveCategories[logicalColumnIndex];
        switch (sourcePrimitiveCategory) {
        case VOID:
          VectorizedBatchUtil.setNullColIsNullValue(batch.cols[projectionColumnNum], batchIndex);
          return;
        case BOOLEAN:
          ((LongColumnVector) batch.cols[projectionColumnNum]).vector[batchIndex] =
              (deserializeRead.currentBoolean ? 1 : 0);
          break;
        case BYTE:
          ((LongColumnVector) batch.cols[projectionColumnNum]).vector[batchIndex] =
              deserializeRead.currentByte;
          break;
        case SHORT:
          ((LongColumnVector) batch.cols[projectionColumnNum]).vector[batchIndex] =
              deserializeRead.currentShort;
          break;
        case INT:
          ((LongColumnVector) batch.cols[projectionColumnNum]).vector[batchIndex] =
              deserializeRead.currentInt;
          break;
        case LONG:
          ((LongColumnVector) batch.cols[projectionColumnNum]).vector[batchIndex] =
              deserializeRead.currentLong;
          break;
        case TIMESTAMP:
          ((TimestampColumnVector) batch.cols[projectionColumnNum]).set(
              batchIndex, deserializeRead.currentTimestampWritable.getTimestamp());
          break;
        case DATE:
          ((LongColumnVector) batch.cols[projectionColumnNum]).vector[batchIndex] =
              deserializeRead.currentDateWritable.getDays();
          break;
        case FLOAT:
          ((DoubleColumnVector) batch.cols[projectionColumnNum]).vector[batchIndex] =
              deserializeRead.currentFloat;
          break;
        case DOUBLE:
          ((DoubleColumnVector) batch.cols[projectionColumnNum]).vector[batchIndex] =
              deserializeRead.currentDouble;
          break;
        case BINARY:
        case STRING:
          ((BytesColumnVector) batch.cols[projectionColumnNum]).setVal(
              batchIndex,
              deserializeRead.currentBytes,
              deserializeRead.currentBytesStart,
              deserializeRead.currentBytesLength);
          break;
        case VARCHAR:
          {
            // Use the basic STRING bytes read to get access, then use our optimal truncate/trim method
            // that does not use Java String objects.
            int adjustedLength = StringExpr.truncate(
                deserializeRead.currentBytes,
                deserializeRead.currentBytesStart,
                deserializeRead.currentBytesLength,
                maxLengths[logicalColumnIndex]);
            ((BytesColumnVector) batch.cols[projectionColumnNum]).setVal(
                batchIndex,
                deserializeRead.currentBytes,
                deserializeRead.currentBytesStart,
                adjustedLength);
          }
          break;
        case CHAR:
          {
            // Use the basic STRING bytes read to get access, then use our optimal truncate/trim method
            // that does not use Java String objects.
            int adjustedLength = StringExpr.rightTrimAndTruncate(
                deserializeRead.currentBytes,
                deserializeRead.currentBytesStart,
                deserializeRead.currentBytesLength,
                maxLengths[logicalColumnIndex]);
            ((BytesColumnVector) batch.cols[projectionColumnNum]).setVal(
                batchIndex,
                deserializeRead.currentBytes,
                deserializeRead.currentBytesStart,
                adjustedLength);
          }
          break;
        case DECIMAL:
          ((DecimalColumnVector) batch.cols[projectionColumnNum]).set(
              batchIndex, deserializeRead.currentHiveDecimalWritable.getHiveDecimal());
          break;
        case INTERVAL_YEAR_MONTH:
          ((LongColumnVector) batch.cols[projectionColumnNum]).vector[batchIndex] =
              deserializeRead.currentHiveIntervalYearMonthWritable.getHiveIntervalYearMonth().getTotalMonths();
          break;
        case INTERVAL_DAY_TIME:
          ((IntervalDayTimeColumnVector) batch.cols[projectionColumnNum]).set(
              batchIndex, deserializeRead.currentHiveIntervalDayTimeWritable.getHiveIntervalDayTime());
          break;
        default:
          throw new RuntimeException("Primitive category " + sourcePrimitiveCategory.name() +
              " not supported");
        }
      }
      break;
    default:
      throw new RuntimeException("Category " + sourceCategory.name() + " not supported");
    }

    // We always set the null flag to false when there is a value.
    batch.cols[projectionColumnNum].isNull[batchIndex] = false;
  }

  /**
   * Deserialize and convert one row column value.
   *
   * We deserialize into a writable and then pass that writable to an instance of VectorAssignRow
   * to convert the writable to the target data type and assign it into the VectorizedRowBatch.
   *
   * @param batch
   * @param batchIndex
   * @param logicalColumnIndex
   * @throws IOException
   */
  private void deserializeConvertRowColumn(VectorizedRowBatch batch, int batchIndex,
      int logicalColumnIndex) throws IOException {
    final int projectionColumnNum = projectionColumnNums[logicalColumnIndex];
    if (deserializeRead.readCheckNull()) {
      VectorizedBatchUtil.setNullColIsNullValue(batch.cols[projectionColumnNum], batchIndex);
      return;
    }

    // We have a value for the row column.
    Category sourceCategory = sourceCategories[logicalColumnIndex];
    if (sourceCategory == null) {
      /*
       * This is a column that we don't want (i.e. not included).
       * The deserializeRead.readCheckNull() has read the field, so we are done.
       */
      return;
    }
    Writable convertSourceWritable = convertSourceWritables[logicalColumnIndex];
    switch (sourceCategory) {
    case PRIMITIVE:
      {
        PrimitiveCategory sourcePrimitiveCategory = sourcePrimitiveCategories[logicalColumnIndex];
        switch (sourcePrimitiveCategory) {
        case VOID:
          convertSourceWritable = null;
          break;
        case BOOLEAN:
          ((BooleanWritable) convertSourceWritable).set(deserializeRead.currentBoolean);
          break;
        case BYTE:
          ((ByteWritable) convertSourceWritable).set(deserializeRead.currentByte);
          break;
        case SHORT:
          ((ShortWritable) convertSourceWritable).set(deserializeRead.currentShort);
          break;
        case INT:
          ((IntWritable) convertSourceWritable).set(deserializeRead.currentInt);
          break;
        case LONG:
          ((LongWritable) convertSourceWritable).set(deserializeRead.currentLong);
          break;
        case TIMESTAMP:
          ((TimestampWritable) convertSourceWritable).set(deserializeRead.currentTimestampWritable);
          break;
        case DATE:
          ((DateWritable) convertSourceWritable).set(deserializeRead.currentDateWritable);
          break;
        case FLOAT:
          ((FloatWritable) convertSourceWritable).set(deserializeRead.currentFloat);
          break;
        case DOUBLE:
          ((DoubleWritable) convertSourceWritable).set(deserializeRead.currentDouble);
          break;
        case BINARY:
          if (deserializeRead.currentBytes == null) {
            LOG.info("null binary entry: batchIndex " + batchIndex + " projection column num " + projectionColumnNum);
          }

          ((BytesWritable) convertSourceWritable).set(
              deserializeRead.currentBytes,
              deserializeRead.currentBytesStart,
              deserializeRead.currentBytesLength);
          break;
        case STRING:
          if (deserializeRead.currentBytes == null) {
            throw new RuntimeException(
                "null string entry: batchIndex " + batchIndex + " projection column num " + projectionColumnNum);
          }

          // Use org.apache.hadoop.io.Text as our helper to go from byte[] to String.
          ((Text) convertSourceWritable).set(
              deserializeRead.currentBytes,
              deserializeRead.currentBytesStart,
              deserializeRead.currentBytesLength);
          break;
        case VARCHAR:
          {
            // Use the basic STRING bytes read to get access, then use our optimal truncate/trim method
            // that does not use Java String objects.
            if (deserializeRead.currentBytes == null) {
              throw new RuntimeException(
                  "null varchar entry: batchIndex " + batchIndex + " projection column num " + projectionColumnNum);
            }

            int adjustedLength = StringExpr.truncate(
                deserializeRead.currentBytes,
                deserializeRead.currentBytesStart,
                deserializeRead.currentBytesLength,
                maxLengths[logicalColumnIndex]);

            ((HiveVarcharWritable) convertSourceWritable).set(
                new String(
                  deserializeRead.currentBytes,
                  deserializeRead.currentBytesStart,
                  adjustedLength,
                  Charsets.UTF_8),
                -1);
          }
          break;
        case CHAR:
          {
            // Use the basic STRING bytes read to get access, then use our optimal truncate/trim method
            // that does not use Java String objects.
            if (deserializeRead.currentBytes == null) {
              throw new RuntimeException(
                  "null char entry: batchIndex " + batchIndex + " projection column num " + projectionColumnNum);
            }

            int adjustedLength = StringExpr.rightTrimAndTruncate(
                deserializeRead.currentBytes,
                deserializeRead.currentBytesStart,
                deserializeRead.currentBytesLength,
                maxLengths[logicalColumnIndex]);

            ((HiveCharWritable) convertSourceWritable).set(
                new String(
                  deserializeRead.currentBytes,
                  deserializeRead.currentBytesStart,
                  adjustedLength, Charsets.UTF_8),
                -1);
          }
          break;
        case DECIMAL:
          ((HiveDecimalWritable) convertSourceWritable).set(
              deserializeRead.currentHiveDecimalWritable);
          break;
        case INTERVAL_YEAR_MONTH:
          ((HiveIntervalYearMonthWritable) convertSourceWritable).set(
              deserializeRead.currentHiveIntervalYearMonthWritable);
          break;
        case INTERVAL_DAY_TIME:
          ((HiveIntervalDayTimeWritable) convertSourceWritable).set(
              deserializeRead.currentHiveIntervalDayTimeWritable);
          break;
        default:
          throw new RuntimeException("Primitive category " + sourcePrimitiveCategory.name() +
              " not supported");
        }
      }
      break;
    default:
      throw new RuntimeException("Category " + sourceCategory.name() + " not supported");
    }

    /*
     * Convert our source object we just read into the target object and store that in the
     * VectorizedRowBatch.
     */
    convertVectorAssignRow.assignConvertRowColumn(batch, batchIndex, logicalColumnIndex,
        convertSourceWritable);
  }

  /**
   * Specify the range of bytes to deserialize in the next call to the deserialize method.
   *
   * @param bytes
   * @param offset
   * @param length
   */
  public void setBytes(byte[] bytes, int offset, int length) {
    deserializeRead.set(bytes, offset, length);
  }

  /**
   * Deserialize a row from the range of bytes specified by setBytes.
   *
   * Use getDetailedReadPositionString to get detailed read position information to help
   * diagnose exceptions that are thrown...
   *
   * @param batch
   * @param batchIndex
   * @throws IOException
   */
  public void deserialize(VectorizedRowBatch batch, int batchIndex) throws IOException {
    final int count = isConvert.length;
    for (int i = 0; i < count; i++) {
      if (isConvert[i]) {
        deserializeConvertRowColumn(batch, batchIndex, i);
      } else {
        deserializeRowColumn(batch, batchIndex, i);
      }
    }
    deserializeRead.extraFieldsCheck();
  }

  public String getDetailedReadPositionString() {
    return deserializeRead.getDetailedReadPositionString();
  }
}
