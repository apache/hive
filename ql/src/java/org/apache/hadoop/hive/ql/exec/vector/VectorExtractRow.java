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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
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

  Category[] categories;
              // The data type category of each column being extracted.

  PrimitiveCategory[] primitiveCategories;
              // The data type primitive category of each column being assigned.

  int[] maxLengths;
              // For the CHAR and VARCHAR data types, the maximum character length of
              // the columns.  Otherwise, 0.

  Writable[] primitiveWritables;
            // The extracted values will be placed in these writables.

  /*
   * Allocate the various arrays.
   */
  private void allocateArrays(int count) {
    projectionColumnNums = new int[count];
    categories = new Category[count];
    primitiveCategories = new PrimitiveCategory[count];
    maxLengths = new int[count];
    primitiveWritables = new Writable[count];
  }

  /*
   * Initialize one column's array entries.
   */
  private void initEntry(int logicalColumnIndex, int projectionColumnNum, TypeInfo typeInfo) {
    projectionColumnNums[logicalColumnIndex] = projectionColumnNum;
    Category category = typeInfo.getCategory();
    categories[logicalColumnIndex] = category;
    if (category == Category.PRIMITIVE) {
      PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
      PrimitiveCategory primitiveCategory = primitiveTypeInfo.getPrimitiveCategory();
      primitiveCategories[logicalColumnIndex] = primitiveCategory;

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

      primitiveWritables[logicalColumnIndex] =
          VectorizedBatchUtil.getPrimitiveWritable(primitiveCategory);
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

      int projectionColumnNum = projectedColumns[i];

      TypeInfo typeInfo = typeInfos[i];

      initEntry(i, projectionColumnNum, typeInfo);
    }
  }

  /*
   * Initialize using data type names.
   * No projection -- the column range 0 .. types.size()-1
   */
  public void init(List<String> typeNames) throws HiveException {

    final int count = typeNames.size();
    allocateArrays(count);

    for (int i = 0; i < count; i++) {

      TypeInfo typeInfo =
          TypeInfoUtils.getTypeInfoFromTypeString(typeNames.get(i));

      initEntry(i, i, typeInfo);
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
  public Object extractRowColumn(VectorizedRowBatch batch, int batchIndex, int logicalColumnIndex) {
    final int projectionColumnNum = projectionColumnNums[logicalColumnIndex];
    ColumnVector colVector = batch.cols[projectionColumnNum];
    if (colVector == null) {
      // The planner will not include unneeded columns for reading but other parts of execution
      // may ask for them..
      return null;
    }
    int adjustedIndex = (colVector.isRepeating ? 0 : batchIndex);
    if (!colVector.noNulls && colVector.isNull[adjustedIndex]) {
      return null;
    }

    Category category = categories[logicalColumnIndex];
    switch (category) {
    case PRIMITIVE:
      {
        Writable primitiveWritable =
            primitiveWritables[logicalColumnIndex];
        PrimitiveCategory primitiveCategory = primitiveCategories[logicalColumnIndex];
        switch (primitiveCategory) {
        case VOID:
          return null;
        case BOOLEAN:
          ((BooleanWritable) primitiveWritable).set(
              ((LongColumnVector) batch.cols[projectionColumnNum]).vector[adjustedIndex] == 0 ?
                  false : true);
          return primitiveWritable;
        case BYTE:
          ((ByteWritable) primitiveWritable).set(
              (byte) ((LongColumnVector) batch.cols[projectionColumnNum]).vector[adjustedIndex]);
          return primitiveWritable;
        case SHORT:
          ((ShortWritable) primitiveWritable).set(
              (short) ((LongColumnVector) batch.cols[projectionColumnNum]).vector[adjustedIndex]);
          return primitiveWritable;
        case INT:
          ((IntWritable) primitiveWritable).set(
              (int) ((LongColumnVector) batch.cols[projectionColumnNum]).vector[adjustedIndex]);
          return primitiveWritable;
        case LONG:
          ((LongWritable) primitiveWritable).set(
              ((LongColumnVector) batch.cols[projectionColumnNum]).vector[adjustedIndex]);
          return primitiveWritable;
        case TIMESTAMP:
          ((TimestampWritable) primitiveWritable).set(
              ((TimestampColumnVector) batch.cols[projectionColumnNum]).asScratchTimestamp(adjustedIndex));
          return primitiveWritable;
        case DATE:
          ((DateWritable) primitiveWritable).set(
              (int) ((LongColumnVector) batch.cols[projectionColumnNum]).vector[adjustedIndex]);
          return primitiveWritable;
        case FLOAT:
          ((FloatWritable) primitiveWritable).set(
              (float) ((DoubleColumnVector) batch.cols[projectionColumnNum]).vector[adjustedIndex]);
          return primitiveWritable;
        case DOUBLE:
          ((DoubleWritable) primitiveWritable).set(
              ((DoubleColumnVector) batch.cols[projectionColumnNum]).vector[adjustedIndex]);
          return primitiveWritable;
        case BINARY:
          {
            BytesColumnVector bytesColVector =
                ((BytesColumnVector) batch.cols[projectionColumnNum]);
            byte[] bytes = bytesColVector.vector[adjustedIndex];
            int start = bytesColVector.start[adjustedIndex];
            int length = bytesColVector.length[adjustedIndex];

            if (bytes == null) {
              LOG.info("null binary entry: batchIndex " + batchIndex + " projection column num " + projectionColumnNum);
            }

            BytesWritable bytesWritable = (BytesWritable) primitiveWritable;
            bytesWritable.set(bytes, start, length);
            return primitiveWritable;
          }
        case STRING:
          {
            BytesColumnVector bytesColVector =
                ((BytesColumnVector) batch.cols[projectionColumnNum]);
            byte[] bytes = bytesColVector.vector[adjustedIndex];
            int start = bytesColVector.start[adjustedIndex];
            int length = bytesColVector.length[adjustedIndex];

            if (bytes == null) {
              nullBytesReadError(primitiveCategory, batchIndex, projectionColumnNum);
            }

            // Use org.apache.hadoop.io.Text as our helper to go from byte[] to String.
            ((Text) primitiveWritable).set(bytes, start, length);
            return primitiveWritable;
          }
        case VARCHAR:
          {
            BytesColumnVector bytesColVector =
                ((BytesColumnVector) batch.cols[projectionColumnNum]);
            byte[] bytes = bytesColVector.vector[adjustedIndex];
            int start = bytesColVector.start[adjustedIndex];
            int length = bytesColVector.length[adjustedIndex];

            if (bytes == null) {
              nullBytesReadError(primitiveCategory, batchIndex, projectionColumnNum);
            }

            int adjustedLength = StringExpr.truncate(bytes, start, length,
                maxLengths[logicalColumnIndex]);

            HiveVarcharWritable hiveVarcharWritable = (HiveVarcharWritable) primitiveWritable;
            hiveVarcharWritable.set(new String(bytes, start, adjustedLength, Charsets.UTF_8), -1);
            return primitiveWritable;
          }
        case CHAR:
          {
            BytesColumnVector bytesColVector =
                ((BytesColumnVector) batch.cols[projectionColumnNum]);
            byte[] bytes = bytesColVector.vector[adjustedIndex];
            int start = bytesColVector.start[adjustedIndex];
            int length = bytesColVector.length[adjustedIndex];

            if (bytes == null) {
              nullBytesReadError(primitiveCategory, batchIndex, projectionColumnNum);
            }

            int adjustedLength = StringExpr.rightTrimAndTruncate(bytes, start, length,
                maxLengths[logicalColumnIndex]);

            HiveCharWritable hiveCharWritable = (HiveCharWritable) primitiveWritable;
            hiveCharWritable.set(new String(bytes, start, adjustedLength, Charsets.UTF_8),
                maxLengths[logicalColumnIndex]);
            return primitiveWritable;
          }
        case DECIMAL:
          // The HiveDecimalWritable set method will quickly copy the deserialized decimal writable fields.
          ((HiveDecimalWritable) primitiveWritable).set(
              ((DecimalColumnVector) batch.cols[projectionColumnNum]).vector[adjustedIndex]);
          return primitiveWritable;
        case INTERVAL_YEAR_MONTH:
          ((HiveIntervalYearMonthWritable) primitiveWritable).set(
              (int) ((LongColumnVector) batch.cols[projectionColumnNum]).vector[adjustedIndex]);
          return primitiveWritable;
        case INTERVAL_DAY_TIME:
          ((HiveIntervalDayTimeWritable) primitiveWritable).set(
              ((IntervalDayTimeColumnVector) batch.cols[projectionColumnNum]).asScratchIntervalDayTime(adjustedIndex));
          return primitiveWritable;
        default:
          throw new RuntimeException("Primitive category " + primitiveCategory.name() +
              " not supported");
        }
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

  private void nullBytesReadError(PrimitiveCategory primitiveCategory, int batchIndex,
    int projectionColumnNum) {
    throw new RuntimeException("null " + primitiveCategory.name() +
        " entry: batchIndex " + batchIndex + " projection column num " + projectionColumnNum);
  }
}
