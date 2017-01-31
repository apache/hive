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
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.fast.SerializeWrite;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * This class serializes columns from a row in a VectorizedRowBatch into a serialization format.
 *
 * The caller provides the hive type names and column numbers in the order desired to
 * serialize.
 *
 * This class uses an provided SerializeWrite object to directly serialize by writing
 * field-by-field into a serialization format from the primitive values of the VectorizedRowBatch.
 *
 * Note that when serializing a row, the logical mapping using selected in use has already
 * been performed.
 */
public final class VectorSerializeRow<T extends SerializeWrite> {

  private T serializeWrite;

  private Category[] categories;
  private PrimitiveCategory[] primitiveCategories;

  private int[] outputColumnNums;

  public VectorSerializeRow(T serializeWrite) {
    this();
    this.serializeWrite = serializeWrite;
  }

  // Not public since we must have the serialize write object.
  private VectorSerializeRow() {
  }

  public void init(List<String> typeNames, int[] columnMap) throws HiveException {

    final int size = typeNames.size();
    categories = new Category[size];
    primitiveCategories = new PrimitiveCategory[size];
    outputColumnNums = Arrays.copyOf(columnMap, size);
    TypeInfo typeInfo;
    for (int i = 0; i < size; i++) {
      typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeNames.get(i));
      categories[i] = typeInfo.getCategory();
      if (categories[i] == Category.PRIMITIVE) {
        primitiveCategories[i] = ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
      }
    }
  }

  public void init(List<String> typeNames) throws HiveException {

    final int size = typeNames.size();
    categories = new Category[size];
    primitiveCategories = new PrimitiveCategory[size];
    outputColumnNums = new int[size];
    TypeInfo typeInfo;
    for (int i = 0; i < size; i++) {
      typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeNames.get(i));
      categories[i] = typeInfo.getCategory();
      if (categories[i] == Category.PRIMITIVE) {
        primitiveCategories[i] = ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
      }
      outputColumnNums[i] = i;
    }
  }

  public void init(TypeInfo[] typeInfos, int[] columnMap)
      throws HiveException {

    final int size = typeInfos.length;
    categories = new Category[size];
    primitiveCategories = new PrimitiveCategory[size];
    outputColumnNums = Arrays.copyOf(columnMap, size);
    TypeInfo typeInfo;
    for (int i = 0; i < typeInfos.length; i++) {
      typeInfo = typeInfos[i];
      categories[i] = typeInfo.getCategory();
      if (categories[i] == Category.PRIMITIVE) {
        primitiveCategories[i] = ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
      }
    }
  }

  public int getCount() {
    return categories.length;
  }

  public void setOutput(Output output) {
    serializeWrite.set(output);
  }

  public void setOutputAppend(Output output) {
    serializeWrite.setAppend(output);
  }

  private boolean hasAnyNulls;
  private boolean isAllNulls;

  /*
   * Note that when serializing a row, the logical mapping using selected in use has already
   * been performed.  batchIndex is the actual index of the row.
   */
  public void serializeWrite(VectorizedRowBatch batch, int batchIndex) throws IOException {

    hasAnyNulls = false;
    isAllNulls = true;
    ColumnVector colVector;
    int adjustedBatchIndex;
    final int size = categories.length;
    for (int i = 0; i < size; i++) {
      colVector = batch.cols[outputColumnNums[i]];
      if (colVector.isRepeating) {
        adjustedBatchIndex = 0;
      } else {
        adjustedBatchIndex = batchIndex;
      }
      if (!colVector.noNulls && colVector.isNull[adjustedBatchIndex]) {
        serializeWrite.writeNull();
        hasAnyNulls = true;
        continue;
      }
      isAllNulls = false;
      switch (categories[i]) {
      case PRIMITIVE:
        switch (primitiveCategories[i]) {
        case BOOLEAN:
          serializeWrite.writeBoolean(((LongColumnVector) colVector).vector[adjustedBatchIndex] != 0);
          break;
        case BYTE:
          serializeWrite.writeByte((byte) ((LongColumnVector) colVector).vector[adjustedBatchIndex]);
          break;
        case SHORT:
          serializeWrite.writeShort((short) ((LongColumnVector) colVector).vector[adjustedBatchIndex]);
          break;
        case INT:
          serializeWrite.writeInt((int) ((LongColumnVector) colVector).vector[adjustedBatchIndex]);
          break;
        case LONG:
          serializeWrite.writeLong(((LongColumnVector) colVector).vector[adjustedBatchIndex]);
          break;
        case DATE:
          serializeWrite.writeDate((int) ((LongColumnVector) colVector).vector[adjustedBatchIndex]);
          break;
        case TIMESTAMP:
          serializeWrite.writeTimestamp(((TimestampColumnVector) colVector).asScratchTimestamp(adjustedBatchIndex));
          break;
        case FLOAT:
          serializeWrite.writeFloat((float) ((DoubleColumnVector) colVector).vector[adjustedBatchIndex]);
          break;
        case DOUBLE:
          serializeWrite.writeDouble(((DoubleColumnVector) colVector).vector[adjustedBatchIndex]);
          break;
        case STRING:
        case CHAR:
        case VARCHAR:
          {
            // We store CHAR and VARCHAR without pads, so write with STRING.
            BytesColumnVector bytesColVector = (BytesColumnVector) colVector;
            serializeWrite.writeString(
                bytesColVector.vector[adjustedBatchIndex],
                bytesColVector.start[adjustedBatchIndex],
                bytesColVector.length[adjustedBatchIndex]);
          }
          break;
        case BINARY:
          {
            BytesColumnVector bytesColVector = (BytesColumnVector) colVector;
            serializeWrite.writeBinary(
                bytesColVector.vector[adjustedBatchIndex],
                bytesColVector.start[adjustedBatchIndex],
                bytesColVector.length[adjustedBatchIndex]);
          }
          break;
        case DECIMAL:
          {
            DecimalColumnVector decimalColVector = (DecimalColumnVector) colVector;
            serializeWrite.writeHiveDecimal(decimalColVector.vector[adjustedBatchIndex], decimalColVector.scale);
          }
          break;
        case INTERVAL_YEAR_MONTH:
          serializeWrite.writeHiveIntervalYearMonth((int) ((LongColumnVector) colVector).vector[adjustedBatchIndex]);
          break;
        case INTERVAL_DAY_TIME:
          serializeWrite.writeHiveIntervalDayTime(((IntervalDayTimeColumnVector) colVector).asScratchIntervalDayTime(adjustedBatchIndex));
          break;
        default:
          throw new RuntimeException("Unexpected primitive category " + primitiveCategories[i]);
        }
        break;
      default:
        throw new RuntimeException("Unexpected category " + categories[i]);
      }
    }
  }

  public boolean getHasAnyNulls() {
    return hasAnyNulls;
  }

  public boolean getIsAllNulls() {
    return isAllNulls;
  }
}