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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.fast.SerializeWrite;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;

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

  private TypeInfo[] typeInfos;

  private ObjectInspector[] objectInspectors;

  private int[] outputColumnNums;

  private VectorExtractRow vectorExtractRow;

  public VectorSerializeRow(T serializeWrite) {
    this();
    this.serializeWrite = serializeWrite;
    vectorExtractRow = new VectorExtractRow();
  }

  // Not public since we must have the serialize write object.
  private VectorSerializeRow() {
  }

  public void init(List<String> typeNames, int[] columnMap) throws HiveException {

    final int size = typeNames.size();
    typeInfos = new TypeInfo[size];
    outputColumnNums = Arrays.copyOf(columnMap, size);
    objectInspectors = new ObjectInspector[size];
    for (int i = 0; i < size; i++) {
      final TypeInfo typeInfo =
          TypeInfoUtils.getTypeInfoFromTypeString(typeNames.get(i));
      typeInfos[i] = typeInfo;
      objectInspectors[i] =
          TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
    }

    vectorExtractRow.init(typeInfos, outputColumnNums);
  }

  public void init(List<String> typeNames) throws HiveException {

    final int size = typeNames.size();
    typeInfos = new TypeInfo[size];
    outputColumnNums = new int[size];
    objectInspectors = new ObjectInspector[size];
    for (int i = 0; i < size; i++) {
      final TypeInfo typeInfo =
          TypeInfoUtils.getTypeInfoFromTypeString(typeNames.get(i));
      typeInfos[i] = typeInfo;
      objectInspectors[i] =
          TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
      outputColumnNums[i] = i;
    }

    vectorExtractRow.init(typeInfos);
  }

  public void init(TypeInfo[] typeInfos, int[] columnMap)
      throws HiveException {

    final int size = typeInfos.length;
    this.typeInfos = Arrays.copyOf(typeInfos, size);
    outputColumnNums = Arrays.copyOf(columnMap, size);
    objectInspectors = new ObjectInspector[size];
    for (int i = 0; i < size; i++) {
      objectInspectors[i] =
          TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfos[i]);
    }

    vectorExtractRow.init(this.typeInfos, outputColumnNums);
  }

  public int getCount() {
    return typeInfos.length;
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
    for (int i = 0; i < typeInfos.length; i++) {
      final ColumnVector colVector = batch.cols[outputColumnNums[i]];
      serializeWrite(colVector, typeInfos[i], objectInspectors[i], batchIndex);
    }
  }

  private void serializeWrite(
      ColumnVector colVector, TypeInfo typeInfo,
      ObjectInspector objectInspector, int batchIndex) throws IOException {

    int adjustedBatchIndex;
    if (colVector.isRepeating) {
      adjustedBatchIndex = 0;
    } else {
      adjustedBatchIndex = batchIndex;
    }
    if (!colVector.noNulls && colVector.isNull[adjustedBatchIndex]) {
      serializeWrite.writeNull();
      hasAnyNulls = true;
      return;
    }
    isAllNulls = false;

    final Category category = typeInfo.getCategory();
    switch (category) {
    case PRIMITIVE:
      serializePrimitiveWrite(colVector, (PrimitiveTypeInfo) typeInfo, adjustedBatchIndex);
      break;
    case LIST:
      serializeListWrite(
          (ListColumnVector) colVector,
          (ListTypeInfo) typeInfo,
          (ListObjectInspector) objectInspector,
          adjustedBatchIndex);
      break;
    case MAP:
      serializeMapWrite(
          (MapColumnVector) colVector,
          (MapTypeInfo) typeInfo,
          (MapObjectInspector) objectInspector,
          adjustedBatchIndex);
      break;
    case STRUCT:
      serializeStructWrite(
          (StructColumnVector) colVector,
          (StructTypeInfo) typeInfo,
          (StructObjectInspector) objectInspector,
          adjustedBatchIndex);
      break;
    case UNION:
      serializeUnionWrite(
          (UnionColumnVector) colVector,
          (UnionTypeInfo) typeInfo,
          (UnionObjectInspector) objectInspector,
          adjustedBatchIndex);
      break;
    default:
      throw new RuntimeException("Unexpected category " + category);
    }
  }

  private void serializeUnionWrite(
      UnionColumnVector colVector, UnionTypeInfo typeInfo,
      UnionObjectInspector objectInspector, int adjustedBatchIndex) throws IOException {

    final byte tag = (byte) colVector.tags[adjustedBatchIndex];
    final ColumnVector fieldColumnVector = colVector.fields[tag];
    final TypeInfo objectTypeInfo = typeInfo.getAllUnionObjectTypeInfos().get(tag);

    serializeWrite.beginUnion(tag);
    serializeWrite(
        fieldColumnVector,
        objectTypeInfo,
        objectInspector.getObjectInspectors().get(tag),
        adjustedBatchIndex);
    serializeWrite.finishUnion();
  }

  private void serializeStructWrite(
      StructColumnVector colVector, StructTypeInfo typeInfo,
      StructObjectInspector objectInspector, int adjustedBatchIndex) throws IOException {

    final ColumnVector[] fieldColumnVectors = colVector.fields;
    final List<TypeInfo> fieldTypeInfos = typeInfo.getAllStructFieldTypeInfos();
    final List<? extends StructField> structFields = objectInspector.getAllStructFieldRefs();
    final int size = fieldTypeInfos.size();

    final List list = (List) vectorExtractRow.extractRowColumn(
        colVector, typeInfo, objectInspector, adjustedBatchIndex);

    serializeWrite.beginStruct(list);
    for (int i = 0; i < size; i++) {
      if (i > 0) {
        serializeWrite.separateStruct();
      }
      serializeWrite(
          fieldColumnVectors[i],
          fieldTypeInfos.get(i),
          structFields.get(i).getFieldObjectInspector(),
          adjustedBatchIndex);
    }
    serializeWrite.finishStruct();
  }

  private void serializeMapWrite(
      MapColumnVector colVector, MapTypeInfo typeInfo,
      MapObjectInspector objectInspector, int adjustedBatchIndex) throws IOException {

    final ColumnVector keyColumnVector = colVector.keys;
    final ColumnVector valueColumnVector = colVector.values;
    final TypeInfo keyTypeInfo = typeInfo.getMapKeyTypeInfo();
    final TypeInfo valueTypeInfo = typeInfo.getMapValueTypeInfo();
    final int offset = (int) colVector.offsets[adjustedBatchIndex];
    final int size = (int) colVector.lengths[adjustedBatchIndex];

    final Map map = (Map) vectorExtractRow.extractRowColumn(
        colVector, typeInfo, objectInspector, adjustedBatchIndex);

    serializeWrite.beginMap(map);
    for (int i = 0; i < size; i++) {
      if (i > 0) {
        serializeWrite.separateKeyValuePair();
      }
      serializeWrite(keyColumnVector, keyTypeInfo,
          objectInspector.getMapKeyObjectInspector(), offset + i);
      serializeWrite.separateKey();
      serializeWrite(valueColumnVector, valueTypeInfo,
          objectInspector.getMapValueObjectInspector(), offset + i);
    }
    serializeWrite.finishMap();
  }

  private void serializeListWrite(
      ListColumnVector colVector, ListTypeInfo typeInfo,
      ListObjectInspector objectInspector, int adjustedBatchIndex) throws IOException {

    final ColumnVector childColumnVector = colVector.child;
    final TypeInfo elementTypeInfo = typeInfo.getListElementTypeInfo();
    final int offset = (int) colVector.offsets[adjustedBatchIndex];
    final int size = (int) colVector.lengths[adjustedBatchIndex];

    final ObjectInspector elementObjectInspector = objectInspector.getListElementObjectInspector();
    final List list = (List) vectorExtractRow.extractRowColumn(
        colVector, typeInfo, objectInspector, adjustedBatchIndex);

    serializeWrite.beginList(list);
    for (int i = 0; i < size; i++) {
      if (i > 0) {
        serializeWrite.separateList();
      }
      serializeWrite(
          childColumnVector, elementTypeInfo, elementObjectInspector, offset + i);
    }
    serializeWrite.finishList();
  }

  private void serializePrimitiveWrite(
      ColumnVector colVector, PrimitiveTypeInfo typeInfo, int adjustedBatchIndex) throws IOException {

    final PrimitiveCategory primitiveCategory = typeInfo.getPrimitiveCategory();
    switch (primitiveCategory) {
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
        final BytesColumnVector bytesColVector = (BytesColumnVector) colVector;
        serializeWrite.writeString(
            bytesColVector.vector[adjustedBatchIndex],
            bytesColVector.start[adjustedBatchIndex],
            bytesColVector.length[adjustedBatchIndex]);
      }
      break;
    case BINARY:
      {
        final BytesColumnVector bytesColVector = (BytesColumnVector) colVector;
        serializeWrite.writeBinary(
            bytesColVector.vector[adjustedBatchIndex],
            bytesColVector.start[adjustedBatchIndex],
            bytesColVector.length[adjustedBatchIndex]);
      }
      break;
    case DECIMAL:
      {
        final DecimalColumnVector decimalColVector = (DecimalColumnVector) colVector;
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
      throw new RuntimeException("Unexpected primitive category " + primitiveCategory);
    }
  }

  public boolean getHasAnyNulls() {
    return hasAnyNulls;
  }

  public boolean getIsAllNulls() {
    return isAllNulls;
  }
}