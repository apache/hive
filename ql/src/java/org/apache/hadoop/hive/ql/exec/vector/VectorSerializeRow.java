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

  private Field root;

  private static class Field {
    Field[] children;

    boolean isPrimitive;
    Category category;
    PrimitiveCategory primitiveCategory;
    TypeInfo typeInfo;

    int count;

    ObjectInspector objectInspector;
    int outputColumnNum;

    Field() {
      children = null;
      isPrimitive = false;
      category = null;
      primitiveCategory = null;
      typeInfo = null;
      count = 0;
      objectInspector = null;
      outputColumnNum = -1;
    }
  }

  private VectorExtractRow vectorExtractRow;

  public VectorSerializeRow(T serializeWrite) {
    this();
    this.serializeWrite = serializeWrite;
    vectorExtractRow = new VectorExtractRow();
  }

  // Not public since we must have the serialize write object.
  private VectorSerializeRow() {
  }

  private Field[] createFields(TypeInfo[] typeInfos) {
    final Field[] children = new Field[typeInfos.length];
    for (int i = 0; i < typeInfos.length; i++) {
      children[i] = createField(typeInfos[i]);
    }
    return children;
  }

  private Field createField(TypeInfo typeInfo) {
    final Field field = new Field();
    final Category category = typeInfo.getCategory();
    field.category = category;
    field.typeInfo = typeInfo;
    if (category == Category.PRIMITIVE) {
      field.isPrimitive = true;
      field.primitiveCategory = ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
    } else {
      field.isPrimitive = false;
      field.objectInspector =
          TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
      switch (category) {
      case LIST:
        field.children = new Field[1];
        field.children[0] = createField(((ListTypeInfo) typeInfo).getListElementTypeInfo());
        break;
      case MAP:
        field.children = new Field[2];
        field.children[0] = createField(((MapTypeInfo) typeInfo).getMapKeyTypeInfo());
        field.children[1] = createField(((MapTypeInfo) typeInfo).getMapValueTypeInfo());
        break;
      case STRUCT:
        StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
        List<TypeInfo> fieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
        field.children = createFields(fieldTypeInfos.toArray(new TypeInfo[fieldTypeInfos.size()]));
        break;
      case UNION:
        UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
        List<TypeInfo> objectTypeInfos = unionTypeInfo.getAllUnionObjectTypeInfos();
        field.children = createFields(objectTypeInfos.toArray(new TypeInfo[objectTypeInfos.size()]));
        break;
      default:
        throw new RuntimeException();
      }
      field.count = field.children.length;
    }
    return field;
  }

  public void init(List<String> typeNames, int[] columnMap) throws HiveException {

    TypeInfo[] typeInfos =
        TypeInfoUtils.typeInfosFromTypeNames(typeNames).toArray(new TypeInfo[typeNames.size()]);

    final int count = typeInfos.length;

    root = new Field();
    root.isPrimitive = false;
    root.category = Category.STRUCT;
    root.children = createFields(typeInfos);
    root.count = count;
    root.objectInspector = null;
    int[] outputColumnNums = new int[count];
    for (int i = 0; i < count; i++) {
      final int outputColumnNum = columnMap[i];
      outputColumnNums[i] = outputColumnNum;
      root.children[i].outputColumnNum = outputColumnNum;
    }

    vectorExtractRow.init(typeInfos, outputColumnNums);
  }

  public void init(List<String> typeNames) throws HiveException {

    TypeInfo[] typeInfos =
        TypeInfoUtils.typeInfosFromTypeNames(typeNames).toArray(new TypeInfo[typeNames.size()]);

    final int count = typeInfos.length;

    root = new Field();
    root.isPrimitive = false;
    root.category = Category.STRUCT;
    root.children = createFields(typeInfos);
    root.count = count;
    root.objectInspector = null;
    for (int i = 0; i < count; i++) {
      root.children[i].outputColumnNum = i;
    }

    vectorExtractRow.init(typeInfos);
  }

  public void init(TypeInfo[] typeInfos)
      throws HiveException {

    final int count = typeInfos.length;

    root = new Field();
    root.isPrimitive = false;
    root.category = Category.STRUCT;
    root.children = createFields(typeInfos);
    root.count = count;
    root.objectInspector = null;
    for (int i = 0; i < count; i++) {
      root.children[i].outputColumnNum = i;
    }

    vectorExtractRow.init(typeInfos);
  }

  public void init(TypeInfo[] typeInfos, int[] columnMap)
      throws HiveException {

    final int count = typeInfos.length;

    root = new Field();
    root.isPrimitive = false;
    root.category = Category.STRUCT;
    root.children = createFields(typeInfos);
    root.count = count;
    root.objectInspector = null;
    int[] outputColumnNums = new int[count];
    for (int i = 0; i < count; i++) {
      final int outputColumnNum = columnMap[i];
      outputColumnNums[i] = outputColumnNum;
      root.children[i].outputColumnNum = outputColumnNum;
    }

    vectorExtractRow.init(typeInfos, outputColumnNums);
  }

  public int getCount() {
    return root.count;
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
    final Field[] children = root.children;
    final int size = root.count;
    for (int i = 0; i < size; i++) {
      final Field field = children[i];
      final ColumnVector colVector = batch.cols[field.outputColumnNum];
      serializeWrite(colVector, field, batchIndex);
    }
  }

  private void serializeWrite(
      ColumnVector colVector, Field field, int batchIndex) throws IOException {

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

    if (field.isPrimitive) {
      serializePrimitiveWrite(colVector, field, adjustedBatchIndex);
      return;
    }
    final Category category = field.category;
    switch (category) {
    case LIST:
      serializeListWrite(
          (ListColumnVector) colVector,
          field,
          adjustedBatchIndex);
      break;
    case MAP:
      serializeMapWrite(
          (MapColumnVector) colVector,
          field,
          adjustedBatchIndex);
      break;
    case STRUCT:
      serializeStructWrite(
          (StructColumnVector) colVector,
          field,
          adjustedBatchIndex);
      break;
    case UNION:
      serializeUnionWrite(
          (UnionColumnVector) colVector,
          field,
          adjustedBatchIndex);
      break;
    default:
      throw new RuntimeException("Unexpected category " + category);
    }
  }

  private void serializeUnionWrite(
      UnionColumnVector colVector, Field field, int adjustedBatchIndex) throws IOException {

    UnionTypeInfo typeInfo = (UnionTypeInfo) field.typeInfo;
    UnionObjectInspector objectInspector = (UnionObjectInspector) field.objectInspector;

    final byte tag = (byte) colVector.tags[adjustedBatchIndex];
    final ColumnVector fieldColumnVector = colVector.fields[tag];
    final Field childField = field.children[tag];

    serializeWrite.beginUnion(tag);
    serializeWrite(
        fieldColumnVector,
        childField,
        adjustedBatchIndex);
    serializeWrite.finishUnion();
  }

  private void serializeStructWrite(
      StructColumnVector colVector, Field field, int adjustedBatchIndex) throws IOException {

    StructTypeInfo typeInfo = (StructTypeInfo) field.typeInfo;
    StructObjectInspector objectInspector = (StructObjectInspector) field.objectInspector;

    final ColumnVector[] fieldColumnVectors = colVector.fields;
    final Field[] children = field.children;
    final List<? extends StructField> structFields = objectInspector.getAllStructFieldRefs();
    final int size = field.count;

    final List list = (List) vectorExtractRow.extractRowColumn(
        colVector, typeInfo, objectInspector, adjustedBatchIndex);

    serializeWrite.beginStruct(list);
    for (int i = 0; i < size; i++) {
      if (i > 0) {
        serializeWrite.separateStruct();
      }
      serializeWrite(
          fieldColumnVectors[i],
          children[i],
          adjustedBatchIndex);
    }
    serializeWrite.finishStruct();
  }

  private void serializeMapWrite(
      MapColumnVector colVector, Field field, int adjustedBatchIndex) throws IOException {

    MapTypeInfo typeInfo = (MapTypeInfo) field.typeInfo;
    MapObjectInspector objectInspector = (MapObjectInspector) field.objectInspector;

    final ColumnVector keyColumnVector = colVector.keys;
    final ColumnVector valueColumnVector = colVector.values;
    final Field keyField = field.children[0];
    final Field valueField = field.children[1];
    final int offset = (int) colVector.offsets[adjustedBatchIndex];
    final int size = (int) colVector.lengths[adjustedBatchIndex];

    final Map map = (Map) vectorExtractRow.extractRowColumn(
        colVector, typeInfo, objectInspector, adjustedBatchIndex);

    serializeWrite.beginMap(map);
    for (int i = 0; i < size; i++) {
      if (i > 0) {
        serializeWrite.separateKeyValuePair();
      }
      serializeWrite(keyColumnVector, keyField, offset + i);
      serializeWrite.separateKey();
      serializeWrite(valueColumnVector, valueField, offset + i);
    }
    serializeWrite.finishMap();
  }

  private void serializeListWrite(
      ListColumnVector colVector, Field field, int adjustedBatchIndex) throws IOException {

    final ListTypeInfo typeInfo = (ListTypeInfo) field.typeInfo;
    final ListObjectInspector objectInspector = (ListObjectInspector) field.objectInspector;

    final ColumnVector childColumnVector = colVector.child;
    final Field elementField = field.children[0];
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
          childColumnVector, elementField, offset + i);
    }
    serializeWrite.finishList();
  }

  private void serializePrimitiveWrite(
      ColumnVector colVector, Field field, int adjustedBatchIndex) throws IOException {

    final PrimitiveCategory primitiveCategory = field.primitiveCategory;
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
        if (colVector instanceof Decimal64ColumnVector) {
          final Decimal64ColumnVector decimal64ColVector = (Decimal64ColumnVector) colVector;
          serializeWrite.writeDecimal64(decimal64ColVector.vector[adjustedBatchIndex], decimal64ColVector.scale);
        } else {
          final DecimalColumnVector decimalColVector = (DecimalColumnVector) colVector;
          serializeWrite.writeHiveDecimal(decimalColVector.vector[adjustedBatchIndex], decimalColVector.scale);
        }
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