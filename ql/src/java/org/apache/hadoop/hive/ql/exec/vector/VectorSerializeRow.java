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

import org.apache.hadoop.hive.common.type.Timestamp;
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
    Field[] children = null;
    boolean isPrimitive = false;
    Category category = null;
    PrimitiveCategory primitiveCategory = null;
    TypeInfo typeInfo = null;
    int count = 0;
    ObjectInspector objectInspector = null;
    int outputColumnNum = -1;
    VectorSerializeWriter writer = null;
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
      switch (field.primitiveCategory) {
        case BOOLEAN:
          field.writer = new VectorSerializeBooleanWriter();
          break;
        case BYTE:
          field.writer = new VectorSerializeByteWriter();
          break;
        case SHORT:
          field.writer = new VectorSerializeShortWriter();
          break;
        case INT:
          field.writer = new VectorSerializeIntWriter();
          break;
        case LONG:
          field.writer = new VectorSerializeLongWriter();
          break;
        case DATE:
          field.writer = new VectorSerializeDateWriter();
          break;
        case TIMESTAMP:
          field.writer = new VectorSerializeTimestampWriter();
          break;
        case FLOAT:
          field.writer = new VectorSerializeFloatWriter();
          break;
        case DOUBLE:
          field.writer = new VectorSerializeDoubleWriter();
          break;
        case STRING:
        case CHAR:
        case VARCHAR:
          field.writer = new VectorSerializeStringWriter();
          break;
        case BINARY:
          field.writer = new VectorSerializeBinaryWriter();
          break;
        case DECIMAL:
          field.writer = new VectorSerializeDecimalWriter();
          break;
        case INTERVAL_YEAR_MONTH:
          field.writer = new VectorSerializeHiveIntervalYearMonthWriter();
          break;
        case INTERVAL_DAY_TIME:
          field.writer = new VectorSerializeHiveIntervalDayTimeWriter();
          break;
        default:
          throw new RuntimeException("Unexpected primitive category " + field.primitiveCategory);
      }
    } else {
      field.isPrimitive = false;
      field.objectInspector =
          TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
      switch (category) {
      case LIST:
        field.children = new Field[1];
        field.children[0] = createField(((ListTypeInfo) typeInfo).getListElementTypeInfo());
        field.writer = new VectorSerializeListWriter();
        break;
      case MAP:
        field.children = new Field[2];
        field.children[0] = createField(((MapTypeInfo) typeInfo).getMapKeyTypeInfo());
        field.children[1] = createField(((MapTypeInfo) typeInfo).getMapValueTypeInfo());
        field.writer = new VectorSerializeMapWriter();
        break;
      case STRUCT:
        StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
        List<TypeInfo> fieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
        field.children = createFields(fieldTypeInfos.toArray(new TypeInfo[fieldTypeInfos.size()]));
        field.writer = new VectorSerializeStructWriter();
        break;
      case UNION:
        UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
        List<TypeInfo> objectTypeInfos = unionTypeInfo.getAllUnionObjectTypeInfos();
        field.children = createFields(objectTypeInfos.toArray(new TypeInfo[objectTypeInfos.size()]));
        field.writer = new VectorSerializeUnionWriter();
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
    field.writer.serialize(colVector, field, adjustedBatchIndex);
  }

  abstract static class VectorSerializeWriter {
    abstract void serialize(Object colVector, Field field, int adjustedBatchIndex) throws IOException;
  }

  class VectorSerializeUnionWriter extends VectorSerializeWriter {
    @Override
    void serialize(Object colInfo, Field field, int adjustedBatchIndex) throws IOException {
      serializeUnionWrite((UnionColumnVector)colInfo, field, adjustedBatchIndex);
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
  }

  class VectorSerializeStructWriter extends VectorSerializeWriter {
    @Override
    void serialize(Object colInfo, Field field, int adjustedBatchIndex) throws IOException {
      serializeStructWrite((StructColumnVector)colInfo, field, adjustedBatchIndex);
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
  }

  class VectorSerializeMapWriter extends VectorSerializeWriter {
    @Override
    void serialize(Object colInfo, Field field, int adjustedBatchIndex) throws IOException {
      serializeMapWrite((MapColumnVector)colInfo, field, adjustedBatchIndex);
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
  }

  class VectorSerializeListWriter extends VectorSerializeWriter {
    @Override
    void serialize(Object colInfo, Field field, int adjustedBatchIndex) throws IOException {
      serializeListWrite((ListColumnVector)colInfo, field, adjustedBatchIndex);
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
  }

  class VectorSerializeBooleanWriter extends VectorSerializeWriter {
    @Override
    void serialize(Object colInfo, Field field, int adjustedBatchIndex) throws IOException {
      serializeWrite.writeBoolean(
              ((LongColumnVector) colInfo).vector[adjustedBatchIndex] != 0);
    }
  }

  class VectorSerializeByteWriter extends VectorSerializeWriter {
    @Override
    void serialize(Object colInfo, Field field, int adjustedBatchIndex) throws IOException {
      serializeWrite.writeByte(
              (byte) ((LongColumnVector) colInfo).vector[adjustedBatchIndex]);
    }
  }

  class VectorSerializeShortWriter extends VectorSerializeWriter {
    @Override
    void serialize(Object colInfo, Field field, int adjustedBatchIndex) throws IOException {
      serializeWrite.writeShort(
              (short) ((LongColumnVector) colInfo).vector[adjustedBatchIndex]);
    }
  }

  class VectorSerializeIntWriter extends VectorSerializeWriter {
    @Override
    void serialize(Object colInfo, Field field, int adjustedBatchIndex) throws IOException {
      serializeWrite.writeInt(
              (int) ((LongColumnVector) colInfo).vector[adjustedBatchIndex]);
    }
  }

  class VectorSerializeLongWriter extends VectorSerializeWriter {
    @Override
    void serialize(Object colInfo, Field field, int adjustedBatchIndex) throws IOException {
      serializeWrite.writeLong(
              ((LongColumnVector) colInfo).vector[adjustedBatchIndex]);
    }
  }

  class VectorSerializeDateWriter extends VectorSerializeWriter {
    @Override
    void serialize(Object colInfo, Field field, int adjustedBatchIndex) throws IOException {
      serializeWrite.writeDate(
              (int) ((LongColumnVector) colInfo).vector[adjustedBatchIndex]);
    }
  }

  class VectorSerializeTimestampWriter extends VectorSerializeWriter {
    @Override
    void serialize(Object colInfo, Field field, int adjustedBatchIndex) throws IOException {
      // From java.sql.Timestamp used by vectorization to serializable
      // org.apache.hadoop.hive.common.type.Timestamp
      java.sql.Timestamp ts =
              ((TimestampColumnVector) colInfo).asScratchTimestamp(adjustedBatchIndex);
      Timestamp serializableTS = Timestamp.ofEpochMilli(ts.getTime(), ts.getNanos());
      serializeWrite.writeTimestamp(serializableTS);
    }
  }

  class VectorSerializeFloatWriter extends VectorSerializeWriter {
    @Override
    void serialize(Object colInfo, Field field, int adjustedBatchIndex) throws IOException {
      serializeWrite.writeFloat(
              (float) ((DoubleColumnVector) colInfo).vector[adjustedBatchIndex]);
    }
  }

  class VectorSerializeDoubleWriter extends VectorSerializeWriter {
    @Override
    void serialize(Object colInfo, Field field, int adjustedBatchIndex) throws IOException {
      serializeWrite.writeDouble(
              ((DoubleColumnVector) colInfo).vector[adjustedBatchIndex]);
    }
  }

  class VectorSerializeStringWriter extends VectorSerializeWriter {
    @Override
    void serialize(Object colInfo, Field field, int adjustedBatchIndex) throws IOException {
      {
        // We store CHAR and VARCHAR without pads, so write with STRING.
        final BytesColumnVector bytesColVector = (BytesColumnVector) colInfo;
        serializeWrite.writeString(
                bytesColVector.vector[adjustedBatchIndex],
                bytesColVector.start[adjustedBatchIndex],
                bytesColVector.length[adjustedBatchIndex]);
      }
    }
  }

  class VectorSerializeBinaryWriter extends VectorSerializeWriter {
    @Override
    void serialize(Object colInfo, Field field, int adjustedBatchIndex) throws IOException {
      final BytesColumnVector bytesColVector = (BytesColumnVector) colInfo;
      serializeWrite.writeBinary(
              bytesColVector.vector[adjustedBatchIndex],
              bytesColVector.start[adjustedBatchIndex],
              bytesColVector.length[adjustedBatchIndex]);
    }
  }

  class VectorSerializeDecimalWriter extends VectorSerializeWriter {
    @Override
    void serialize(Object colInfo, Field field, int adjustedBatchIndex) throws IOException {
      if (colInfo instanceof Decimal64ColumnVector) {
        final Decimal64ColumnVector decimal64ColVector = (Decimal64ColumnVector) colInfo;
        serializeWrite.writeDecimal64(
                decimal64ColVector.vector[adjustedBatchIndex], decimal64ColVector.scale);
      } else {
        final DecimalColumnVector decimalColVector = (DecimalColumnVector) colInfo;
        serializeWrite.writeHiveDecimal(
                decimalColVector.vector[adjustedBatchIndex], decimalColVector.scale);
      }
    }
  }

  class VectorSerializeHiveIntervalYearMonthWriter extends VectorSerializeWriter {
    @Override
    void serialize(Object colInfo, Field field, int adjustedBatchIndex) throws IOException {
      serializeWrite.writeHiveIntervalYearMonth(
              (int) ((LongColumnVector) colInfo).vector[adjustedBatchIndex]);
    }
  }

  class VectorSerializeHiveIntervalDayTimeWriter extends VectorSerializeWriter {
    @Override
    void serialize(Object colInfo, Field field, int adjustedBatchIndex) throws IOException {
      serializeWrite.writeHiveIntervalDayTime(
              ((IntervalDayTimeColumnVector) colInfo).asScratchIntervalDayTime(adjustedBatchIndex));
    }
  }

  public boolean getHasAnyNulls() {
    return hasAnyNulls;
  }

  public boolean getIsAllNulls() {
    return isAllNulls;
  }
}