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

import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.VectorPartitionConversion;
import org.apache.hadoop.hive.serde2.fast.DeserializeRead;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.SettableListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableUnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

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
  protected DataTypePhysicalVariation[] dataTypePhysicalVariations;

  private byte[] inputBytes;

  /**
   * @param deserializeRead   Set useExternalBuffer to true to avoid buffer copying and to get
   *     more efficient reading.
   */
  public VectorDeserializeRow(T deserializeRead) {
    this();
    this.deserializeRead = deserializeRead;
    sourceTypeInfos = deserializeRead.typeInfos();
    dataTypePhysicalVariations = deserializeRead.getDataTypePhysicalVariations();
  }

  // Not public since we must have the deserialize read object.
  private VectorDeserializeRow() {
  }

  private static class Field {

    private boolean isPrimitive;
 
    private Category category;

    private PrimitiveCategory primitiveCategory;
                  //The data type primitive category of the column being deserialized.

    private DataTypePhysicalVariation dataTypePhysicalVariation;

    private int maxLength;
                  // For the CHAR and VARCHAR data types, the maximum character length of
                  // the column.  Otherwise, 0.

    private boolean isConvert;

    /*
     * This member has information for data type conversion.
     * Not defined if there is no conversion.
     */
    private Object conversionWritable;
                  // Conversion requires source be placed in writable so we can call upon
                  // VectorAssignRow to convert and assign the row column.

    private ComplexTypeHelper complexTypeHelper;
                  // For a complex type, a helper object that describes elements, key/value pairs,
                  // or fields.

    private ObjectInspector objectInspector;

    public Field(PrimitiveCategory primitiveCategory, DataTypePhysicalVariation dataTypePhysicalVariation,
        int maxLength) {
      isPrimitive = true;
      this.category = Category.PRIMITIVE;
      this.primitiveCategory = primitiveCategory;
      this.dataTypePhysicalVariation = dataTypePhysicalVariation;
      this.maxLength = maxLength;
      this.isConvert = false;
      this.conversionWritable = null;
      this.complexTypeHelper = null;
      this.objectInspector = PrimitiveObjectInspectorFactory.
          getPrimitiveWritableObjectInspector(primitiveCategory);
    }

    public Field(Category category, ComplexTypeHelper complexTypeHelper, TypeInfo typeInfo) {
      isPrimitive = false;
      this.category = category;
      this.objectInspector = TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(typeInfo);
      this.primitiveCategory = null;
      this.dataTypePhysicalVariation = null;
      this.maxLength = 0;
      this.isConvert = false;
      this.conversionWritable = null;
      this.complexTypeHelper = complexTypeHelper;
    }

    public boolean getIsPrimitive() {
      return isPrimitive;
    }

    public Category getCategory() {
      return category;
    }

    public PrimitiveCategory getPrimitiveCategory() {
      return primitiveCategory;
    }

    public DataTypePhysicalVariation getDataTypePhysicalVariation() {
      return dataTypePhysicalVariation;
    }

    public void setMaxLength(int maxLength) {
      this.maxLength = maxLength;
    }

    public int getMaxLength() {
      return maxLength;
    }

    public void setIsConvert(boolean isConvert) {
      this.isConvert = isConvert;
    }

    public boolean getIsConvert() {
      return isConvert;
    }

    public void setConversionWritable(Object conversionWritable) {
      this.conversionWritable = conversionWritable;
    }

    public Object getConversionWritable() {
      return conversionWritable;
    }

    public ComplexTypeHelper getComplexHelper() {
      return complexTypeHelper;
    }

    public ObjectInspector getObjectInspector() {
      return objectInspector;
    }
  }

  /*
   * These members have information for deserializing a row into the VectorizedRowBatch
   * columns.
   *
   * We say "source" because when there is conversion we are converting th deserialized source into
   * a target data type.
   */

  private boolean useReadField;
                // True when the (random access) readField method of DeserializeRead are being used.

  private int[] readFieldLogicalIndices;
                // The logical indices for reading with readField.

  private int[] projectionColumnNums;
                // Assigning can be a subset of columns, so this is the projection --
                // the batch column numbers.

  private Field topLevelFields[];

  private VectorAssignRow convertVectorAssignRow;
                // Use its conversion ability.

  /*
   * Allocate the source deserialization related arrays.
   */
  private void allocateArrays(int count) {
    projectionColumnNums = new int[count];
    Arrays.fill(projectionColumnNums, -1);
    topLevelFields = new Field[count];
  }

  private Field allocatePrimitiveField(TypeInfo sourceTypeInfo,
      DataTypePhysicalVariation dataTypePhysicalVariation) {
    final PrimitiveTypeInfo sourcePrimitiveTypeInfo = (PrimitiveTypeInfo) sourceTypeInfo;
    final PrimitiveCategory sourcePrimitiveCategory = sourcePrimitiveTypeInfo.getPrimitiveCategory();
    final int maxLength;
    switch (sourcePrimitiveCategory) {
    case CHAR:
      maxLength = ((CharTypeInfo) sourcePrimitiveTypeInfo).getLength();
      break;
    case VARCHAR:
      maxLength = ((VarcharTypeInfo) sourcePrimitiveTypeInfo).getLength();
      break;
    default:
      // No additional data type specific setting.
      maxLength = 0;
      break;
    }
    return new Field(sourcePrimitiveCategory, dataTypePhysicalVariation, maxLength);
  }

  private Field allocateComplexField(TypeInfo sourceTypeInfo) {
    final Category category = sourceTypeInfo.getCategory();
    switch (category) {
    case LIST:
      {
        final ListTypeInfo listTypeInfo = (ListTypeInfo) sourceTypeInfo;
        final ListComplexTypeHelper listHelper =
            new ListComplexTypeHelper(
                allocateField(listTypeInfo.getListElementTypeInfo(), DataTypePhysicalVariation.NONE));
        return new Field(category, listHelper, sourceTypeInfo);
      }
    case MAP:
      {
        final MapTypeInfo mapTypeInfo = (MapTypeInfo) sourceTypeInfo;
        final MapComplexTypeHelper mapHelper =
            new MapComplexTypeHelper(
                allocateField(mapTypeInfo.getMapKeyTypeInfo(), DataTypePhysicalVariation.NONE),
                allocateField(mapTypeInfo.getMapValueTypeInfo(), DataTypePhysicalVariation.NONE));
        return new Field(category, mapHelper, sourceTypeInfo);
      }
    case STRUCT:
      {
        final StructTypeInfo structTypeInfo = (StructTypeInfo) sourceTypeInfo;
        final ArrayList<TypeInfo> fieldTypeInfoList = structTypeInfo.getAllStructFieldTypeInfos();
        final int count = fieldTypeInfoList.size();
        final Field[] fields = new Field[count];
        for (int i = 0; i < count; i++) {
          fields[i] = allocateField(fieldTypeInfoList.get(i), DataTypePhysicalVariation.NONE);
        }
        final StructComplexTypeHelper structHelper =
            new StructComplexTypeHelper(fields);
        return new Field(category, structHelper, sourceTypeInfo);
      }
    case UNION:
      {
        final UnionTypeInfo unionTypeInfo = (UnionTypeInfo) sourceTypeInfo;
        final List<TypeInfo> fieldTypeInfoList = unionTypeInfo.getAllUnionObjectTypeInfos();
        final int count = fieldTypeInfoList.size();
        final Field[] fields = new Field[count];
        for (int i = 0; i < count; i++) {
          fields[i] = allocateField(fieldTypeInfoList.get(i), DataTypePhysicalVariation.NONE);
        }
        final UnionComplexTypeHelper unionHelper =
            new UnionComplexTypeHelper(fields);
        return new Field(category, unionHelper, sourceTypeInfo);
      }
    default:
      throw new RuntimeException("Category " + category + " not supported");
    }
  }

  private Field allocateField(TypeInfo sourceTypeInfo, DataTypePhysicalVariation dataTypePhysicalVariation) {
    switch (sourceTypeInfo.getCategory()) {
    case PRIMITIVE:
      return allocatePrimitiveField(sourceTypeInfo, dataTypePhysicalVariation);
    case LIST:
    case MAP:
    case STRUCT:
    case UNION:
      return allocateComplexField(sourceTypeInfo);
    default:
      throw new RuntimeException("Category " + sourceTypeInfo.getCategory() + " not supported");
    }
  }

  /*
   * Initialize one column's source deserializtion information.
   */
  private void initTopLevelField(int logicalColumnIndex, int projectionColumnNum,
      TypeInfo sourceTypeInfo, DataTypePhysicalVariation dataTypePhysicalVariation) {

    projectionColumnNums[logicalColumnIndex] = projectionColumnNum;

    topLevelFields[logicalColumnIndex] = allocateField(sourceTypeInfo, dataTypePhysicalVariation);
  }

  /*
   * Initialize the conversion related arrays.  Assumes initTopLevelField has already been called.
   */
  private void addTopLevelConversion(int logicalColumnIndex, TypeInfo targetTypeInfo) {

    final Field field = topLevelFields[logicalColumnIndex];
    field.setIsConvert(true);

    if (field.getIsPrimitive()) {

      PrimitiveTypeInfo targetPrimitiveTypeInfo = (PrimitiveTypeInfo) targetTypeInfo;
      switch (targetPrimitiveTypeInfo.getPrimitiveCategory()) {
      case CHAR:
        field.setMaxLength(((CharTypeInfo) targetPrimitiveTypeInfo).getLength());
        break;
      case VARCHAR:
        field.setMaxLength(((VarcharTypeInfo) targetPrimitiveTypeInfo).getLength());
        break;
      default:
        // No additional data type specific setting.
        break;
      }
      field.setConversionWritable(
          VectorizedBatchUtil.getPrimitiveWritable(field.getPrimitiveCategory()));
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
      initTopLevelField(i, outputColumn, sourceTypeInfos[i], dataTypePhysicalVariations[i]);
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
      initTopLevelField(i, outputColumn, sourceTypeInfos[i], dataTypePhysicalVariations[i]);
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
      initTopLevelField(i, outputColumn, sourceTypeInfos[i], dataTypePhysicalVariations[i]);
    }
  }

  public void init(boolean[] columnsToIncludeTruncated) throws HiveException {

    // When truncated included is used, its length must be at least the number of source type infos.
    // When longer, we assume the caller will default with nulls, etc.
    Preconditions.checkState(
        columnsToIncludeTruncated == null ||
        columnsToIncludeTruncated.length == sourceTypeInfos.length);

    final int columnCount = sourceTypeInfos.length;
    allocateArrays(columnCount);

    int includedCount = 0;
    final int[] includedIndices = new int[columnCount];

    for (int i = 0; i < columnCount; i++) {

      if (columnsToIncludeTruncated != null && !columnsToIncludeTruncated[i]) {

        // Field not included in query.

      } else {

        initTopLevelField(i, i, sourceTypeInfos[i], dataTypePhysicalVariations[i]);
        includedIndices[includedCount++] = i;
      }
    }

    // Optimizing for readField?
    if (includedCount < columnCount && deserializeRead.isReadFieldSupported()) {
      useReadField = true;
      readFieldLogicalIndices = Arrays.copyOf(includedIndices, includedCount);
    }

  }

  /**
   * Initialize for converting the source data type that are going to be read with the
   * DeserializedRead interface passed to the constructor to the target data types desired in
   * the VectorizedRowBatch.
   *
   * No projection -- using the column range 0 .. columnCount-1
   *
   * @param targetTypeInfos
   * @param columnsToIncludeTruncated
   * @throws HiveException
   */
  public void initConversion(TypeInfo[] targetTypeInfos,
      boolean[] columnsToIncludeTruncated) throws HiveException {

    // We assume the caller will handle extra columns default with nulls, etc.
    Preconditions.checkState(targetTypeInfos.length >= sourceTypeInfos.length);

    // When truncated included is used, its length must be at least the number of source type infos.
    // When longer, we assume the caller will default with nulls, etc.
    Preconditions.checkState(
        columnsToIncludeTruncated == null ||
        columnsToIncludeTruncated.length >= sourceTypeInfos.length);

    final int columnCount = sourceTypeInfos.length;
    allocateArrays(columnCount);

    int includedCount = 0;
    int[] includedIndices = new int[columnCount];

    boolean atLeastOneConvert = false;
    for (int i = 0; i < columnCount; i++) {

      if (columnsToIncludeTruncated != null && !columnsToIncludeTruncated[i]) {

        // Field not included in query.

      } else {

        TypeInfo sourceTypeInfo = sourceTypeInfos[i];
        TypeInfo targetTypeInfo = targetTypeInfos[i];

        if (!sourceTypeInfo.equals(targetTypeInfo)) {

          if (VectorPartitionConversion.isImplicitVectorColumnConversion(sourceTypeInfo, targetTypeInfo)) {

            // Do implicit conversion from source type to target type.
            initTopLevelField(i, i, sourceTypeInfo, dataTypePhysicalVariations[i]);

          } else {

            // Do formal conversion...
            initTopLevelField(i, i, sourceTypeInfo, dataTypePhysicalVariations[i]);

            // UNDONE: No for List and Map; Yes for Struct and Union when field count different...
            addTopLevelConversion(i, targetTypeInfo);
            atLeastOneConvert = true;

          }
        } else {

          // No conversion.
          initTopLevelField(i, i, sourceTypeInfo, dataTypePhysicalVariations[i]);

        }

        includedIndices[includedCount++] = i;
      }
    }

    // Optimizing for readField?
    if (includedCount < columnCount && deserializeRead.isReadFieldSupported()) {
      useReadField = true;
      readFieldLogicalIndices = Arrays.copyOf(includedIndices, includedCount);
    }

    if (atLeastOneConvert) {

      // Let the VectorAssignRow class do the conversion.
      convertVectorAssignRow = new VectorAssignRow();
      convertVectorAssignRow.initConversion(sourceTypeInfos, targetTypeInfos,
          columnsToIncludeTruncated);
    }
  }

  public void init() throws HiveException {
    init(0);
  }

  private void storePrimitiveRowColumn(ColumnVector colVector, Field field,
      int batchIndex, boolean canRetainByteRef) throws IOException {

    switch (field.getPrimitiveCategory()) {
    case VOID:
      VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      return;
    case BOOLEAN:
      ((LongColumnVector) colVector).vector[batchIndex] = (deserializeRead.currentBoolean ? 1 : 0);
      break;
    case BYTE:
      ((LongColumnVector) colVector).vector[batchIndex] = deserializeRead.currentByte;
      break;
    case SHORT:
      ((LongColumnVector) colVector).vector[batchIndex] = deserializeRead.currentShort;
      break;
    case INT:
      ((LongColumnVector) colVector).vector[batchIndex] = deserializeRead.currentInt;
      break;
    case LONG:
      ((LongColumnVector) colVector).vector[batchIndex] = deserializeRead.currentLong;
      break;
    case TIMESTAMP:
      ((TimestampColumnVector) colVector).set(
          batchIndex, deserializeRead.currentTimestampWritable.getTimestamp().toSqlTimestamp());
      break;
    case DATE:
      ((LongColumnVector) colVector).vector[batchIndex] = deserializeRead.currentDateWritable.getDays();
      break;
    case FLOAT:
      ((DoubleColumnVector) colVector).vector[batchIndex] = deserializeRead.currentFloat;
      break;
    case DOUBLE:
      ((DoubleColumnVector) colVector).vector[batchIndex] = deserializeRead.currentDouble;
      break;
    case BINARY:
    case STRING:
      {
        final BytesColumnVector bytesColVec = ((BytesColumnVector) colVector);
        if (deserializeRead.currentExternalBufferNeeded) {
          bytesColVec.ensureValPreallocated(deserializeRead.currentExternalBufferNeededLen);
          deserializeRead.copyToExternalBuffer(
              bytesColVec.getValPreallocatedBytes(), bytesColVec.getValPreallocatedStart());
          bytesColVec.setValPreallocated(
              batchIndex,
              deserializeRead.currentExternalBufferNeededLen);
        } else if (canRetainByteRef && inputBytes == deserializeRead.currentBytes) {
          bytesColVec.setRef(
              batchIndex,
              deserializeRead.currentBytes,
              deserializeRead.currentBytesStart,
              deserializeRead.currentBytesLength);
        } else {
          bytesColVec.setVal(
              batchIndex,
              deserializeRead.currentBytes,
              deserializeRead.currentBytesStart,
              deserializeRead.currentBytesLength);
        }
      }
      break;
    case VARCHAR:
      {
        // Use the basic STRING bytes read to get access, then use our optimal truncate/trim method
        // that does not use Java String objects.
        final BytesColumnVector bytesColVec = ((BytesColumnVector) colVector);
        if (deserializeRead.currentExternalBufferNeeded) {
          // Write directly into our BytesColumnVector value buffer.
          bytesColVec.ensureValPreallocated(deserializeRead.currentExternalBufferNeededLen);
          final byte[] convertBuffer = bytesColVec.getValPreallocatedBytes();
          final int convertBufferStart = bytesColVec.getValPreallocatedStart();
          deserializeRead.copyToExternalBuffer(
              convertBuffer,
              convertBufferStart);
          bytesColVec.setValPreallocated(
              batchIndex,
              StringExpr.truncate(
                  convertBuffer,
                  convertBufferStart,
                  deserializeRead.currentExternalBufferNeededLen,
                  field.getMaxLength()));
        } else if (canRetainByteRef && inputBytes == deserializeRead.currentBytes) {
          bytesColVec.setRef(
              batchIndex,
              deserializeRead.currentBytes,
              deserializeRead.currentBytesStart,
              StringExpr.truncate(
                  deserializeRead.currentBytes,
                  deserializeRead.currentBytesStart,
                  deserializeRead.currentBytesLength,
                  field.getMaxLength()));
        } else {
          bytesColVec.setVal(
              batchIndex,
              deserializeRead.currentBytes,
              deserializeRead.currentBytesStart,
              StringExpr.truncate(
                  deserializeRead.currentBytes,
                  deserializeRead.currentBytesStart,
                  deserializeRead.currentBytesLength,
                  field.getMaxLength()));
        }
      }
      break;
    case CHAR:
      {
        // Use the basic STRING bytes read to get access, then use our optimal truncate/trim method
        // that does not use Java String objects.
        final BytesColumnVector bytesColVec = ((BytesColumnVector) colVector);
        if (deserializeRead.currentExternalBufferNeeded) {
          // Write directly into our BytesColumnVector value buffer.
          bytesColVec.ensureValPreallocated(deserializeRead.currentExternalBufferNeededLen);
          final byte[] convertBuffer = bytesColVec.getValPreallocatedBytes();
          final int convertBufferStart = bytesColVec.getValPreallocatedStart();
          deserializeRead.copyToExternalBuffer(
              convertBuffer,
              convertBufferStart);
          bytesColVec.setValPreallocated(
              batchIndex,
              StringExpr.rightTrimAndTruncate(
                  convertBuffer,
                  convertBufferStart,
                  deserializeRead.currentExternalBufferNeededLen,
                  field.getMaxLength()));
        } else if (canRetainByteRef && inputBytes == deserializeRead.currentBytes) {
          bytesColVec.setRef(
              batchIndex,
              deserializeRead.currentBytes,
              deserializeRead.currentBytesStart,
              StringExpr.rightTrimAndTruncate(
                  deserializeRead.currentBytes,
                  deserializeRead.currentBytesStart,
                  deserializeRead.currentBytesLength,
                  field.getMaxLength()));
        } else {
          bytesColVec.setVal(
              batchIndex,
              deserializeRead.currentBytes,
              deserializeRead.currentBytesStart,
              StringExpr.rightTrimAndTruncate(
                  deserializeRead.currentBytes,
                  deserializeRead.currentBytesStart,
                  deserializeRead.currentBytesLength,
                  field.getMaxLength()));
        }
      }
      break;
    case DECIMAL:
      if (field.getDataTypePhysicalVariation() == DataTypePhysicalVariation.DECIMAL_64) {
        ((Decimal64ColumnVector) colVector).vector[batchIndex] = deserializeRead.currentDecimal64;
      } else {
        // The DecimalColumnVector set method will quickly copy the deserialized decimal writable fields.
        ((DecimalColumnVector) colVector).set(
            batchIndex, deserializeRead.currentHiveDecimalWritable);
      }
      break;
    case INTERVAL_YEAR_MONTH:
      ((LongColumnVector) colVector).vector[batchIndex] =
          deserializeRead.currentHiveIntervalYearMonthWritable.getHiveIntervalYearMonth().getTotalMonths();
      break;
    case INTERVAL_DAY_TIME:
      ((IntervalDayTimeColumnVector) colVector).set(
          batchIndex, deserializeRead.currentHiveIntervalDayTimeWritable.getHiveIntervalDayTime());
      break;
    default:
      throw new RuntimeException("Primitive category " + field.getPrimitiveCategory() +
          " not supported");
    }
  }

  private static class ComplexTypeHelper {
  }

  private static class ListComplexTypeHelper extends ComplexTypeHelper {

    private Field elementField;

    public ListComplexTypeHelper(Field elementField) {
      this.elementField = elementField;
    }

    public Field getElementField() {
      return elementField;
    }
  }

  private static class MapComplexTypeHelper extends ComplexTypeHelper {

    private Field keyField;
    private Field valueField;

    public MapComplexTypeHelper(Field keyField, Field valueField) {
      this.keyField = keyField;
      this.valueField = valueField;
    }

    public Field getKeyField() {
      return keyField;
    }

    public Field getValueField() {
      return valueField;
    }
  }

  private static class FieldsComplexTypeHelper extends ComplexTypeHelper {

    private Field[] fields;

    public FieldsComplexTypeHelper(Field[] fields) {
      this.fields = fields;
    }

    public Field[] getFields() {
      return fields;
    }
  }

  private static class StructComplexTypeHelper extends FieldsComplexTypeHelper {

    public StructComplexTypeHelper(Field[] fields) {
      super(fields);
    }
  }

  private static class UnionComplexTypeHelper extends FieldsComplexTypeHelper {

    public UnionComplexTypeHelper(Field[] fields) {
      super(fields);
    }
  }

  // UNDONE: Presumption of *append*

  private void storeComplexFieldRowColumn(ColumnVector fieldColVector,
      Field field, int batchIndex, boolean canRetainByteRef) throws IOException {

    if (!deserializeRead.readComplexField()) {
      VectorizedBatchUtil.setNullColIsNullValue(fieldColVector, batchIndex);
      return;
    }

    if (field.getIsPrimitive()) {
      storePrimitiveRowColumn(fieldColVector, field, batchIndex, canRetainByteRef);
    } else {
      switch (field.getCategory()) {
      case LIST:
        storeListRowColumn(fieldColVector, field, batchIndex, canRetainByteRef);
        break;
      case MAP:
        storeMapRowColumn(fieldColVector, field, batchIndex, canRetainByteRef);
        break;
      case STRUCT:
        storeStructRowColumn(fieldColVector, field, batchIndex, canRetainByteRef);
        break;
      case UNION:
        storeUnionRowColumn(fieldColVector, field, batchIndex, canRetainByteRef);
        break;
      default:
        throw new RuntimeException("Category " + field.getCategory() + " not supported");
      }
    }

    fieldColVector.isNull[batchIndex] = false;
  }

  private void storeListRowColumn(ColumnVector colVector,
      Field field, int batchIndex, boolean canRetainByteRef) throws IOException {

    final ListColumnVector listColVector = (ListColumnVector) colVector;
    final ColumnVector elementColVector = listColVector.child;
    int offset = listColVector.childCount;
    listColVector.isNull[batchIndex] = false;
    listColVector.offsets[batchIndex] = offset;

    final ListComplexTypeHelper listHelper = (ListComplexTypeHelper) field.getComplexHelper();

    int listLength = 0;
    while (deserializeRead.isNextComplexMultiValue()) {

      // Ensure child size.
      final int childCapacity = listColVector.child.isNull.length;
      final int childCount = listColVector.childCount;
      if (childCapacity < childCount / 0.75) {
        listColVector.child.ensureSize(childCapacity * 2, true);
      }

      storeComplexFieldRowColumn(
          elementColVector, listHelper.getElementField(), offset, canRetainByteRef);
      offset++;
      listLength++;
    }

    listColVector.childCount += listLength;
    listColVector.lengths[batchIndex] = listLength;
  }

  private void storeMapRowColumn(ColumnVector colVector,
      Field field, int batchIndex, boolean canRetainByteRef) throws IOException {

    final MapColumnVector mapColVector = (MapColumnVector) colVector;
    final MapComplexTypeHelper mapHelper = (MapComplexTypeHelper) field.getComplexHelper();
    final ColumnVector keysColVector = mapColVector.keys;
    final ColumnVector valuesColVector = mapColVector.values;
    int offset = mapColVector.childCount;
    mapColVector.offsets[batchIndex] = offset;
    mapColVector.isNull[batchIndex] = false;

    int keyValueCount = 0;
    while (deserializeRead.isNextComplexMultiValue()) {

      // Ensure child size.
      final int childCapacity = mapColVector.keys.isNull.length;
      final int childCount = mapColVector.childCount;
      if (childCapacity < childCount / 0.75) {
        mapColVector.keys.ensureSize(childCapacity * 2, true);
        mapColVector.values.ensureSize(childCapacity * 2, true);
      }

      // Key.
      storeComplexFieldRowColumn(
          keysColVector, mapHelper.getKeyField(), offset, canRetainByteRef);

      // Value.
      storeComplexFieldRowColumn(
          valuesColVector, mapHelper.getValueField(), offset, canRetainByteRef);

      offset++;
      keyValueCount++;
    }

    mapColVector.childCount += keyValueCount;
    mapColVector.lengths[batchIndex] = keyValueCount;
  }

  private void storeStructRowColumn(ColumnVector colVector,
      Field field, int batchIndex, boolean canRetainByteRef) throws IOException {

    final StructColumnVector structColVector = (StructColumnVector) colVector;
    final ColumnVector[] colVectorFields = structColVector.fields;
    final StructComplexTypeHelper structHelper = (StructComplexTypeHelper) field.getComplexHelper();
    final Field[] fields = structHelper.getFields();
    structColVector.isNull[batchIndex] = false;

    int i = 0;
    for (ColumnVector colVectorField : colVectorFields) {
      storeComplexFieldRowColumn(
          colVectorField,
          fields[i],
          batchIndex,
          canRetainByteRef);
      i++;
    }
    deserializeRead.finishComplexVariableFieldsType();
  }

  private void storeUnionRowColumn(ColumnVector colVector,
      Field field, int batchIndex, boolean canRetainByteRef) throws IOException {

    deserializeRead.readComplexField();

    // The read field of the Union gives us its tag.
    final int tag = deserializeRead.currentInt;

    final UnionColumnVector unionColVector = (UnionColumnVector) colVector;
    final ColumnVector[] colVectorFields = unionColVector.fields;
    final UnionComplexTypeHelper unionHelper = (UnionComplexTypeHelper) field.getComplexHelper();

    unionColVector.isNull[batchIndex] = false;
    unionColVector.tags[batchIndex] = tag;

    storeComplexFieldRowColumn(
        colVectorFields[tag],
        unionHelper.getFields()[tag],
        batchIndex,
        canRetainByteRef);
    deserializeRead.finishComplexVariableFieldsType();
  }

  /**
   * Store one row column value that is the current value in deserializeRead.
   *
   * @param batch
   * @param batchIndex
   * @param logicalColumnIndex
   * @param canRetainByteRef    Specify true when it is safe to retain references to the bytes
   *                            source for DeserializeRead.  I.e. the STRING, CHAR/VARCHAR data
   *                            can be set in BytesColumnVector with setRef instead of with setVal
   *                            which copies data.  An example of a safe usage is referring to bytes
   *                            in a hash table entry that is immutable.
   * @throws IOException
   */
  private void storeRowColumn(VectorizedRowBatch batch, int batchIndex,
      Field field, int logicalColumnIndex, boolean canRetainByteRef) throws IOException {

    final int projectionColumnNum = projectionColumnNums[logicalColumnIndex];
    final ColumnVector colVector = batch.cols[projectionColumnNum];

    if (field.getIsPrimitive()) {
      storePrimitiveRowColumn(colVector, field, batchIndex, canRetainByteRef);
    } else {
      switch (field.getCategory()) {
      case LIST:
        storeListRowColumn(colVector, field, batchIndex, canRetainByteRef);
        break;
      case MAP:
        storeMapRowColumn(colVector, field, batchIndex, canRetainByteRef);
        break;
      case STRUCT:
        storeStructRowColumn(colVector, field, batchIndex, canRetainByteRef);
        break;
      case UNION:
        storeUnionRowColumn(colVector, field, batchIndex, canRetainByteRef);
        break;
      default:
        throw new RuntimeException("Category " + field.getCategory() + " not supported");
      }
    }

    // We always set the null flag to false when there is a value.
    colVector.isNull[batchIndex] = false;
  }

  /**
   * Convert one row column value that is the current value in deserializeRead.
   *
   * We deserialize into a writable and then pass that writable to an instance of VectorAssignRow
   * to convert the writable to the target data type and assign it into the VectorizedRowBatch.
   *
   * @param batch
   * @param batchIndex
   * @param logicalColumnIndex
   * @throws IOException
   */
  private void convertRowColumn(VectorizedRowBatch batch, int batchIndex,
      Field field, int logicalColumnIndex) throws IOException {

    final int projectionColumnIndex = projectionColumnNums[logicalColumnIndex];
    final ColumnVector colVector = batch.cols[projectionColumnIndex];

    final Object convertSourceWritable;
    if (field.getIsPrimitive()) {
      convertSourceWritable = convertPrimitiveRowColumn(batchIndex, field);
    } else {
      switch (field.getCategory()) {
      case LIST:
        convertSourceWritable = convertListRowColumn(colVector, batchIndex, field);
        break;
      case MAP:
        convertSourceWritable = convertMapRowColumn(colVector, batchIndex, field);
        break;
      case STRUCT:
        convertSourceWritable = convertStructRowColumn(colVector, batchIndex, field);
        break;
      case UNION:
        convertSourceWritable = convertUnionRowColumn(colVector, batchIndex, field);
        break;
      default:
        throw new RuntimeException();
      }
    }

    /*
     * Convert our source object we just read into the target object and store that in the
     * VectorizedRowBatch.
     */
    convertVectorAssignRow.assignConvertRowColumn(
        batch, batchIndex, logicalColumnIndex, convertSourceWritable);
  }

  private Object convertComplexFieldRowColumn(ColumnVector colVector, int batchIndex,
      Field field) throws IOException {

    if (!deserializeRead.readComplexField()) {
      VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      return null;
    }

    colVector.isNull[batchIndex] = false;
    if (field.getIsPrimitive()) {
      return convertPrimitiveRowColumn(batchIndex, field);
    }

    switch (field.getCategory()) {
    case LIST:
      return convertListRowColumn(colVector, batchIndex, field);
    case MAP:
      return convertMapRowColumn(colVector, batchIndex, field);
    case STRUCT:
      return convertStructRowColumn(colVector, batchIndex, field);
    case UNION:
      return convertUnionRowColumn(colVector, batchIndex, field);
    default:
      throw new RuntimeException();
    }
  }

  private Object convertPrimitiveRowColumn(int batchIndex, Field field) throws IOException {

    Object writable = field.getConversionWritable();
    switch (field.getPrimitiveCategory()) {
    case VOID:
      writable = null;
      break;
    case BOOLEAN:
      {
        if (writable == null) {
          writable = new BooleanWritable();
        }
        ((BooleanWritable) writable).set(deserializeRead.currentBoolean);
      }
      break;
    case BYTE:
      {
        if (writable == null) {
          writable = new ByteWritable();
        }
        ((ByteWritable) writable).set(deserializeRead.currentByte);
      }
      break;
    case SHORT:
      {
        if (writable == null) {
          writable = new ShortWritable();
        }
        ((ShortWritable) writable).set(deserializeRead.currentShort);
      }
      break;
    case INT:
      {
        if (writable == null) {
          writable = new IntWritable();
        }
        ((IntWritable) writable).set(deserializeRead.currentInt);
      }
      break;
    case LONG:
      {
        if (writable == null) {
          writable = new LongWritable();
        }
        ((LongWritable) writable).set(deserializeRead.currentLong);
      }
      break;
    case TIMESTAMP:
      {
        if (writable == null) {
          writable = new TimestampWritableV2();
        }
        ((TimestampWritableV2) writable).set(deserializeRead.currentTimestampWritable);
      }
      break;
    case DATE:
      {
        if (writable == null) {
          writable = new DateWritableV2();
        }
        ((DateWritableV2) writable).set(deserializeRead.currentDateWritable);
      }
      break;
    case FLOAT:
      {
        if (writable == null) {
          writable = new FloatWritable();
        }
        ((FloatWritable) writable).set(deserializeRead.currentFloat);
      }
      break;
    case DOUBLE:
      {
        if (writable == null) {
          writable = new DoubleWritable();
        }
        ((DoubleWritable) writable).set(deserializeRead.currentDouble);
      }
      break;
    case BINARY:
      {
        if (writable == null) {
          writable = new BytesWritable();
        }
        if (deserializeRead.currentBytes == null) {
          LOG.info(
              "null binary entry: batchIndex " + batchIndex);
        }

        ((BytesWritable) writable).set(
            deserializeRead.currentBytes,
            deserializeRead.currentBytesStart,
            deserializeRead.currentBytesLength);
        break;
      }
    case STRING:
      {
        if (writable == null) {
          writable = new Text();
        }
        if (deserializeRead.currentBytes == null) {
          throw new RuntimeException(
              "null string entry: batchIndex " + batchIndex);
        }

        // Use org.apache.hadoop.io.Text as our helper to go from byte[] to String.
        ((Text) writable).set(
            deserializeRead.currentBytes,
            deserializeRead.currentBytesStart,
            deserializeRead.currentBytesLength);
      }
      break;
    case VARCHAR:
      {
        if (writable == null) {
          writable = new HiveVarcharWritable();
        }
        // Use the basic STRING bytes read to get access, then use our optimal truncate/trim method
        // that does not use Java String objects.
        if (deserializeRead.currentBytes == null) {
          throw new RuntimeException(
              "null varchar entry: batchIndex " + batchIndex);
        }

        int adjustedLength = StringExpr.truncate(
            deserializeRead.currentBytes,
            deserializeRead.currentBytesStart,
            deserializeRead.currentBytesLength,
            field.getMaxLength());

        ((HiveVarcharWritable) writable).set(
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
        if (writable == null) {
          writable = new HiveCharWritable();
        }
        // Use the basic STRING bytes read to get access, then use our optimal truncate/trim method
        // that does not use Java String objects.
        if (deserializeRead.currentBytes == null) {
          throw new RuntimeException(
              "null char entry: batchIndex " + batchIndex);
        }

        int adjustedLength = StringExpr.rightTrimAndTruncate(
            deserializeRead.currentBytes,
            deserializeRead.currentBytesStart,
            deserializeRead.currentBytesLength,
            field.getMaxLength());

        ((HiveCharWritable) writable).set(
            new String(
              deserializeRead.currentBytes,
              deserializeRead.currentBytesStart,
              adjustedLength, Charsets.UTF_8),
            -1);
      }
      break;
    case DECIMAL:
      {
        if (writable == null) {
          writable = new HiveDecimalWritable();
        }
        ((HiveDecimalWritable) writable).set(
            deserializeRead.currentHiveDecimalWritable);
      }
      break;
    case INTERVAL_YEAR_MONTH:
      {
        if (writable == null) {
          writable = new HiveIntervalYearMonthWritable();
        }
        ((HiveIntervalYearMonthWritable) writable).set(
            deserializeRead.currentHiveIntervalYearMonthWritable);
      }
      break;
    case INTERVAL_DAY_TIME:
      {
        if (writable == null) {
          writable = new HiveIntervalDayTimeWritable();
        }
        ((HiveIntervalDayTimeWritable) writable).set(
            deserializeRead.currentHiveIntervalDayTimeWritable);
      }
      break;
    default:
      throw new RuntimeException("Primitive category " + field.getPrimitiveCategory() +
          " not supported");
    }
    return writable;
  }

  private Object convertListRowColumn(
      ColumnVector colVector, int batchIndex, Field field) throws IOException {

    final SettableListObjectInspector listOI = (SettableListObjectInspector) field.objectInspector;
    final ListComplexTypeHelper listHelper = (ListComplexTypeHelper) field.getComplexHelper();
    final Field elementField = listHelper.getElementField();
    final List<Object> tempList = new ArrayList<>();
    final ListColumnVector listColumnVector = (ListColumnVector) colVector;

    while (deserializeRead.isNextComplexMultiValue()) {
      tempList.add(
          convertComplexFieldRowColumn(listColumnVector.child, batchIndex, elementField));
    }

    final int size = tempList.size();
    final Object list = listOI.create(size);
    for (int i = 0; i < size; i++) {
      listOI.set(list, i, tempList.get(i));
    }
    return list;
  }

  private Object convertMapRowColumn(
      ColumnVector colVector, int batchIndex, Field field) throws IOException {

    final SettableMapObjectInspector mapOI = (SettableMapObjectInspector) field.objectInspector;
    final MapComplexTypeHelper mapHelper = (MapComplexTypeHelper) field.getComplexHelper();
    final Field keyField = mapHelper.getKeyField();
    final Field valueField = mapHelper.getValueField();
    final MapColumnVector mapColumnVector = (MapColumnVector) colVector;

    final Object map = mapOI.create();
    while (deserializeRead.isNextComplexMultiValue()) {
      final Object key = convertComplexFieldRowColumn(mapColumnVector.keys, batchIndex, keyField);
      final Object value = convertComplexFieldRowColumn(mapColumnVector.values, batchIndex, valueField);
      mapOI.put(map, key, value);
    }
    return map;
  }

  private Object convertStructRowColumn(
      ColumnVector colVector, int batchIndex, Field field) throws IOException {

    final SettableStructObjectInspector structOI = (SettableStructObjectInspector) field.objectInspector;
    final List<? extends StructField> structFields = structOI.getAllStructFieldRefs();
    final StructComplexTypeHelper structHelper = (StructComplexTypeHelper) field.getComplexHelper();
    final Field[] fields = structHelper.getFields();
    final StructColumnVector structColumnVector = (StructColumnVector) colVector;

    final Object struct = structOI.create();
    for (int i = 0; i < fields.length; i++) {
      final Object fieldObject =
          convertComplexFieldRowColumn(structColumnVector.fields[i], batchIndex, fields[i]);
      structOI.setStructFieldData(struct, structFields.get(i), fieldObject);
    }
    deserializeRead.finishComplexVariableFieldsType();
    return struct;
  }

  private Object convertUnionRowColumn(
      ColumnVector colVector, int batchIndex, Field field) throws IOException {

    final SettableUnionObjectInspector unionOI = (SettableUnionObjectInspector) field.objectInspector;
    final UnionComplexTypeHelper unionHelper = (UnionComplexTypeHelper) field.getComplexHelper();
    final Field[] fields = unionHelper.getFields();
    final UnionColumnVector unionColumnVector = (UnionColumnVector) colVector;

    final Object union = unionOI.create();
    final int tag = deserializeRead.currentInt;
    unionOI.setFieldAndTag(union, new StandardUnion((byte) tag,
        convertComplexFieldRowColumn(unionColumnVector.fields[tag], batchIndex, fields[tag])), (byte) tag);
    deserializeRead.finishComplexVariableFieldsType();
    return union;
  }

  /**
   * Specify the range of bytes to deserialize in the next call to the deserialize method.
   *
   * @param bytes
   * @param offset
   * @param length
   */
  public void setBytes(byte[] bytes, int offset, int length) {
    inputBytes = bytes;
    deserializeRead.set(bytes, offset, length);
  }

  /**
   * Deserialize a row from the range of bytes specified by setBytes.
   *
   * Use getDetailedReadPositionString to get detailed read position information to help
   * diagnose exceptions that are thrown...
   *
   * This version of deserialize does not keep byte references to string/char/varchar/binary data
   * type field.  The bytes are copied into the BytesColumnVector buffer with setVal.
   * (See deserializeByRef below if keep references is safe).
   *
   * @param batch
   * @param batchIndex
   * @throws IOException
   */
  public void deserialize(VectorizedRowBatch batch, int batchIndex) throws IOException {

    // Pass false for canRetainByteRef since we will NOT be keeping byte references to the input
    // bytes with the BytesColumnVector.setRef method.

    final int count = topLevelFields.length;

    Field field;

    if (!useReadField) {
      for (int i = 0; i < count; i++) {
        final int projectionColumnNum = projectionColumnNums[i];
        if (projectionColumnNum == -1) {
          // We must read through fields we do not want.
          deserializeRead.skipNextField();
          continue;
        }
        if (!deserializeRead.readNextField()) {
          ColumnVector colVector = batch.cols[projectionColumnNum];
          colVector.isNull[batchIndex] = true;
          colVector.noNulls = false;
          continue;
        }
        // The current* members of deserializeRead have the field value.
        field = topLevelFields[i];
        if (field.getIsConvert()) {
          convertRowColumn(batch, batchIndex, field, i);
        } else {
          storeRowColumn(batch, batchIndex, field, i, /* canRetainByteRef */ false);
        }
      }
    } else {
      final int readFieldCount = readFieldLogicalIndices.length;
      for (int i = 0; i < readFieldCount; i++) {
        final int logicalIndex = readFieldLogicalIndices[i];
        // Jump to the field we want and read it.
        if (!deserializeRead.readField(logicalIndex)) {
          ColumnVector colVector = batch.cols[projectionColumnNums[logicalIndex]];
          colVector.isNull[batchIndex] = true;
          colVector.noNulls = false;
          continue;
        }
        // The current* members of deserializeRead have the field value.
        field = topLevelFields[logicalIndex];
        if (field.getIsConvert()) {
          convertRowColumn(batch, batchIndex, field, logicalIndex);
        } else {
          storeRowColumn(batch, batchIndex, field, logicalIndex, /* canRetainByteRef */ false);
        }
      }
    }
  }

  /**
   * Deserialize a row from the range of bytes specified by setBytes.
   *
   * Use this method instead of deserialize when it is safe to retain references to the bytes source
   * for DeserializeRead.  I.e. the STRING, CHAR/VARCHAR data can be set in BytesColumnVector with
   * setRef instead of with setVal which copies data.
   *
   * An example of a safe usage:
   *   Referring to bytes in a hash table entry that is immutable.
   *
   * An example of a unsafe usage:
   *   Referring to bytes in a reduce receive buffer that will be overwritten with new data.
   *
   * Use getDetailedReadPositionString to get detailed read position information to help
   * diagnose exceptions that are thrown...
   *
   * @param batch
   * @param batchIndex
   * @throws IOException
   */
  public void deserializeByRef(VectorizedRowBatch batch, int batchIndex) throws IOException {

    final int count = topLevelFields.length;

    Field field;

    if (!useReadField) {
      for (int i = 0; i < count; i++) {
        final int projectionColumnNum = projectionColumnNums[i];
        if (projectionColumnNum == -1) {
          // We must read through fields we do not want.
          deserializeRead.skipNextField();
          continue;
        }
        if (!deserializeRead.readNextField()) {
          ColumnVector colVector = batch.cols[projectionColumnNum];
          colVector.isNull[batchIndex] = true;
          colVector.noNulls = false;
          continue;
        }
        // The current* members of deserializeRead have the field value.
        field = topLevelFields[i];
        if (field.getIsConvert()) {
          convertRowColumn(batch, batchIndex, field, i);
        } else {
          storeRowColumn(batch, batchIndex, field, i, /* canRetainByteRef */ true);
        }
      }
    } else {
      final int readFieldCount = readFieldLogicalIndices.length;
      for (int i = 0; i < readFieldCount; i++) {
        final int logicalIndex = readFieldLogicalIndices[i];
        // Jump to the field we want and read it.
        if (!deserializeRead.readField(logicalIndex)) {
          ColumnVector colVector = batch.cols[projectionColumnNums[logicalIndex]];
          colVector.isNull[batchIndex] = true;
          colVector.noNulls = false;
          continue;
        }
        // The current* members of deserializeRead have the field value.
        field = topLevelFields[logicalIndex];
        if (field.getIsConvert()) {
          convertRowColumn(batch, batchIndex, field, logicalIndex);
        } else {
          storeRowColumn(batch, batchIndex, field, logicalIndex, /* canRetainByteRef */ true);
        }
      }
    }
  }


  public String getDetailedReadPositionString() {
    return deserializeRead.getDetailedReadPositionString();
  }
}
