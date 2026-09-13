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

package org.apache.hadoop.hive.ql.io.parquet.vector;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.parquet.ParquetRuntimeException;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.InvalidSchemaException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 * Reusable, stateless helper that builds the {@link VectorizedColumnReader} array for one Parquet
 * row group, given an already-read {@link PageReadStore}. The logic here was lifted verbatim from
 * {@link VectorizedParquetRecordReader} so that both the row-by-split reader and the LLAP
 * cache-backed consumer ({@code ParquetEncodedDataConsumer}) can share a single, behavior-preserving
 * implementation of the Hive-type-driven Parquet column-reader construction.
 *
 * <p>The instance only carries the {@code fileSchema} (used for the schema-evolution check). All
 * per-row-group state lives in the supplied {@link PageReadStore}.
 */
public class ParquetRowGroupDecoder {

  private static final int MAP_DEFINITION_LEVEL_MAX = 3;

  private final MessageType fileSchema;
  private final Map<String, Object> initialDefaults;

  public ParquetRowGroupDecoder(MessageType fileSchema, Map<String, Object> initialDefaults) {
    this.fileSchema = fileSchema;
    this.initialDefaults = initialDefaults;
  }

  /**
   * Builds the per-(requested-)column {@link VectorizedColumnReader} array for a row group.
   *
   * @param pages                  the row group's page store (from {@code reader.readRowGroup(..)}
   *                               / {@code readNextRowGroup()})
   * @param requestedSchema        the projected Parquet schema being read
   * @param columnTypesList        the Hive type infos for ALL table columns (indexed by table col id)
   * @param colsToInclude          the table column ids being read, in requested-schema field order;
   *                               may be empty (e.g. {@code count(*)}), in which case all readers are
   *                               null
   * @param readAllColumns         whether projection is "read all columns"
   * @param skipTimestampConversion see {@code VectorizedParquetRecordReader}
   * @param writerTimezone           see {@code VectorizedParquetRecordReader}
   * @param skipProlepticConversion  see {@code VectorizedParquetRecordReader}
   * @param legacyConversionEnabled  see {@code VectorizedParquetRecordReader}
   * @return one reader per requested-schema field (null entries where no reader is needed)
   */
  public VectorizedColumnReader[] buildColumnReaders(
      PageReadStore pages,
      MessageType requestedSchema,
      List<TypeInfo> columnTypesList,
      List<Integer> colsToInclude,
      boolean readAllColumns,
      boolean skipTimestampConversion,
      ZoneId writerTimezone,
      boolean skipProlepticConversion,
      boolean legacyConversionEnabled) throws IOException {
    List<Type> types = requestedSchema.getFields();
    VectorizedColumnReader[] columnReaders = new VectorizedColumnReader[types.size()];

    if (!readAllColumns) {
      // certain queries like select count(*) from table do not have
      // any projected columns and still have isReadAllColumns as false
      // in such cases columnReaders are not needed
      // However, if colsToInclude is not empty we should initialize each columnReader
      if (!colsToInclude.isEmpty()) {
        for (int i = 0; i < types.size(); ++i) {
          columnReaders[i] = buildVectorizedParquetReader(
              columnTypesList.get(colsToInclude.get(i)), types.get(i), pages,
              requestedSchema.getColumns(), skipTimestampConversion, writerTimezone,
              skipProlepticConversion, legacyConversionEnabled, 0, 0);
        }
      }
    } else {
      for (int i = 0; i < types.size(); ++i) {
        columnReaders[i] = buildVectorizedParquetReader( columnTypesList.get(i),
            types.get(i), pages, requestedSchema.getColumns(), skipTimestampConversion,
            writerTimezone, skipProlepticConversion, legacyConversionEnabled, 0, 0);
      }
    }
    return columnReaders;
  }

  private static List<ColumnDescriptor> getAllColumnDescriptorByType(
      int depth,
      Type type,
      List<ColumnDescriptor> columns) throws ParquetRuntimeException {
    List<ColumnDescriptor> res = new ArrayList<>();
    for (ColumnDescriptor descriptor : columns) {
      if (depth >= descriptor.getPath().length) {
        throw new InvalidSchemaException("Corrupted Parquet schema");
      }
      if (type.getName().equals(descriptor.getPath()[depth])) {
        res.add(descriptor);
      }
    }
    return res;
  }

  // TODO support only non nested case
  private static PrimitiveType getElementType(Type type) {
    if (type.isPrimitive()) {
      return type.asPrimitiveType();
    }
    if (type.asGroupType().getFields().size() > 1) {
      throw new RuntimeException(
          "Current Parquet Vectorization reader doesn't support nested type");
    }

    Type childType = type.asGroupType().getFields().get(0);

    // Parquet file generated using thrift may have child type as PrimitiveType
    if (childType.isPrimitive()) {
      return childType.asPrimitiveType();
    } else {
      return childType.asGroupType().getFields().get(0).asPrimitiveType();
    }
  }

  // Build VectorizedParquetColumnReader via Hive typeInfo and Parquet schema
  private VectorizedColumnReader buildVectorizedParquetReader(
      TypeInfo typeInfo,
      Type type,
      PageReadStore pages,
      List<ColumnDescriptor> columnDescriptors,
      boolean skipTimestampConversion,
      ZoneId writerTimezone,
      boolean skipProlepticConversion,
      boolean legacyConversionEnabled,
      int depth, int currentDefLevel) throws IOException {
    int typeDefLevel = currentDefLevel;
    if (type.isRepetition(Type.Repetition.OPTIONAL) || type.isRepetition(Type.Repetition.REPEATED)) {
      typeDefLevel++;
    }
    List<ColumnDescriptor> descriptors =
        getAllColumnDescriptorByType(depth, type, columnDescriptors);
    // Support for schema evolution: if the column from the current
    // query schema is not present in the file schema, return a dummy
    // reader that produces nulls. This allows queries to proceed even
    // when new columns have been added after the file was written.
    if (!fileSchema.getColumns().contains(descriptors.get(0))) {
      return new VectorizedDummyColumnReader(Optional.ofNullable(initialDefaults)
          .map(defaults -> defaults.getOrDefault(descriptors.get(0).getPath()[0], null)).orElse(null));
    }
    switch (typeInfo.getCategory()) {
    case PRIMITIVE:
      if (columnDescriptors == null || columnDescriptors.isEmpty()) {
        throw new RuntimeException(
            "Failed to find related Parquet column descriptor with type " + type);
      }
      return new VectorizedPrimitiveColumnReader(descriptors.get(0),
          pages.getPageReader(descriptors.get(0)), skipTimestampConversion, writerTimezone,
          skipProlepticConversion, legacyConversionEnabled, type, typeInfo);
    case STRUCT:
      StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
      List<VectorizedColumnReader> fieldReaders = new ArrayList<>();
      List<TypeInfo> fieldTypes = structTypeInfo.getAllStructFieldTypeInfos();
      List<Type> types = type.asGroupType().getFields();
      for (int i = 0; i < fieldTypes.size(); i++) {
        VectorizedColumnReader r =
            buildVectorizedParquetReader( fieldTypes.get(i), types.get(i), pages,
                descriptors, skipTimestampConversion, writerTimezone, skipProlepticConversion,
                legacyConversionEnabled, depth + 1, typeDefLevel);
        if (r != null) {
          fieldReaders.add(r);
        } else {
          throw new RuntimeException(
              "Fail to build Parquet vectorized reader based on Hive type " + fieldTypes.get(i)
                  .getTypeName() + " and Parquet type" + types.get(i).toString());
        }
      }
      return new VectorizedStructColumnReader(fieldReaders, typeDefLevel);
    case LIST:
      checkListColumnSupport(((ListTypeInfo) typeInfo).getListElementTypeInfo());
      if (columnDescriptors == null || columnDescriptors.isEmpty()) {
        throw new RuntimeException(
            "Failed to find related Parquet column descriptor with type " + type);
      }

      return new VectorizedListColumnReader(descriptors.get(0),
          pages.getPageReader(descriptors.get(0)), skipTimestampConversion, writerTimezone,
          skipProlepticConversion, legacyConversionEnabled, getElementType(type), typeInfo);
    case MAP:
      if (columnDescriptors == null || columnDescriptors.isEmpty()) {
        throw new RuntimeException(
            "Failed to find related Parquet column descriptor with type " + type);
      }

      // to handle the different Map definition in Parquet, eg:
      // definition has 1 group:
      //   repeated group map (MAP_KEY_VALUE)
      //     {required binary key (UTF8); optional binary value (UTF8);}
      // definition has 2 groups:
      //   optional group m1 (MAP) {
      //     repeated group map (MAP_KEY_VALUE)
      //       {required binary key (UTF8); optional binary value (UTF8);}
      //   }
      int nestGroup = 0;
      GroupType groupType = type.asGroupType();
      // if FieldCount == 2, get types for key & value,
      // otherwise, continue to get the group type until MAP_DEFINITION_LEVEL_MAX.
      while (groupType.getFieldCount() < 2) {
        if (nestGroup > MAP_DEFINITION_LEVEL_MAX) {
          throw new RuntimeException(
              "More than " + MAP_DEFINITION_LEVEL_MAX + " level is found in Map definition, " +
                  "Failed to get the field types for Map with type " + type);
        }
        groupType = groupType.getFields().get(0).asGroupType();
        nestGroup++;
      }
      List<Type> kvTypes = groupType.getFields();
      VectorizedListColumnReader keyListColumnReader = new VectorizedListColumnReader(
          descriptors.get(0), pages.getPageReader(descriptors.get(0)), skipTimestampConversion,
          writerTimezone, skipProlepticConversion, legacyConversionEnabled, kvTypes.get(0), typeInfo);
      VectorizedListColumnReader valueListColumnReader = new VectorizedListColumnReader(
          descriptors.get(1), pages.getPageReader(descriptors.get(1)), skipTimestampConversion,
          writerTimezone, skipProlepticConversion, legacyConversionEnabled, kvTypes.get(1), typeInfo);
      return new VectorizedMapColumnReader(keyListColumnReader, valueListColumnReader);
    case UNION:
    default:
      throw new RuntimeException("Unsupported category " + typeInfo.getCategory().name());
    }
  }

  /**
   * Check if the element type in list is supported by vectorization read.
   * Supported type: INT, BYTE, SHORT, DATE, INTERVAL_YEAR_MONTH, LONG, BOOLEAN, DOUBLE, BINARY,
   *                 STRING, CHAR, VARCHAR, FLOAT, DECIMAL
   */
  private static void checkListColumnSupport(TypeInfo elementType) {
    if (elementType instanceof org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo) {
      switch (((org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo) elementType)
          .getPrimitiveCategory()) {
        case INTERVAL_DAY_TIME:
        case TIMESTAMP:
          throw new RuntimeException("Unsupported primitive type used in list:: " + elementType);
        default:
          // supported
      }
    } else {
      throw new RuntimeException("Unsupported type used in list:" + elementType);
    }
  }
}
