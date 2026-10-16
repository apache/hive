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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.io.parquet.vector;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
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
 * row group, given an already-read {@link PageReadStore}. The logic here came from
 * {@link VectorizedParquetRecordReader} so that both the row-by-split reader and the LLAP
 * cache-backed consumer ({@code ParquetEncodedDataConsumer}) share one implementation of the
 * Hive-type-driven Parquet column-reader construction.
 *
 * <p>Behaviour-preserving, not character-for-character: the schema errors that were
 * {@code RuntimeException} there are the {@link InvalidSchemaException} and
 * {@link UnsupportedOperationException} parquet already defines for them, which is what the
 * callers' decline paths catch.
 *
 * <p>The instance only carries per-file state (the file's leaf columns, used for the
 * schema-evolution check), so it is built once per file and reused for every row group; all
 * per-row-group state lives in the supplied {@link PageReadStore}.
 */
public class ParquetRowGroupDecoder {

  /**
   * Group levels the MAP case unwraps looking for the {key, value} group. parquet-format puts
   * exactly one between the MAP-annotated group and that pair, in the canonical layout and in every
   * backward-compatible one; three is inherited slack, not a spec limit. Off by one in practice:
   * the bound is tested before the counter is bumped, so a fourth wrapper still reads.
   */
  private static final int MAX_MAP_WRAPPER_GROUPS = 3;

  /**
   * Bag of writer-timezone / proleptic / legacy conversion flags forwarded to every primitive
   * column reader. Grouped so the reader-construction entry point stays under the parameter cap.
   */
  public record TimestampConversionOptions(
      boolean skipTimestampConversion,
      ZoneId writerTimezone,
      boolean skipProlepticConversion,
      boolean legacyConversionEnabled) {
  }

  private final Set<ColumnDescriptor> fileColumns;
  private final Map<String, Object> initialDefaults;

  public ParquetRowGroupDecoder(MessageType fileSchema, Map<String, Object> initialDefaults) {
    // Materialized once: getColumns() rebuilds the whole leaf list on every call, and the
    // schema-evolution check below runs per requested column.
    this.fileColumns = new HashSet<>(fileSchema.getColumns());
    this.initialDefaults = initialDefaults;
  }

  /**
   * Builds the per-(requested-)column {@link VectorizedColumnReader} array for a row group.
   *
   * @param pages           the row group's page store (from {@code reader.readRowGroup(..)}
   *                        / {@code readNextRowGroup()})
   * @param requestedSchema the projected Parquet schema being read
   * @param columnTypesList the Hive type infos for ALL table columns (indexed by table col id)
   * @param colsToInclude   the table column ids being read, in requested-schema field order;
   *                        may be empty (e.g. {@code count(*)}), in which case all readers are null
   * @param readAllColumns  whether projection is "read all columns"
   * @param options         writer-timezone / proleptic / legacy conversion flags, see
   *                        {@link TimestampConversionOptions}
   * @return one reader per requested-schema field (null entries where no reader is needed)
   */
  public VectorizedColumnReader[] buildColumnReaders(
      PageReadStore pages,
      MessageType requestedSchema,
      List<TypeInfo> columnTypesList,
      List<Integer> colsToInclude,
      boolean readAllColumns,
      TimestampConversionOptions options) throws IOException {
    List<Type> types = requestedSchema.getFields();
    VectorizedColumnReader[] columnReaders = new VectorizedColumnReader[types.size()];

    // Certain queries like "select count(*) from table" project no columns and still have
    // isReadAllColumns as false; in such cases no columnReader is needed and the array stays null.
    if (!readAllColumns && colsToInclude.isEmpty()) {
      return columnReaders;
    }
    // Hoisted: getColumns() walks the schema and rebuilds the whole leaf list on every call, and
    // this one is the same list for every column of every row group of the split.
    List<ColumnDescriptor> requestedLeaves = requestedSchema.getColumns();
    for (int i = 0; i < types.size(); ++i) {
      int tableCol = readAllColumns ? i : colsToInclude.get(i);
      columnReaders[i] = buildVectorizedParquetReader(columnTypesList.get(tableCol), types.get(i),
          pages, requestedLeaves, options, 0, 0);
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

  /**
   * The primitive element type of a LIST's three-level encoding. Nested types are unsupported here
   * on purpose; callers detect that in advance
   * ({@code ParquetEncodedDataReader.projectsNestedTypes}) and route the query to the non-native
   * reader.
   */
  private static PrimitiveType listElementType(Type type) {
    if (type.isPrimitive()) {
      return type.asPrimitiveType();
    }
    if (type.asGroupType().getFields().size() > 1) {
      throw new UnsupportedOperationException(
          "Current Parquet Vectorization reader doesn't support nested type");
    }

    Type childType = type.asGroupType().getFields().getFirst();

    // Parquet file generated using thrift may have child type as PrimitiveType
    if (childType.isPrimitive()) {
      return childType.asPrimitiveType();
    } else {
      return childType.asGroupType().getFields().getFirst().asPrimitiveType();
    }
  }

  // Build VectorizedParquetColumnReader via Hive typeInfo and Parquet schema
  private VectorizedColumnReader buildVectorizedParquetReader(
      TypeInfo typeInfo,
      Type type,
      PageReadStore pages,
      List<ColumnDescriptor> columnDescriptors,
      TimestampConversionOptions options,
      int depth, int currentDefLevel) throws IOException {
    int typeDefLevel = currentDefLevel;
    if (type.isRepetition(Type.Repetition.OPTIONAL) || type.isRepetition(Type.Repetition.REPEATED)) {
      typeDefLevel++;
    }
    List<ColumnDescriptor> descriptors =
        getAllColumnDescriptorByType(depth, type, columnDescriptors);
    if (descriptors.isEmpty()) {
      throw new InvalidSchemaException(
          "Failed to find related Parquet column descriptor with type " + type);
    }
    ColumnDescriptor descriptor = descriptors.getFirst();
    // Support for schema evolution: if the column from the current
    // query schema is not present in the file schema, return a dummy
    // reader that produces nulls. This allows queries to proceed even
    // when new columns have been added after the file was written.
    if (!fileColumns.contains(descriptor)) {
      return new VectorizedDummyColumnReader(
          initialDefaults == null ? null : initialDefaults.get(descriptor.getPath()[0]));
    }
    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        return new VectorizedPrimitiveColumnReader(descriptor, pages.getPageReader(descriptor),
            options.skipTimestampConversion(), options.writerTimezone(),
            options.skipProlepticConversion(), options.legacyConversionEnabled(), type, typeInfo);
      case STRUCT:
        StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
        List<VectorizedColumnReader> fieldReaders = new ArrayList<>();
        List<TypeInfo> fieldTypes = structTypeInfo.getAllStructFieldTypeInfos();
        List<Type> types = type.asGroupType().getFields();
        for (int i = 0; i < fieldTypes.size(); i++) {
          fieldReaders.add(buildVectorizedParquetReader(fieldTypes.get(i), types.get(i), pages,
              descriptors, options, depth + 1, typeDefLevel));
        }
        return new VectorizedStructColumnReader(fieldReaders, typeDefLevel);
      case LIST:
        checkListColumnSupport(((ListTypeInfo) typeInfo).getListElementTypeInfo());
        return listReader(descriptor, pages, options, listElementType(type), typeInfo);
      case MAP:
        // Unwrap group levels until the {key, value} group - see MAX_MAP_WRAPPER_GROUPS, and
        // parquet-format's LogicalTypes spec for the shapes the MAP annotation allows.
        int nestGroup = 0;
        GroupType groupType = type.asGroupType();
        while (groupType.getFieldCount() < 2) {
          if (nestGroup > MAX_MAP_WRAPPER_GROUPS) {
            throw new InvalidSchemaException(
                "More than " + MAX_MAP_WRAPPER_GROUPS + " level is found in Map definition, "
                    + "Failed to get the field types for Map with type " + type);
          }
          groupType = groupType.getFields().getFirst().asGroupType();
          nestGroup++;
        }
        List<Type> kvTypes = groupType.getFields();
        return new VectorizedMapColumnReader(
            listReader(descriptor, pages, options, kvTypes.getFirst(), typeInfo),
            listReader(descriptors.get(1), pages, options, kvTypes.get(1), typeInfo));
      case UNION:
    default:
      throw new UnsupportedOperationException("Unsupported category " + typeInfo.getCategory().name());
    }
  }

  /** The one reader shape a LIST, and each half of a MAP, is read through. */
  private static VectorizedListColumnReader listReader(ColumnDescriptor descriptor,
      PageReadStore pages, TimestampConversionOptions options, Type elementType, TypeInfo typeInfo)
      throws IOException {
    return new VectorizedListColumnReader(descriptor, pages.getPageReader(descriptor),
        options.skipTimestampConversion(), options.writerTimezone(),
        options.skipProlepticConversion(), options.legacyConversionEnabled(), elementType, typeInfo);
  }

  /**
   * Rejects the list element types the vectorized readers cannot decode: anything that is not a
   * primitive, and of the primitives TIMESTAMP and INTERVAL_DAY_TIME. Every other primitive is
   * accepted, so a new one needs no change here - which is the reason this is a reject list rather
   * than the accept list the javadoc used to claim it was.
   */
  private static void checkListColumnSupport(TypeInfo elementType) {
    if (elementType instanceof PrimitiveTypeInfo primitiveTypeInfo) {
      switch (primitiveTypeInfo.getPrimitiveCategory()) {
      case INTERVAL_DAY_TIME, TIMESTAMP:
        throw new UnsupportedOperationException("Unsupported primitive type used in list: " + elementType);
      default:
        // supported
      }
    } else {
      throw new UnsupportedOperationException("Unsupported type used in list: " + elementType);
    }
  }
}
