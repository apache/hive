/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.hive;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.Pair;


public final class HiveSchemaUtil {

  private HiveSchemaUtil() {
  }

  /**
   * Converts the Iceberg schema to a Hive schema (list of FieldSchema objects).
   * @param schema The original Iceberg schema to convert
   * @return The Hive column list generated from the Iceberg schema
   */
  public static List<FieldSchema> convert(Schema schema) {
    return schema.columns().stream()
        .map(col -> new FieldSchema(col.name(), convertToTypeString(col.type()), col.doc()))
        .collect(Collectors.toList());
  }

  /**
   * Converts a Hive schema (list of FieldSchema objects) to an Iceberg schema. If some of the types are not convertible
   * then exception is thrown.
   * @param fieldSchemas The list of the columns
   * @return An equivalent Iceberg Schema
   */
  public static Schema convert(List<FieldSchema> fieldSchemas) {
    return convert(fieldSchemas, Collections.emptyMap(), false);
  }

  /**
   * Converts a Hive schema (list of FieldSchema objects) to an Iceberg schema.
   *
   * @param fieldSchemas  The list of the columns
   * @param defaultValues Default values for columns, if any. The map is from column name to default value.
   * @param autoConvert   If <code>true</code> then TINYINT and SMALLINT is converted to INTEGER and VARCHAR and CHAR is
   *                      converted to STRING. Otherwise if these types are used in the Hive schema then exception is
   *                      thrown.
   * @return An equivalent Iceberg Schema
   */
  public static Schema convert(List<FieldSchema> fieldSchemas, Map<String, String> defaultValues, boolean autoConvert) {
    List<String> names = Lists.newArrayListWithExpectedSize(fieldSchemas.size());
    List<TypeInfo> typeInfos = Lists.newArrayListWithExpectedSize(fieldSchemas.size());
    List<String> comments = Lists.newArrayListWithExpectedSize(fieldSchemas.size());

    for (FieldSchema col : fieldSchemas) {
      names.add(col.getName().toLowerCase());
      typeInfos.add(TypeInfoUtils.getTypeInfoFromTypeString(col.getType()));
      comments.add(col.getComment());
    }
    return HiveSchemaConverter.convert(names, typeInfos, comments, autoConvert, defaultValues);
  }

  /**
   * Converts the Hive partition columns to Iceberg identity partition specification.
   * @param schema The Iceberg schema
   * @param fieldSchemas The partition column specification
   * @return The Iceberg partition specification
   */
  public static PartitionSpec spec(Schema schema, List<FieldSchema> fieldSchemas) {
    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
    fieldSchemas.forEach(fieldSchema -> builder.identity(fieldSchema.getName().toLowerCase()));
    return builder.build();
  }

  /**
   * Converts the Hive list of column names and column types to an Iceberg schema. If some of the types are not
   * convertible then exception is thrown.
   * @param names The list of the Hive column names
   * @param types The list of the Hive column types
   * @param comments The list of the Hive column comments
   * @return The Iceberg schema
   */
  public static Schema convert(List<String> names, List<TypeInfo> types, List<String> comments) {
    return HiveSchemaConverter.convert(names, types, comments, false, Collections.emptyMap());
  }

  /**
   * Converts the Hive list of column names and column types to an Iceberg schema.
   * @param names The list of the Hive column names
   * @param types The list of the Hive column types
   * @param comments The list of the Hive column comments, can be null
   * @param autoConvert If <code>true</code> then TINYINT and SMALLINT is converted to INTEGER and VARCHAR and CHAR is
   *                    converted to STRING. Otherwise if these types are used in the Hive schema then exception is
   *                    thrown.
   * @return The Iceberg schema
   */
  public static Schema convert(List<String> names, List<TypeInfo> types, List<String> comments, boolean autoConvert) {
    return HiveSchemaConverter.convert(names, types, comments, autoConvert, Collections.emptyMap());
  }

  /**
   * Converts an Iceberg type to a Hive TypeInfo object.
   * @param type The Iceberg type
   * @return The Hive type
   */
  public static TypeInfo convert(Type type) {
    return TypeInfoUtils.getTypeInfoFromTypeString(convertToTypeString(type));
  }

  /**
   * Converts a Hive typeInfo object to an Iceberg type.
   *
   * @param typeInfo     The Hive type
   * @param defaultValue the default value for the column, if any
   * @return The Iceberg type
   */
  public static Type convert(TypeInfo typeInfo, String defaultValue) {
    return HiveSchemaConverter.convert(typeInfo, false, defaultValue);
  }

  /**
   * Returns a SchemaDifference containing those fields which are present in only one of the collections, as well as
   * those fields which are present in both (in terms of the name) but their type or comment has changed.
   *
   * @param minuendCollection    Collection of fields to subtract from
   * @param subtrahendCollection Collection of fields to subtract
   * @param schema               the iceberg table schema, if available. Used to compare default values
   * @param defaultValues        the column default values
   * @param bothDirections       Whether or not to compute the missing fields from the minuendCollection as well
   * @return the difference between the two schemas
   */
  public static SchemaDifference getSchemaDiff(Collection<FieldSchema> minuendCollection,
      Collection<FieldSchema> subtrahendCollection, Schema schema, Map<String, String> defaultValues,
      boolean bothDirections) {
    SchemaDifference difference = new SchemaDifference();

    for (FieldSchema first : minuendCollection) {
      boolean found = false;
      for (FieldSchema second : subtrahendCollection) {
        if (Objects.equals(first.getName(), second.getName())) {
          found = true;
          if (!Objects.equals(first.getType(), second.getType())) {
            difference.addTypeChanged(first);
          }
          if (!Objects.equals(first.getComment(), second.getComment())) {
            difference.addCommentChanged(first);
          }
        }
      }
      if (!found) {
        difference.addMissingFromSecond(first);
      }
    }

    if (bothDirections) {
      SchemaDifference otherWay = getSchemaDiff(subtrahendCollection, minuendCollection, null, defaultValues, false);
      otherWay.getMissingFromSecond().forEach(difference::addMissingFromFirst);
    }

    if (schema != null) {
      for (Types.NestedField field : schema.columns()) {
        if (!isRemovedField(field, difference.getMissingFromFirst())) {
          getDefaultValDiff(field, defaultValues, difference);
        }
      }
    }

    return difference;
  }

  private static boolean isRemovedField(Types.NestedField field, List<FieldSchema> missingFields) {
    for (FieldSchema fieldSchema : missingFields) {
      if (fieldSchema.getName().equalsIgnoreCase(field.name())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Computes whether the default value has changed for the given field.
   * @param field         the field to check for default value change
   * @param defaultValues the default values for the table schema, if available. Used to compare default values
   * @param difference    the SchemaDifference object to update with the default value change if any
   */
  private static void getDefaultValDiff(Types.NestedField field, Map<String, String> defaultValues,
      SchemaDifference difference) {

    String defaultStr = defaultValues.get(field.name());

    // Skip if no default at all
    if (defaultStr == null && field.writeDefault() == null) {
      return;
    }

    if (field.type().isPrimitiveType()) {
      Object expectedDefault = HiveSchemaUtil.getDefaultValue(defaultStr, field.type());
      if (expectedDefault instanceof Literal<?>) {
        expectedDefault = ((Literal<?>) expectedDefault).value();
      }
      if (!Objects.equals(expectedDefault, field.writeDefault())) {
        difference.addDefaultChanged(field, expectedDefault);
      }
    } else if (field.type().isStructType()) {
      Map<String, String> structDefaults = getDefaultValuesMap(defaultStr);

      for (Types.NestedField nested : field.type().asStructType().fields()) {
        getDefaultValDiff(nested, structDefaults, difference);
      }
    }
  }


  /**
   * Compares two lists of columns to each other to find the (singular) column that was moved. This works ideally for
   * identifying the column that was moved by an ALTER TABLE ... CHANGE COLUMN command.
   *
   * Note: This method is only suitable for finding a single reordered column.
   * Consequently, this method is NOT suitable for handling scenarios where multiple column reorders are possible at the
   * same time, such as ALTER TABLE ... REPLACE COLUMNS commands.
   *
   * @param updated The list of the columns after some updates have taken place (if any)
   * @param old The list of the original columns
   * @param renameMapping A map of name aliases for the updated columns (e.g. if a column rename occurred)
   * @return A pair consisting of the reordered column's name, and its preceding column's name (if any).
   *         Returns a null in case there are no out of order columns.
   */
  public static Pair<String, Optional<String>> getReorderedColumn(List<FieldSchema> updated,
                                                                        List<FieldSchema> old,
                                                                        Map<String, String> renameMapping) {
    // first collect the updated index for each column
    Map<String, Integer> nameToNewIndex = Maps.newHashMap();
    for (int i = 0; i < updated.size(); ++i) {
      String updatedCol = renameMapping.getOrDefault(updated.get(i).getName(), updated.get(i).getName());
      nameToNewIndex.put(updatedCol, i);
    }

    // find the column which has the highest index difference between its position in the old vs the updated list
    String reorderedColName = null;
    int maxIndexDiff = 0;
    for (int oldIndex = 0; oldIndex < old.size(); ++oldIndex) {
      String oldName = old.get(oldIndex).getName();
      Integer newIndex = nameToNewIndex.get(oldName);
      if (newIndex != null) {
        int indexDiff = Math.abs(newIndex - oldIndex);
        if (maxIndexDiff < indexDiff) {
          maxIndexDiff = indexDiff;
          reorderedColName = oldName;
        }
      }
    }

    if (maxIndexDiff == 0) {
      // if there are no changes in index, there were no reorders
      return null;
    } else {
      int newIndex = nameToNewIndex.get(reorderedColName);
      if (newIndex > 0) {
        // if the newIndex > 0, that means the column was moved after another column:
        // ALTER TABLE tbl CHANGE COLUMN reorderedColName reorderedColName type AFTER previousColName;
        String previousColName = renameMapping.getOrDefault(
            updated.get(newIndex - 1).getName(), updated.get(newIndex - 1).getName());
        return Pair.of(reorderedColName, Optional.of(previousColName));
      } else {
        // if the newIndex is 0, that means the column was moved to the first position:
        // ALTER TABLE tbl CHANGE COLUMN reorderedColName reorderedColName type FIRST;
        return Pair.of(reorderedColName, Optional.empty());
      }
    }
  }

  public static class SchemaDifference {
    private final List<FieldSchema> missingFromFirst = Lists.newArrayList();
    private final List<FieldSchema> missingFromSecond = Lists.newArrayList();
    private final List<FieldSchema> typeChanged = Lists.newArrayList();
    private final List<FieldSchema> commentChanged = Lists.newArrayList();
    private final Map<Types.NestedField, Object> defaultChanged = Maps.newHashMap();

    public List<FieldSchema> getMissingFromFirst() {
      return missingFromFirst;
    }

    public List<FieldSchema> getMissingFromSecond() {
      return missingFromSecond;
    }

    public List<FieldSchema> getTypeChanged() {
      return typeChanged;
    }

    public List<FieldSchema> getCommentChanged() {
      return commentChanged;
    }

    public Map<Types.NestedField, Object> getDefaultChanged() {
      return defaultChanged;
    }

    public boolean isEmpty() {
      return missingFromFirst.isEmpty() && missingFromSecond.isEmpty() && typeChanged.isEmpty() &&
          commentChanged.isEmpty() && defaultChanged.isEmpty();
    }

    void addMissingFromFirst(FieldSchema field) {
      missingFromFirst.add(field);
    }

    void addMissingFromSecond(FieldSchema field) {
      missingFromSecond.add(field);
    }

    void addTypeChanged(FieldSchema field) {
      typeChanged.add(field);
    }

    void addCommentChanged(FieldSchema field) {
      commentChanged.add(field);
    }

    void addDefaultChanged(Types.NestedField field, Object defaultValue) {
      defaultChanged.put(field, defaultValue);
    }
  }


  public static String convertToTypeString(Type type) {
    switch (type.typeId()) {
      case BOOLEAN:
        return "boolean";
      case INTEGER:
        return "int";
      case LONG:
        return "bigint";
      case FLOAT:
        return "float";
      case DOUBLE:
        return "double";
      case DATE:
        return "date";
      case TIME:
      case STRING:
      case UUID:
        return "string";
      case TIMESTAMP:
        Types.TimestampType timestampType = (Types.TimestampType) type;
        if (HiveVersion.min(HiveVersion.HIVE_3) && timestampType.shouldAdjustToUTC()) {
          return "timestamp with local time zone";
        }
        return "timestamp";
      case TIMESTAMP_NANO:
        Types.TimestampNanoType timestampNanoType = (Types.TimestampNanoType) type;
        if (timestampNanoType.shouldAdjustToUTC()) {
          return "timestamp with local time zone(9)";
        }
        return "timestamp(9)";
      case FIXED:
      case BINARY:
        return "binary";
      case DECIMAL:
        final Types.DecimalType decimalType = (Types.DecimalType) type;
        return String.format("decimal(%s,%s)", decimalType.precision(), decimalType.scale());
      case STRUCT:
        final Types.StructType structType = type.asStructType();
        final String nameToType = structType.fields().stream()
            .map(f -> String.format("%s:%s", f.name(), convert(f.type())))
            .collect(Collectors.joining(","));
        return String.format("struct<%s>", nameToType);
      case LIST:
        final Types.ListType listType = type.asListType();
        return String.format("array<%s>", convert(listType.elementType()));
      case MAP:
        final Types.MapType mapType = type.asMapType();
        return String.format("map<%s,%s>", convert(mapType.keyType()), convert(mapType.valueType()));
      case VARIANT:
        return "variant";
      default:
        throw new UnsupportedOperationException(type + " is not supported");
    }
  }

  public static void setDefaultValues(Record record, List<Types.NestedField> fields, Set<String> missingColumns) {
    for (Types.NestedField field : fields) {
      Object fieldValue = record.getField(field.name());

      if (fieldValue == null) {
        boolean isMissing = missingColumns.contains(field.name());

        if (isMissing) {
          if (field.type().isStructType()) {
            // Create struct and apply defaults to all nested fields
            Record nestedRecord = GenericRecord.create(field.type().asStructType());
            record.setField(field.name(), nestedRecord);
            // For nested fields, we consider ALL fields as "missing" to apply defaults
            setDefaultValuesForNestedStruct(nestedRecord, field.type().asStructType().fields());
          } else if (field.writeDefault() != null) {
            Object defaultValue = convertToWriteType(field.writeDefault(), field.type());
            record.setField(field.name(), defaultValue);
          }
        }
        // Explicit NULLs remain NULL
      } else if (field.type().isStructType() && fieldValue instanceof Record) {
        // For existing structs, apply defaults to any null nested fields
        setDefaultValuesForNestedStruct((Record) fieldValue, field.type().asStructType().fields());
      }
    }
  }

  /**
   * Sets a value into a {@link Record} using a struct-only field path (top-level column or nested
   * through structs). Intermediate struct records are created as needed.
   *
   * <p>If the path traverses a non-struct type (e.g. list/map), the operation is ignored.
   */
  public static void setStructField(Record root, String[] path, Object value) {
    if (root == null || path == null || path.length == 0) {
      return;
    }
    Record current = root;
    Types.StructType currentStruct = root.struct();

    for (int i = 0; i < path.length - 1; i++) {
      String fieldName = path[i];
      Types.NestedField field = currentStruct.field(fieldName);
      if (field == null || !field.type().isStructType()) {
        return;
      }
      Types.StructType nestedStruct = field.type().asStructType();
      current = getOrCreateStructRecord(current, fieldName, nestedStruct);
      currentStruct = nestedStruct;
    }

    current.setField(path[path.length - 1], value);
  }

  private static Record getOrCreateStructRecord(
      Record parent, String fieldName, Types.StructType structType) {
    Object value = parent.getField(fieldName);
    if (value instanceof Record) {
      return (Record) value;
    }
    Record record = GenericRecord.create(structType);
    parent.setField(fieldName, record);
    return record;
  }

  // Special method for nested structs that always applies defaults to null fields
  private static void setDefaultValuesForNestedStruct(Record record, List<Types.NestedField> fields) {
    for (Types.NestedField field : fields) {
      Object fieldValue = record.getField(field.name());

      if (fieldValue == null && field.writeDefault() != null) {
        // Always apply default to null fields in nested structs
        Object defaultValue = convertToWriteType(field.writeDefault(), field.type());
        record.setField(field.name(), defaultValue);
      } else if (field.type().isStructType() && fieldValue instanceof Record) {
        // Recursively process nested structs
        setDefaultValuesForNestedStruct((Record) fieldValue, field.type().asStructType().fields());
      }
    }
  }

  public static Object convertToWriteType(Object value, Type type) {
    if (value == null) {
      return null;
    }

    switch (type.typeId()) {
      case DATE:
        // Convert days since epoch (Integer) to LocalDate
        if (value instanceof Integer) {
          return DateTimeUtil.dateFromDays((Integer) value);
        }
        break;
      case TIMESTAMP:
        // Convert microseconds since epoch (Long) to LocalDateTime
        if (value instanceof Long) {
          Types.TimestampType timestampType = (Types.TimestampType) type;
          return timestampType.shouldAdjustToUTC() ?
              DateTimeUtil.timestamptzFromMicros((Long) value) :
              DateTimeUtil.timestampFromMicros((Long) value);
        }
        break;
      case TIMESTAMP_NANO:
        // Convert nanoseconds since epoch (Long) to LocalDateTime
        if (value instanceof Long) {
          Types.TimestampNanoType timestampNanoType = (Types.TimestampNanoType) type;
          return timestampNanoType.shouldAdjustToUTC() ?
              DateTimeUtil.timestamptzFromNanos((Long) value) :
              DateTimeUtil.timestampFromNanos((Long) value);
        }
        break;
      default:
        // For other types, no conversion needed
        return value;
    }

    return value; // fallback
  }

  public static Map<String, String> getDefaultValuesMap(String defaultValue) {
    if (StringUtils.isEmpty(defaultValue)) {
      return Collections.emptyMap();
    }
    // For Struct, the default value is expected to be in key:value format
    return Splitter.on(',').trimResults().withKeyValueSeparator(':').split(stripQuotes(defaultValue));
  }

  public static String stripQuotes(String val) {
    if (val.charAt(0) == '\'' && val.charAt(val.length() - 1) == '\'' ||
        val.charAt(0) == '"' && val.charAt(val.length() - 1) == '"') {
      return val.substring(1, val.length() - 1);
    }
    return val;
  }

  public static Object getDefaultValue(String defaultValue, Type type) {
    if (defaultValue == null) {
      return null;
    }
    return switch (type.typeId()) {
      case DATE, TIME, TIMESTAMP, TIMESTAMP_NANO ->
          Literal.of(stripQuotes(defaultValue)).to(type);
      default -> Conversions.fromPartitionString(type, stripQuotes(defaultValue));
    };
  }
}
