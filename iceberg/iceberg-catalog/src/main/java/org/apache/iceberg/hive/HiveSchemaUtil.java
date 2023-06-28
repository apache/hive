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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
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
    return convert(fieldSchemas, false);
  }

  /**
   * Converts a Hive schema (list of FieldSchema objects) to an Iceberg schema.
   * @param fieldSchemas The list of the columns
   * @param autoConvert If <code>true</code> then TINYINT and SMALLINT is converted to INTEGER and VARCHAR and CHAR is
   *                    converted to STRING. Otherwise if these types are used in the Hive schema then exception is
   *                    thrown.
   * @return An equivalent Iceberg Schema
   */
  public static Schema convert(List<FieldSchema> fieldSchemas, boolean autoConvert) {
    List<String> names = Lists.newArrayListWithExpectedSize(fieldSchemas.size());
    List<TypeInfo> typeInfos = Lists.newArrayListWithExpectedSize(fieldSchemas.size());
    List<String> comments = Lists.newArrayListWithExpectedSize(fieldSchemas.size());

    for (FieldSchema col : fieldSchemas) {
      names.add(col.getName().toLowerCase());
      typeInfos.add(TypeInfoUtils.getTypeInfoFromTypeString(col.getType()));
      comments.add(col.getComment());
    }
    return HiveSchemaConverter.convert(names, typeInfos, comments, autoConvert);
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
    return HiveSchemaConverter.convert(names, types, comments, false);
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
    return HiveSchemaConverter.convert(names, types, comments, autoConvert);
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
   * @param typeInfo The Hive type
   * @return The Iceberg type
   */
  public static Type convert(TypeInfo typeInfo) {
    return HiveSchemaConverter.convert(typeInfo, false);
  }

  /**
   * Returns a SchemaDifference containing those fields which are present in only one of the collections, as well as
   * those fields which are present in both (in terms of the name) but their type or comment has changed.
   * @param minuendCollection Collection of fields to subtract from
   * @param subtrahendCollection Collection of fields to subtract
   * @param bothDirections Whether or not to compute the missing fields from the minuendCollection as well
   * @return the difference between the two schemas
   */
  public static SchemaDifference getSchemaDiff(Collection<FieldSchema> minuendCollection,
                                               Collection<FieldSchema> subtrahendCollection, boolean bothDirections) {
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
      SchemaDifference otherWay = getSchemaDiff(subtrahendCollection, minuendCollection, false);
      otherWay.getMissingFromSecond().forEach(difference::addMissingFromFirst);
    }

    return difference;
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

    public boolean isEmpty() {
      return missingFromFirst.isEmpty() && missingFromSecond.isEmpty() && typeChanged.isEmpty() &&
          commentChanged.isEmpty();
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
  }


  private static String convertToTypeString(Type type) {
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
      default:
        throw new UnsupportedOperationException(type + " is not supported");
    }
  }
}
