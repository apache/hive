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

package org.apache.iceberg.mr.hive.variant;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;

/**
 * Utility class for handling Variant Projection Pushdown (Column Pruning).
 */
public final class VariantProjectionUtil {

  private VariantProjectionUtil() {
  }

  public record VariantColumnDescriptor(
      int rootColumnIndex,
      int[] fieldPath,
      String[] physicalPath,
      Type prunedSchema) {
  }

  public record VariantProjection(
      List<VariantColumnDescriptor> variantColumns,
      List<ColumnDescriptor> requestedColumns) {

    public static VariantProjection create(
        MessageType fileSchema, Configuration conf, Schema icebergSchema) {
      String columns = conf.get(IOConstants.COLUMNS);
      if (columns == null || columns.isEmpty()) {
        return null;
      }

      List<String> columnNames = DataWritableReadSupport.getColumnNames(columns);
      if (columnNames == null || columnNames.isEmpty()) {
        return null;
      }

      boolean readAll = ColumnProjectionUtils.isReadAllColumns(conf);
      List<Integer> readColumnIds = ColumnProjectionUtils.getReadColumnIDs(conf);
      Set<String> nestedPaths = ColumnProjectionUtils.getNestedColumnPaths(conf);

      List<VariantColumnDescriptor> variantColumns =
          discoverVariantColumns(fileSchema, icebergSchema, columnNames, readAll, readColumnIds, nestedPaths);
      if (variantColumns.isEmpty()) {
        return null;
      }

      List<ColumnDescriptor> requestedColumns = computeRequestedColumns(fileSchema, variantColumns);
      if (requestedColumns.isEmpty()) {
        return null;
      }

      return new VariantProjection(variantColumns, requestedColumns);
    }
  }

  private static List<VariantColumnDescriptor> discoverVariantColumns(
      MessageType fileSchema,
      Schema icebergSchema,
      List<String> columnNames,
      boolean readAll,
      List<Integer> readColumnIds,
      Set<String> nestedPaths) {

    boolean[] projected = projectedTopLevelColumns(readAll, readColumnIds, columnNames.size());

    List<VariantColumnDescriptor> result = Lists.newArrayList();
    for (int colIndex = 0; colIndex < columnNames.size(); colIndex++) {
      if (!projected[colIndex]) {
        continue;
      }

      String columnName = columnNames.get(colIndex);
      // Resolve the logical Iceberg field (handling potential name mismatches from Hive)
      Types.NestedField field = findIcebergField(icebergSchema, columnName, colIndex);
      if (field == null || field.type() == null) {
        continue;
      }

      // Resolve the physical Parquet type (handling schema evolution/IDs)
      Type parquetType = findParquetType(fileSchema, field);
      if (parquetType == null) {
        continue;
      }

      collectVariantColumns(
          colIndex,
          field.type(),
          parquetType,
          new int[0],
          new String[] { columnName },
          new String[] { parquetType.getName() },
          result,
          nestedPaths);
    }

    return result;
  }

  /**
   * Resolves the Iceberg field for the given Hive column.
   * Prioritizes lookup by name, falling back to positional index if not found (e.g. rename).
   */
  private static Types.NestedField findIcebergField(Schema schema, String name, int index) {
    Types.NestedField field = schema.findField(name);
    if (field != null) {
      return field;
    }

    // Fallback: If Hive's column configuration uses an old name (e.g. "payload") but the Iceberg schema
    // uses the new name (e.g. "data"), try to match by position if the index is valid.
    List<Types.NestedField> columns = schema.columns();
    if (index >= 0 && index < columns.size()) {
      return columns.get(index);
    }

    return null;
  }

  /**
   * Resolves the Parquet Type for the given Iceberg field.
   * Prioritizes lookup by Field ID (for schema evolution), falling back to name.
   */
  private static Type findParquetType(GroupType parent, Types.NestedField child) {
    int id = child.fieldId();
    for (Type candidate : parent.getFields()) {
      if (candidate.getId() != null && candidate.getId().intValue() == id) {
        return candidate;
      }
    }

    String name = child.name();
    if (name != null && !name.isEmpty() && parent.containsField(name)) {
      return parent.getType(name);
    }

    return null;
  }

  private static boolean[] projectedTopLevelColumns(
      boolean readAll, List<Integer> readColumnIds, int fieldCount) {
    boolean[] projected = new boolean[fieldCount];
    if (readAll) {
      Arrays.fill(projected, true);
      return projected;
    }

    if (readColumnIds == null || readColumnIds.isEmpty()) {
      return projected;
    }

    for (Integer id : readColumnIds) {
      if (id != null && id >= 0 && id < fieldCount) {
        projected[id] = true;
      }
    }

    return projected;
  }

  private static void collectVariantColumns(
      int rootColumnIndex,
      org.apache.iceberg.types.Type icebergType,
      Type parquetType,
      int[] fieldPath,
      String[] logicalPath,
      String[] physicalPath,
      List<VariantColumnDescriptor> results,
      Set<String> nestedPaths) {

    switch (icebergType.typeId()) {
      case VARIANT:
        VariantColumnDescriptor variantColumn =
            getOrCreateVariantColumn(
                rootColumnIndex, parquetType, fieldPath, logicalPath, physicalPath, nestedPaths);
        if (variantColumn != null) {
          results.add(variantColumn);
        }
        return;

      case STRUCT:
        collectFromStruct(
            rootColumnIndex,
            icebergType.asStructType(),
            parquetType,
            fieldPath,
            logicalPath,
            physicalPath,
            results,
            nestedPaths);
        return;

      default:
        // VARIANT shredding is not applied within arrays or maps, or other types
    }
  }

  private static void collectFromStruct(
      int rootColumnIndex,
      Types.StructType structType,
      Type parquetType,
      int[] fieldPath,
      String[] logicalPath,
      String[] physicalPath,
      List<VariantColumnDescriptor> results,
      Set<String> nestedPaths) {

    if (parquetType.isPrimitive()) {
      return;
    }

    GroupType parquetGroup = parquetType.asGroupType();
    List<Types.NestedField> fields = structType.fields();

    for (int i = 0; i < fields.size(); i++) {
      Types.NestedField field = fields.get(i);
      Type nestedParquetType = findParquetType(parquetGroup, field);
      if (nestedParquetType == null) {
        continue;
      }

      collectVariantColumns(
          rootColumnIndex,
          field.type(),
          nestedParquetType,
          append(fieldPath, i),
          append(logicalPath, field.name()),
          append(physicalPath, nestedParquetType.getName()),
          results,
          nestedPaths);
    }
  }

  private static VariantColumnDescriptor getOrCreateVariantColumn(
      int rootColumnIndex,
      Type parquetType,
      int[] fieldPath,
      String[] logicalPath,
      String[] physicalPath,
      Set<String> nestedPaths) {
    if (parquetType.isPrimitive()) {
      return null;
    }

    GroupType variantGroup = parquetType.asGroupType();
    if (!variantGroup.containsField(VariantPathUtil.TYPED_VALUE)) {
      // If there is no typed_value, variant is unshredded. Hive's reader is sufficient.
      return null;
    }

    Set<String> projectedPaths = computeProjectedPaths(logicalPath, nestedPaths);

    // Prune the schema to match the projection so that the Reader only expects columns that will be present in the I/O
    // We use the logical projection paths to prune the physical schema structure
    Type prunedSchema = new VariantSchemaPruner(projectedPaths).prune(parquetType);

    return new VariantColumnDescriptor(
        rootColumnIndex, fieldPath, physicalPath, prunedSchema);
  }

  private static Set<String> computeProjectedPaths(String[] logicalPath, Set<String> nestedPaths) {
    if (nestedPaths == null || nestedPaths.isEmpty()) {
      return null;
    }

    String prefix = String.join(".", logicalPath).toLowerCase() + ".";
    Set<String> projectedPaths = Sets.newHashSet();
    for (String path : nestedPaths) {
      if (path.startsWith(prefix)) {
        projectedPaths.add(path.substring(prefix.length()));
      }
    }

    if (projectedPaths.isEmpty()) {
      // No paths matched this column in global nestedPaths: implies full column read.
      // Set to null to enable fast path in computeRequestedColumns.
      return null;
    }

    return projectedPaths;
  }

  private static int[] append(int[] path, int index) {
    int[] copy = Arrays.copyOf(path, path.length + 1);
    copy[path.length] = index;
    return copy;
  }

  private static String[] append(String[] path, String segment) {
    String[] copy = Arrays.copyOf(path, path.length + 1);
    copy[path.length] = segment;
    return copy;
  }

  private static List<ColumnDescriptor> computeRequestedColumns(
      MessageType fileSchema, List<VariantColumnDescriptor> variantColumns) {
    // Build the list of Parquet leaf columns needed to read all requested VARIANT columns.
    List<ColumnDescriptor> requested = Lists.newArrayList();
    for (ColumnDescriptor desc : fileSchema.getColumns()) {
      String[] pathParts = desc.getPath();
      if (pathParts == null || pathParts.length == 0) {
        continue;
      }

      for (VariantColumnDescriptor vc : variantColumns) {
        if (startsWith(pathParts, vc.physicalPath())) {
          // Check if path exists in vc.prunedSchema()
          if (containsPath(vc.prunedSchema(), pathParts, vc.physicalPath().length)) {
            requested.add(desc);
          }
          break;
        }
      }
    }

    return requested;
  }

  private static boolean containsPath(Type root, String[] path, int offset) {
    Type current = root;
    for (int i = offset; i < path.length; i++) {
      if (current.isPrimitive()) {
        return false;
      }
      GroupType group = current.asGroupType();
      if (!group.containsField(path[i])) {
        return false;
      }
      current = group.getType(path[i]);
    }
    return true;
  }

  private static boolean startsWith(String[] path, String[] prefix) {
    if (path == null || prefix == null || path.length < prefix.length) {
      return false;
    }
    for (int i = 0; i < prefix.length; i++) {
      if (!Objects.equals(path[i], prefix[i])) {
        return false;
      }
    }
    return true;
  }

  /**
   * Helper class to prune the Parquet physical schema of a Variant column based on projected paths.
   * Handles the specific structure of Shredded Variants (metadata, value, typed_value).
   */
  private static class VariantSchemaPruner {
    private final ProjectionNode rootNode;

    VariantSchemaPruner(Set<String> projectedPaths) {
      this.rootNode = buildProjectionTree(projectedPaths);
    }

    Type prune(Type rootType) {
      if (rootNode == null) {
        return rootType;
      }
      return prune(rootType, rootNode, true);
    }

    private Type prune(Type type, ProjectionNode node, boolean isVariantWrapper) {
      if (shouldStopPruning(type, node)) {
        return type;
      }

      GroupType group = type.asGroupType();
      List<Type> newFields = Lists.newArrayList();
      boolean changed = false;

      for (Type child : group.getFields()) {
        Type prunedChild = null;

        if (isVariantWrapper) {
          // Mode 1: Variant Wrapper (metadata, value, typed_value)
          String name = child.getName();
          if (VariantPathUtil.METADATA.equals(name) || VariantPathUtil.VALUE.equals(name)) {
            prunedChild = child;
          } else if (VariantPathUtil.TYPED_VALUE.equals(name)) {
            // Recurse into typed_value, switching to Logical Mode (isVariantWrapper = false)
            // 'typed_value' is a container, so we pass the same 'node' down
            prunedChild = prune(child, node, false);
          }
        } else {
          // Mode 2: Logical Structure (structs, typed_value content)
          ProjectionNode childNode = node.children.get(child.getName().toLowerCase());
          if (childNode != null) {
            // Recurse, checking if the child itself is a nested Variant Wrapper
            prunedChild = prune(child, childNode, isVariantWrapper(child));
          }
        }

        if (isValid(prunedChild)) {
          newFields.add(prunedChild);
          if (prunedChild != child) {
            changed = true;
          }
        } else {
          changed = true; // Dropped
        }
      }

      if (!changed) {
        return type;
      }

      return group.withNewFields(newFields);
    }

    private boolean shouldStopPruning(Type type, ProjectionNode node) {
      if (node.isSelected || type.isPrimitive()) {
        return true;
      }

      OriginalType originalType = type.getOriginalType();
      return originalType == OriginalType.LIST || originalType == OriginalType.MAP;
    }

    private boolean isVariantWrapper(Type type) {
      if (type.isPrimitive()) {
        return false;
      }
      return type.asGroupType().containsField(VariantPathUtil.TYPED_VALUE);
    }

    private boolean isValid(Type child) {
      return child != null && (child.isPrimitive() || !child.asGroupType().getFields().isEmpty());
    }

    private static ProjectionNode buildProjectionTree(Set<String> paths) {
      if (paths == null) {
        return null;
      }

      ProjectionNode root = new ProjectionNode();
      for (String path : paths) {
        ProjectionNode current = root;
        int start = 0;
        int len = path.length();
        while (start < len) {
          int dot = path.indexOf('.', start);
          String segment = (dot == -1) ? path.substring(start) : path.substring(start, dot);
          start = (dot == -1) ? len : dot + 1;
          current = current.getOrCreate(segment.toLowerCase());
        }
        current.isSelected = true;
      }
      return root;
    }

    private static class ProjectionNode {
      private final Map<String, ProjectionNode> children = Maps.newHashMap();
      private boolean isSelected = false;

      ProjectionNode getOrCreate(String segment) {
        return children.computeIfAbsent(segment, k -> new ProjectionNode());
      }
    }
  }
}
