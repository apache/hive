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

package org.apache.iceberg.parquet;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.mr.hive.variant.VariantPathUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.RowGroupFilter;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 * Parquet row-group pruning for Iceberg {@code VARIANT} predicates when shredding is enabled.
 *
 * <p>Hive can produce Iceberg expressions that reference shredded VARIANT pseudo-columns (for example
 * {@code payload.typed_value.tier}) or use {@code extract(payload, "$.tier", "string")}.
 *
 * <p>This class converts those predicates into Parquet filter2 predicates against the physical Parquet columns
 * (for example {@code payload.typed_value.tier.typed_value}) and uses {@link RowGroupFilter} to evaluate row groups.
 *
 * <p><strong>Correctness:</strong> pruning must not introduce false negatives. Because shredding can fall back to the
 * serialized {@code ...value} column, a row group rejected by the Parquet predicate is only dropped when all relevant
 * fallback {@code ...value} columns are provably all-null for that row group.
 *
 * <p>This is an optimization only; Hive still evaluates the full predicate after reading.
 */
public final class VariantParquetFilters {

  private VariantParquetFilters() {
  }

  static FilterCompat.Filter toParquetFilter(MessageType schema, Expression expression) {
    ResolvedVariantFilter resolved = resolveVariantFilter(schema, expression);
    return resolved == null ? null : FilterCompat.get(resolved.predicate());
  }

  /** Single predicate built from one variant field. */
  private record ResolvedVariantPredicate(FilterPredicate predicate, ColumnPath fallbackValueColumn) {
  }

  /** Aggregated filter built from entire expression tree. */
  private record ResolvedVariantFilter(FilterPredicate predicate, List<ColumnPath> fallbackValueColumns) {
  }

  private static ResolvedVariantFilter resolveVariantFilter(MessageType schema, Expression expression) {
    if (schema == null || expression == null) {
      return null;
    }

    VariantPredicateToParquetVisitor visitor = new VariantPredicateToParquetVisitor(schema);
    FilterPredicate predicate = ExpressionVisitors.visit(expression, visitor);
    if (predicate == null) {
      return null;
    }
    return new ResolvedVariantFilter(predicate, visitor.fallbackValueColumns());
  }

  public static boolean[] variantRowGroupMayMatch(
      MessageType fileSchema, Expression filter, List<BlockMetaData> rowGroups) {
    if (fileSchema == null || filter == null || rowGroups == null || rowGroups.isEmpty()) {
      return null;
    }

    ResolvedVariantFilter resolved = resolveVariantFilter(fileSchema, filter);
    if (resolved == null) {
      return null;
    }

    FilterCompat.Filter parquetFilter = FilterCompat.get(resolved.predicate());
    List<BlockMetaData> matchingRowGroups = RowGroupFilter.filterRowGroups(parquetFilter, rowGroups, fileSchema);
    Set<BlockMetaData> matchingSet = Collections.newSetFromMap(new IdentityHashMap<>());
    matchingSet.addAll(matchingRowGroups);

    Set<ColumnPath> fallbackValueColumns = Sets.newHashSet(resolved.fallbackValueColumns());
    boolean[] mayMatch = new boolean[rowGroups.size()];

    for (int i = 0; i < rowGroups.size(); i++) {
      BlockMetaData rowGroup = rowGroups.get(i);
      mayMatch[i] = matchingSet.contains(rowGroup) || mayMatchViaFallback(rowGroup, fallbackValueColumns);
    }

    return mayMatch;
  }

  private static boolean mayMatchViaFallback(BlockMetaData rowGroup, Set<ColumnPath> fallbackValueColumns) {
    if (fallbackValueColumns.isEmpty()) {
      return true;
    }

    for (ColumnChunkMetaData col : rowGroup.getColumns()) {
      if (fallbackValueColumns.contains(col.getPath()) && !isColumnAllNull(col)) {
        return true;
      }
    }
    return false;
  }

  private static List<BlockMetaData> pruneVariantRowGroups(
      MessageType fileSchema, Expression filter, List<BlockMetaData> rowGroups) {
    boolean[] mayMatch = variantRowGroupMayMatch(fileSchema, filter, rowGroups);
    if (mayMatch == null) {
      return rowGroups;
    }

    List<BlockMetaData> kept = Lists.newArrayListWithCapacity(rowGroups.size());
    for (int i = 0; i < rowGroups.size(); i++) {
      if (mayMatch[i]) {
        kept.add(rowGroups.get(i));
      }
    }

    return kept.size() == rowGroups.size() ? rowGroups : kept;
  }

  /** Returns Parquet metadata with row groups pruned using best-effort VARIANT pruning. */
  public static ParquetMetadata pruneVariantRowGroups(
      ParquetMetadata parquetMetadata, MessageType fileSchema, Expression filter) {
    if (parquetMetadata == null || filter == null) {
      return parquetMetadata;
    }

    List<BlockMetaData> rowGroups = parquetMetadata.getBlocks();
    if (rowGroups == null || rowGroups.isEmpty()) {
      return parquetMetadata;
    }

    MessageType schema = fileSchema;
    if (schema == null) {
      if (parquetMetadata.getFileMetaData() == null) {
        return parquetMetadata;
      }
      schema = parquetMetadata.getFileMetaData().getSchema();
    }
    if (schema == null) {
      return parquetMetadata;
    }

    List<BlockMetaData> kept = pruneVariantRowGroups(schema, filter, rowGroups);
    if (kept == rowGroups || parquetMetadata.getFileMetaData() == null) {
      return parquetMetadata;
    }

    return new ParquetMetadata(parquetMetadata.getFileMetaData(), kept);
  }

  private static boolean isColumnAllNull(ColumnChunkMetaData meta) {
    if (meta == null || meta.getStatistics() == null || !meta.getStatistics().isNumNullsSet()) {
      return false;
    }

    return meta.getStatistics().getNumNulls() == meta.getValueCount();
  }

  private static final class VariantPredicateToParquetVisitor
      extends ExpressionVisitors.ExpressionVisitor<FilterPredicate> {
    private final MessageType schema;
    private final List<ColumnPath> fallbackValueColumns = Lists.newArrayList();

    private VariantPredicateToParquetVisitor(MessageType schema) {
      this.schema = schema;
    }

    List<ColumnPath> fallbackValueColumns() {
      return fallbackValueColumns;
    }

    @Override
    public FilterPredicate alwaysTrue() {
      return null;
    }

    @Override
    public FilterPredicate alwaysFalse() {
      return null;
    }

    @Override
    public FilterPredicate not(FilterPredicate child) {
      return child == null ? null : FilterApi.not(child);
    }

    @Override
    public FilterPredicate and(FilterPredicate left, FilterPredicate right) {
      // For AND, partial pushdown is safe: unconvertible predicates are evaluated by Hive after reading.
      if (left == null) {
        return right;
      } else if (right == null) {
        return left;
      }
      return FilterApi.and(left, right);
    }

    @Override
    public FilterPredicate or(FilterPredicate left, FilterPredicate right) {
      // For OR, dropping an unconvertible side is unsafe: it may filter out rows that should match.
      if (left == null || right == null) {
        return null;
      }
      return FilterApi.or(left, right);
    }

    @Override
    public <T> FilterPredicate predicate(UnboundPredicate<T> predicate) {
      if (!isSupportedOperation(predicate.op())) {
        return null;
      }

      VariantColumnPath columnPath = extractVariantColumnPath(predicate);
      if (columnPath == null) {
        return null;
      }

      // Handle unary predicates (IS_NULL, NOT_NULL)
      if (predicate.op() == Operation.IS_NULL ||
          predicate.op() == Operation.NOT_NULL) {
        ResolvedVariantPredicate resolved = resolveVariantNullPredicate(
            schema, columnPath.variantPath, columnPath.typedValuePath, predicate.op());
        if (resolved == null) {
          return null;
        }
        fallbackValueColumns.add(resolved.fallbackValueColumn());
        return resolved.predicate();
      }

      // Handle literal predicates (EQ, NOT_EQ, LT, etc.)
      Literal<T> literal = predicate.literal();
      if (literal == null || literal.value() == null) {
        return null;
      }

      ResolvedVariantPredicate resolved = resolveVariantValuePredicate(
          schema, columnPath.variantPath, columnPath.typedValuePath, predicate.op(), literal.value());
      if (resolved == null) {
        return null;
      }

      fallbackValueColumns.add(resolved.fallbackValueColumn());
      return resolved.predicate();
    }

    private static boolean isSupportedOperation(Operation op) {
      return switch (op) {
        case EQ, NOT_EQ, LT, LT_EQ, GT, GT_EQ, IS_NULL, NOT_NULL -> true;
        default -> false;
      };
    }

    private record VariantColumnPath(List<String> variantPath, List<String> typedValuePath) {
    }

    private static VariantColumnPath extractVariantColumnPath(UnboundPredicate<?> predicate) {
      String shreddedColumnPath = VariantPathUtil.extractVariantShreddedColumn(predicate);
      if (shreddedColumnPath == null) {
        return null;
      }

      int typedValueIdx = shreddedColumnPath.indexOf(VariantPathUtil.TYPED_VALUE_SEGMENT);
      if (typedValueIdx < 0) {
        return null;
      }

      String variantPathStr = shreddedColumnPath.substring(0, typedValueIdx);
      String typedValuePathStr =
          shreddedColumnPath.substring(typedValueIdx + VariantPathUtil.TYPED_VALUE_SEGMENT.length());

      List<String> variantPath = VariantPathUtil.splitPath(variantPathStr);
      List<String> typedValuePath = typedValuePathStr.isEmpty() ?
          Collections.emptyList() : VariantPathUtil.splitPath(typedValuePathStr);

      return new VariantColumnPath(variantPath, typedValuePath);
    }
  }

  private static ResolvedVariantPredicate resolveVariantNullPredicate(
      MessageType schema, List<String> variantPath, List<String> typedValuePath, Operation op) {
    return resolveVariantPredicate(schema, variantPath, typedValuePath, op, null);
  }

  private static ResolvedVariantPredicate resolveVariantValuePredicate(
      MessageType schema, List<String> variantPath, List<String> typedValuePath,
      Operation op, Object literalValue) {
    return resolveVariantPredicate(schema, variantPath, typedValuePath, op, literalValue);
  }

  private static ResolvedVariantPredicate resolveVariantPredicate(
      MessageType schema, List<String> variantPath, List<String> typedValuePath,
      Operation op, Object literalValue) {

    if (CollectionUtils.isEmpty(variantPath) || CollectionUtils.isEmpty(typedValuePath)) {
      return null;
    }
    GroupType variantGroup = resolveVariantGroup(schema, variantPath);
    if (variantGroup == null) {
      return null;
    }
    GroupType currentGroup = rootTypedValueGroup(variantGroup);
    if (currentGroup == null) {
      return null;
    }

    List<String> columnPathSegments =
        Lists.newArrayListWithCapacity(variantPath.size() + typedValuePath.size() + 2);
    columnPathSegments.addAll(variantPath);
    columnPathSegments.add(ParquetVariantVisitor.TYPED_VALUE);

    for (int i = 0; i < typedValuePath.size(); i++) {
      String fieldName = typedValuePath.get(i);
      GroupType objectFieldGroup = resolveObjectFieldGroup(currentGroup, fieldName);
      if (objectFieldGroup == null) {
        return null;
      }

      columnPathSegments.add(fieldName);

      boolean last = i == typedValuePath.size() - 1;
      if (last) {
        PrimitiveType leaf = leafTypedValuePrimitive(objectFieldGroup);
        if (leaf == null) {
          return null;
        }
        columnPathSegments.add(ParquetVariantVisitor.TYPED_VALUE);

        // Dispatch to appropriate builder based on whether we have a literal value
        if (literalValue == null) {
          return buildNullPredicate(columnPathSegments, op);
        } else {
          return buildValuePredicate(columnPathSegments, leaf, op, literalValue);
        }
      }

      currentGroup = objectFieldGroup;
    }

    return null;
  }

  private static GroupType resolveVariantGroup(MessageType schema, List<String> variantPath) {
    try {
      Type variantType = schema.getType(variantPath.toArray(new String[0]));
      if (variantType == null || variantType.isPrimitive()) {
        return null;
      }
      return variantType.asGroupType();
    } catch (RuntimeException e) {
      return null;
    }
  }

  private static GroupType rootTypedValueGroup(GroupType variantGroup) {
    Type rootTypedValue = ParquetSchemaUtil.fieldType(variantGroup, ParquetVariantVisitor.TYPED_VALUE);
    // Root typed_value is not an object; cannot resolve an object path.
    return rootTypedValue != null && !rootTypedValue.isPrimitive() ? rootTypedValue.asGroupType() : null;
  }

  private static GroupType resolveObjectFieldGroup(GroupType parent, String fieldName) {
    Type fieldType = ParquetSchemaUtil.fieldType(parent, fieldName);
    return fieldType != null && !fieldType.isPrimitive() ? fieldType.asGroupType() : null;
  }

  private static PrimitiveType leafTypedValuePrimitive(GroupType objectFieldGroup) {
    Type leaf = ParquetSchemaUtil.fieldType(objectFieldGroup, ParquetVariantVisitor.TYPED_VALUE);
    return leaf != null && leaf.isPrimitive() ? leaf.asPrimitiveType() : null;
  }

  private static ResolvedVariantPredicate buildNullPredicate(
      List<String> typedValueColumnPathSegments, Operation op) {
    String typedValueColumnPath = String.join(".", typedValueColumnPathSegments);

    FilterPredicate predicate = (op == Operation.IS_NULL) ?
        FilterApi.eq(
            FilterApi.binaryColumn(typedValueColumnPath),
            null) :
        FilterApi.notEq(
            FilterApi.binaryColumn(typedValueColumnPath),
            null);

    return toResolvedPredicate(typedValueColumnPathSegments, predicate);
  }

  private static ResolvedVariantPredicate buildValuePredicate(
      List<String> typedValueColumnPathSegments, PrimitiveType typedValueLeaf,
      Operation op, Object literalValue) {
    String typedValueColumnPath = String.join(".", typedValueColumnPathSegments);

    FilterPredicate predicate = PrimitivePredicateFactory.toParquetPredicate(
        typedValueLeaf, typedValueColumnPath, op, literalValue);

    return toResolvedPredicate(typedValueColumnPathSegments, predicate);
  }

  private static ResolvedVariantPredicate toResolvedPredicate(
      List<String> typedValueColumnPathSegments, FilterPredicate predicate) {
    if (predicate == null) {
      return null;
    }
    String[] fallbackValuePathSegments = typedValueColumnPathSegments.toArray(new String[0]);
    fallbackValuePathSegments[fallbackValuePathSegments.length - 1] = ParquetVariantVisitor.VALUE;
    ColumnPath fallbackValueColumn = ColumnPath.get(fallbackValuePathSegments);

    return new ResolvedVariantPredicate(predicate, fallbackValueColumn);
  }

  private static final class PrimitivePredicateFactory {

    private static FilterPredicate toParquetPredicate(
        PrimitiveType primitive, String columnPath, Operation op, Object value) {
      switch (primitive.getPrimitiveTypeName()) {
        case BINARY:
          Binary binaryValue = toBinary(primitive, value);
          return binaryValue == null ? null : compare(op, FilterApi.binaryColumn(columnPath), binaryValue);
        case BOOLEAN:
          if (!(value instanceof Boolean)) {
            return null;
          }
          if (op == Operation.EQ) {
            return FilterApi.eq(FilterApi.booleanColumn(columnPath), (Boolean) value);
          } else if (op == Operation.NOT_EQ) {
            return FilterApi.notEq(FilterApi.booleanColumn(columnPath), (Boolean) value);
          }
          return null;
        case INT32: // Handles INT32 and DATE
          Integer intValue = asInt32(value);
          return intValue == null ? null : compare(op, FilterApi.intColumn(columnPath), intValue);
        case INT64: // Handles INT64, TIMESTAMP_MILLIS, TIMESTAMP_MICROS
          Long longValue = asInt64(value);
          return longValue == null ? null : compare(op, FilterApi.longColumn(columnPath), longValue);
        case FLOAT:
          Float floatValue = asFloat(op, value);
          return floatValue == null ? null : compare(op, FilterApi.floatColumn(columnPath), floatValue);
        case DOUBLE:
          if (!(value instanceof Number)) {
            return null;
          }
          return compare(op, FilterApi.doubleColumn(columnPath), ((Number) value).doubleValue());
        default:
          return null;
      }
    }

    private static Binary toBinary(PrimitiveType primitive, Object value) {
      if (value instanceof Binary binary) {
        return binary;
      } else if (value instanceof ByteBuffer buffer) {
        return Binary.fromReusedByteBuffer(buffer);
      } else if (value instanceof byte[] bytes) {
        return Binary.fromReusedByteArray(bytes);
      } else if (value instanceof CharSequence) {
        if (!(primitive.getLogicalTypeAnnotation() instanceof StringLogicalTypeAnnotation)) {
          return null;
        }
        return Binary.fromString(value.toString());
      }
      return null;
    }

    private static Integer asInt32(Object value) {
      Long longValue = toLongExact(value);
      if (longValue == null || longValue < Integer.MIN_VALUE || longValue > Integer.MAX_VALUE) {
        return null;
      }
      return longValue.intValue();
    }

    private static Long asInt64(Object value) {
      return toLongExact(value);
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private static Long toLongExact(Object value) {
      if (!(value instanceof Number)) {
        return null;
      }
      if (value instanceof Double doubleValue) {
        if (!Double.isFinite(doubleValue) || doubleValue != Math.rint(doubleValue)) {
          return null;
        }
        if (doubleValue < Long.MIN_VALUE || doubleValue > Long.MAX_VALUE) {
          return null;
        }
        long longValue = doubleValue.longValue();
        return ((double) longValue == doubleValue) ? longValue : null;
      } else if (value instanceof Float floatValue) {
        if (!Float.isFinite(floatValue) || floatValue != Math.rint(floatValue)) {
          return null;
        }
        double doubleValue = floatValue.doubleValue();
        if (doubleValue < Long.MIN_VALUE || doubleValue > Long.MAX_VALUE) {
          return null;
        }
        long longValue = (long) doubleValue;
        return ((double) longValue == doubleValue) ? longValue : null;
      }
      return ((Number) value).longValue();
    }

    private static Float asFloat(Operation op, Object value) {
      if (!(value instanceof Number)) {
        return null;
      }
      double doubleValue = ((Number) value).doubleValue();
      if (!Double.isFinite(doubleValue)) {
        return null;
      }
      float floatValue = (float) doubleValue;
      if (op == Operation.EQ || op == Operation.NOT_EQ) {
        return ((double) floatValue == doubleValue) ? floatValue : null;
      }
      return switch (op) {
        case GT, GT_EQ -> {
          if ((double) floatValue > doubleValue) {
            floatValue = Math.nextDown(floatValue);
          }
          yield floatValue;
        }
        case LT, LT_EQ -> {
          if ((double) floatValue < doubleValue) {
            floatValue = Math.nextUp(floatValue);
          }
          yield floatValue;
        }
        default -> null;
      };
    }

    @SuppressWarnings("checkstyle:MethodTypeParameterName")
    private static <C extends Comparable<C>, COL extends Operators.Column<C> & Operators.SupportsLtGt>
        FilterPredicate compare(Operation op, COL column, C value) {
      return switch (op) {
        case EQ -> FilterApi.eq(column, value);
        case NOT_EQ -> FilterApi.notEq(column, value);
        case LT -> FilterApi.lt(column, value);
        case LT_EQ -> FilterApi.ltEq(column, value);
        case GT -> FilterApi.gt(column, value);
        case GT_EQ -> FilterApi.gtEq(column, value);
        default -> null;
      };
    }
  }
}
