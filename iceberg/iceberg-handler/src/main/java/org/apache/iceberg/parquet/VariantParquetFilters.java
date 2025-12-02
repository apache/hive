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

package org.apache.iceberg.parquet;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public final class VariantParquetFilters {

  private static final String TYPED_VALUE_SEGMENT =
      "." + ParquetVariantVisitor.TYPED_VALUE + ".";

  private VariantParquetFilters() {
  }

  public static FilterCompat.Filter convert(MessageType schema, Expression expression) {
    if (schema == null || expression == null) {
      return null;
    }

    FilterPredicate predicate =
        ExpressionVisitors.visit(expression, new ExpressionToPredicate(schema));
    return predicate == null ? null : FilterCompat.get(predicate);
  }

  private static class ExpressionToPredicate
      extends ExpressionVisitors.ExpressionVisitor<FilterPredicate> {
    private final MessageType schema;

    ExpressionToPredicate(MessageType schema) {
      this.schema = schema;
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
      if (left == null) {
        return right;
      } else if (right == null) {
        return left;
      }
      return FilterApi.and(left, right);
    }

    @Override
    public FilterPredicate or(FilterPredicate left, FilterPredicate right) {
      if (left == null) {
        return right;
      } else if (right == null) {
        return left;
      }
      return FilterApi.or(left, right);
    }

    @Override
    public <T> FilterPredicate predicate(UnboundPredicate<T> predicate) {
      if (predicate.op() != Expression.Operation.EQ) {
        return null;
      }

      String column = predicate.ref().name();
      int typedIdx = column.indexOf(TYPED_VALUE_SEGMENT);
      if (typedIdx < 0) {
        return null;
      }

      Literal<T> literal = predicate.literal();
      if (literal == null || literal.value() == null) {
        return null;
      }

      String variantPathString = column.substring(0, typedIdx);
      String typedRemainder = column.substring(typedIdx + TYPED_VALUE_SEGMENT.length());

      List<String> variantPath = splitPath(variantPathString);
      List<String> typedPath =
          typedRemainder.isEmpty() ?
              Collections.emptyList() :
              Arrays.asList(typedRemainder.split("\\."));

      try {
        Type variantType = schema.getType(variantPath.toArray(new String[0]));
        return ParquetVariantVisitor.visit(
            variantType.asGroupType(),
            new VariantFilterVisitor(variantPath, typedPath, literal.value()));
      } catch (RuntimeException e) {
        return null;
      }
    }

    private static List<String> splitPath(String path) {
      if (path == null || path.isEmpty()) {
        return Collections.emptyList();
      }
      return Arrays.asList(path.split("\\."));
    }
  }

  private static class VariantFilterVisitor extends ParquetVariantVisitor<FilterPredicate> {
    private final String[] basePath;
    private final List<String> targetPath;
    private final Object literalValue;
    private final Deque<String> fieldNames = new ArrayDeque<>();

    VariantFilterVisitor(List<String> basePath, List<String> targetPath, Object literalValue) {
      this.basePath = basePath.toArray(new String[0]);
      this.targetPath = targetPath;
      this.literalValue = literalValue;
    }

    @Override
    public void beforeField(Type type) {
      fieldNames.addLast(type.getName());
    }

    @Override
    public void afterField(Type type) {
      fieldNames.removeLast();
    }

    @Override
    public FilterPredicate variant(
        GroupType variant, FilterPredicate metadataResult, FilterPredicate valueResult) {
      return valueResult;
    }

    @Override
    public FilterPredicate metadata(PrimitiveType metadata) {
      return null;
    }

    @Override
    public FilterPredicate serialized(PrimitiveType value) {
      return null;
    }

    @Override
    public FilterPredicate primitive(PrimitiveType primitive) {
      if (!matchesTargetPath()) {
        return null;
      }

      String columnPath = String.join(".", currentPath());
      Object value = literalValue;

      switch (primitive.getPrimitiveTypeName()) {
        case BINARY:
          Binary binaryValue;
          if (value instanceof Binary) {
            binaryValue = (Binary) value;
          } else if (value instanceof byte[]) {
            binaryValue = Binary.fromReusedByteArray((byte[]) value);
          } else if (value instanceof CharSequence) {
            binaryValue = Binary.fromString(value.toString());
          } else {
            return null;
          }
          return FilterApi.eq(FilterApi.binaryColumn(columnPath), binaryValue);
        case BOOLEAN:
          if (!(value instanceof Boolean)) {
            return null;
          }
          return FilterApi.eq(FilterApi.booleanColumn(columnPath), (Boolean) value);
        case INT32:
          if (!(value instanceof Number)) {
            return null;
          }
          return FilterApi.eq(FilterApi.intColumn(columnPath), ((Number) value).intValue());
        case INT64:
          if (!(value instanceof Number)) {
            return null;
          }
          return FilterApi.eq(FilterApi.longColumn(columnPath), ((Number) value).longValue());
        case FLOAT:
          if (!(value instanceof Number)) {
            return null;
          }
          return FilterApi.eq(FilterApi.floatColumn(columnPath), ((Number) value).floatValue());
        case DOUBLE:
          if (!(value instanceof Number)) {
            return null;
          }
          return FilterApi.eq(FilterApi.doubleColumn(columnPath), ((Number) value).doubleValue());
        default:
          return null;
      }
    }

    @Override
    public FilterPredicate value(
        GroupType group, FilterPredicate valueResult, FilterPredicate typedResult) {
      return typedResult != null ? typedResult : valueResult;
    }

    @Override
    public FilterPredicate object(
        GroupType group, FilterPredicate valueResult, List<FilterPredicate> fieldResults) {
      for (FilterPredicate result : fieldResults) {
        if (result != null) {
          return result;
        }
      }
      return valueResult;
    }

    @Override
    public FilterPredicate array(
        GroupType array, FilterPredicate valueResult, FilterPredicate elementResult) {
      return elementResult != null ? elementResult : valueResult;
    }

    private boolean matchesTargetPath() {
      if (fieldNames.size() != targetPath.size() + 2) {
        return false;
      }
      Iterator<String> iterator = fieldNames.iterator();
      if (!Objects.equals(iterator.next(), ParquetVariantVisitor.TYPED_VALUE)) {
        return false;
      }
      for (String segment : targetPath) {
        if (!iterator.hasNext() || !Objects.equals(iterator.next(), segment)) {
          return false;
        }
      }
      return iterator.hasNext() && Objects.equals(iterator.next(), ParquetVariantVisitor.TYPED_VALUE);
    }

    private String[] currentPath() {
      return Stream.concat(Arrays.stream(basePath), fieldNames.stream()).toArray(String[]::new);
    }
  }
}
