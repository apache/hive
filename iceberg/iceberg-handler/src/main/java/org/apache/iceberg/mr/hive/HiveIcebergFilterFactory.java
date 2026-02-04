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

package org.apache.iceberg.mr.hive;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.ql.io.sarg.ExpressionTree;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentImpl;
import org.apache.hadoop.hive.ql.parse.TransformSpec;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.iceberg.common.DynFields;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.NaNUtil;

import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.isNaN;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.or;


public class HiveIcebergFilterFactory {

  private HiveIcebergFilterFactory() {
  }

  public static Expression generateFilterExpression(SearchArgument sarg) {
    return translate(sarg.getExpression(), sarg.getLeaves());
  }

  /**
   * Recursive method to traverse down the ExpressionTree to evaluate each expression and its leaf nodes.
   * @param tree Current ExpressionTree where the 'top' node is being evaluated.
   * @param leaves List of all leaf nodes within the tree.
   * @return Expression that is translated from the Hive SearchArgument.
   */
  private static Expression translate(ExpressionTree tree, List<PredicateLeaf> leaves) {
    List<ExpressionTree> childNodes = tree.getChildren();
    switch (tree.getOperator()) {
      case OR:
        Expression orResult = Expressions.alwaysFalse();
        for (ExpressionTree child : childNodes) {
          orResult = or(orResult, translate(child, leaves));
        }
        return orResult;
      case AND:
        Expression result = Expressions.alwaysTrue();
        for (ExpressionTree child : childNodes) {
          result = and(result, translate(child, leaves));
        }
        return result;
      case NOT:
        return not(translate(childNodes.get(0), leaves));
      case LEAF:
        if (tree.getLeaf() >= leaves.size()) {
          throw new UnsupportedOperationException("No more leaves are available");
        }
        return translateLeaf(leaves.get(tree.getLeaf()));
      case CONSTANT:
        throw new UnsupportedOperationException("CONSTANT operator is not supported");
      default:
        throw new UnsupportedOperationException("Unknown operator: " + tree.getOperator());
    }
  }

  /**
   * Translate leaf nodes from Hive operator to Iceberg operator.
   * @param leaf Leaf node
   * @return Expression fully translated from Hive PredicateLeaf
   */
  private static Expression translateLeaf(PredicateLeaf leaf) {
    TransformSpec transformSpec = TransformSpec.fromStringWithColumnName(leaf.getColumnName());
    UnboundTerm<Object> column = SchemaUtils.toTerm(transformSpec);

    switch (leaf.getOperator()) {
      case EQUALS:
        Object literal = leafToLiteral(leaf, transformSpec);
        return NaNUtil.isNaN(literal) ? isNaN(column) : equal(column, literal);
      case LESS_THAN:
        return lessThan(column, leafToLiteral(leaf, transformSpec));
      case LESS_THAN_EQUALS:
        return lessThanOrEqual(column, leafToLiteral(leaf, transformSpec));
      case IN:
        return in(column, leafToLiteralList(leaf));
      case BETWEEN:
        List<Object> icebergLiterals = leafToLiteralList(leaf);
        if (icebergLiterals.size() == 2) {
          return and(greaterThanOrEqual(column, icebergLiterals.get(0)),
              lessThanOrEqual(column, icebergLiterals.get(1)));
        } else {
          // In case semijoin reduction optimization was applied, there will be a BETWEEN( DynamicValue, DynamicValue)
          // clause, where DynamicValue is not evaluable in Tez AM, where Hive filter is translated into Iceberg filter.
          // Overwriting to constant true as the optimization will be utilized by Hive/Tez and no-op for Iceberg.
          // (Also: the original filter and Iceberg filter are both part of JobConf on the execution side.)
          return Expressions.alwaysTrue();
        }
      case IS_NULL:
        return isNull(column);
      default:
        throw new UnsupportedOperationException("Unknown operator: " + leaf.getOperator());
    }
  }

  // PredicateLeafImpl has a work-around for Kryo serialization with java.util.Date objects where it converts values to
  // Timestamp using Date#getTime. This conversion discards microseconds, so this is a necessary to avoid it.
  private static final DynFields.UnboundField<?> LITERAL_FIELD = DynFields.builder()
      .hiddenImpl(SearchArgumentImpl.PredicateLeafImpl.class, "literal")
      .build();

  private static Object leafToLiteral(PredicateLeaf leaf, TransformSpec transform) {
    switch (leaf.getType()) {
      case LONG:
      case BOOLEAN:
      case FLOAT:
        return leaf.getLiteral();
      case STRING:
        return convertLiteral(leaf.getLiteral(), transform);
      case DATE:
        if (leaf.getLiteral() instanceof Date) {
          return daysFromDate((Date) leaf.getLiteral());
        }
        return daysFromTimestamp((Timestamp) leaf.getLiteral());
      case TIMESTAMP:
        return microsFromTimestamp((Timestamp) LITERAL_FIELD.get(leaf));
      case DECIMAL:
        return hiveDecimalToBigDecimal((HiveDecimalWritable) leaf.getLiteral());

      default:
        throw new UnsupportedOperationException("Unknown type: " + leaf.getType());
    }
  }

  private static Object convertLiteral(Object literal, TransformSpec transform) {
    if (transform == null) {
      return literal;
    }
    try {
      switch (transform.getTransformType()) {
        case YEAR:
          return parseYearToTransformYear(literal.toString());
        case MONTH:
          return parseMonthToTransformMonth(literal.toString());
        case DAY:
          return parseDayToTransformMonth(literal.toString());
        case HOUR:
          return parseHourToTransformHour(literal.toString());
        case TRUNCATE:
          return Transforms.truncate(transform.getTransformParam()).bind(Types.StringType.get())
              .apply(literal.toString());
        case BUCKET:
          return Transforms.bucket(transform.getTransformParam()).bind(Types.StringType.get())
              .apply(literal.toString());
        case IDENTITY:
          return literal;
        default:
          throw new UnsupportedOperationException("Unknown transform: " + transform.getTransformType());
      }
    } catch (NumberFormatException | DateTimeException | IllegalStateException e) {
      throw new RuntimeException(
          String.format("Unable to parse value '%s' as '%s' transform value", literal.toString(), transform));
    }
  }

  public static Object convertPartitionLiteral(Object literal, TransformSpec transform) {
    if (transform == null) {
      return literal;
    }
    try {
      switch (transform.getTransformType()) {
        case YEAR:
          return parseYearToTransformYear(literal.toString());
        case MONTH:
          return parseMonthToTransformMonth(literal.toString());
        case HOUR:
          return parseHourToTransformHour(literal.toString());
        case DAY:
        case TRUNCATE:
        case BUCKET:
        case IDENTITY:
          return literal;
        default:
          throw new UnsupportedOperationException("Unknown transform: " + transform.getTransformType());
      }
    } catch (NumberFormatException | DateTimeException | IllegalStateException e) {
      throw new RuntimeException(
          String.format("Unable to parse value '%s' as '%s' transform value", literal.toString(), transform));
    }
  }

  private static final int ICEBERG_EPOCH_YEAR = 1970;
  private static final int ICEBERG_EPOCH_MONTH = 1;

  /**
   * In the partition path years are represented naturally, e.g. 1984. However, we need
   * to convert it to an integer which represents the years from 1970. So, for 1984 the
   * return value should be 14.
   */
  private static Integer parseYearToTransformYear(String yearStr) {
    int year = Integer.parseInt(yearStr);
    return year - ICEBERG_EPOCH_YEAR;
  }

  /**
   * In the partition path months are represented as year-month, e.g. 2021-01. We
   * need to convert it to a single integer which represents the months from '1970-01'.
   */
  private static Integer parseMonthToTransformMonth(String monthStr) {
    String[] parts = monthStr.split("-", -1);
    Preconditions.checkState(parts.length == 2);
    int year = Integer.parseInt(parts[0]);
    int month = Integer.parseInt(parts[1]);
    int years = year - ICEBERG_EPOCH_YEAR;
    int months = month - ICEBERG_EPOCH_MONTH;
    return years * 12 + months;
  }

  /**
   * In the partition path days are represented as year-month-day, e.g. 2023-12-12.
   * This functions converts this string to an integer which represents the days from
   * '1970-01-01' with the help of Iceberg's type converter.
   */
  private static Integer parseDayToTransformMonth(String monthStr) {
    Literal<Integer> days = Literal.of(monthStr).to(Types.DateType.get());
    return days.value();
  }

  /**
   * In the partition path hours are represented as year-month-day-hour, e.g.
   * 1970-01-01-01. We need to convert it to a single integer which represents the hours
   * from '1970-01-01 00:00:00'.
   */
  private static Integer parseHourToTransformHour(String hourStr) {
    final OffsetDateTime epoch = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
    String[] parts = hourStr.split("-", -1);
    Preconditions.checkState(parts.length == 4);
    int year = Integer.parseInt(parts[0]);
    int month = Integer.parseInt(parts[1]);
    int day = Integer.parseInt(parts[2]);
    int hour = Integer.parseInt(parts[3]);
    OffsetDateTime datetime = OffsetDateTime.of(LocalDateTime.of(year, month, day, hour, /* minute=*/0),
        ZoneOffset.UTC);
    return (int) ChronoUnit.HOURS.between(epoch, datetime);
  }

  private static List<Object> leafToLiteralList(PredicateLeaf leaf) {
    switch (leaf.getType()) {
      case LONG:
      case BOOLEAN:
      case FLOAT:
      case STRING:
        return leaf.getLiteralList();
      case DATE:
        return leaf.getLiteralList().stream().map(value -> daysFromDate((Date) value))
                .collect(Collectors.toList());
      case DECIMAL:
        return leaf.getLiteralList().stream()
                .map(value -> hiveDecimalToBigDecimal((HiveDecimalWritable) value))
                .collect(Collectors.toList());
      case TIMESTAMP:
        return leaf.getLiteralList().stream()
                .map(value -> microsFromTimestamp((Timestamp) value))
                .collect(Collectors.toList());
      default:
        throw new UnsupportedOperationException("Unknown type: " + leaf.getType());
    }
  }

  private static BigDecimal hiveDecimalToBigDecimal(HiveDecimalWritable hiveDecimalWritable) {
    return hiveDecimalWritable.getHiveDecimal().bigDecimalValue().setScale(hiveDecimalWritable.scale());
  }

  // Hive uses `java.sql.Date.valueOf(lit.toString());` to convert a literal to Date
  // Which uses `java.util.Date()` internally to create the object and that uses the TimeZone.getDefaultRef()
  // To get back the expected date we have to use the LocalDate which gets rid of the TimeZone misery as it uses
  // the year/month/day to generate the object
  private static int daysFromDate(Date date) {
    return DateTimeUtil.daysFromDate(date.toLocalDate());
  }

  // Hive uses `java.sql.Timestamp.valueOf(lit.toString());` to convert a literal to Timestamp
  // Which again uses `java.util.Date()` internally to create the object which uses the TimeZone.getDefaultRef()
  // To get back the expected timestamp we have to use the LocalDateTime which gets rid of the TimeZone misery
  // as it uses the year/month/day/hour/min/sec/nanos to generate the object
  private static int daysFromTimestamp(Timestamp timestamp) {
    return DateTimeUtil.daysFromDate(timestamp.toLocalDateTime().toLocalDate());
  }

  // We have to use the LocalDateTime to get the micros. See the comment above.
  private static long microsFromTimestamp(Timestamp timestamp) {
    return DateTimeUtil.microsFromInstant(timestamp.toInstant());
  }
}
