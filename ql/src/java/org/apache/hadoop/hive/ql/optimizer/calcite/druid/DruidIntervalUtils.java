/**
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
package org.apache.hadoop.hive.ql.optimizer.calcite.druid;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang.StringUtils;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

/** 
 * Utilities for generating intervals from RexNode.
 * 
 * Based on Navis logic implemented on Hive data structures.
 * See <a href="https://github.com/druid-io/druid/pull/2880">Druid PR-2880</a>
 * 
 */
@SuppressWarnings({"rawtypes","unchecked"})
public class DruidIntervalUtils {

  protected static final Logger LOG = LoggerFactory.getLogger(DruidIntervalUtils.class);


  /**
   * Given a list of predicates, it generates the equivalent Interval
   * (if possible). It assumes that all the predicates in the input
   * reference a single column : the timestamp column.
   * 
   * @param conjs list of conditions to use for the transformation
   * @return interval representing the conditions in the input list
   */
  public static List<Interval> createInterval(RelDataType type, List<RexNode> conjs) {
    List<Range> ranges = new ArrayList<>();
    for (RexNode child : conjs) {
      List<Range> extractedRanges = extractRanges(type, child, false);
      if (extractedRanges == null || extractedRanges.isEmpty()) {
        // We could not extract, we bail out
        return null;
      }
      if (ranges.isEmpty()) {
        ranges.addAll(extractedRanges);
        continue;
      }
      List<Range> overlapped = Lists.newArrayList();
      for (Range current : ranges) {
        for (Range interval : extractedRanges) {
          if (current.isConnected(interval)) {
            overlapped.add(current.intersection(interval));
          }
        }
      }
      ranges = overlapped;
    }
    List<Range> compactRanges = condenseRanges(ranges);
    LOG.debug("Inferred ranges on interval : " + compactRanges);
    return toInterval(compactRanges);
  }

  protected static List<Interval> toInterval(List<Range> ranges) {
    List<Interval> intervals = Lists.transform(ranges, new Function<Range, Interval>() {
      @Override
      public Interval apply(Range range) {
        if (!range.hasLowerBound() && !range.hasUpperBound()) {
          return DruidTable.DEFAULT_INTERVAL;
        }
        long start = range.hasLowerBound() ? toLong(range.lowerEndpoint()) :
          DruidTable.DEFAULT_INTERVAL.getStartMillis();
        long end = range.hasUpperBound() ? toLong(range.upperEndpoint()) :
          DruidTable.DEFAULT_INTERVAL.getEndMillis();
        if (range.hasLowerBound() && range.lowerBoundType() == BoundType.OPEN) {
          start++;
        }
        if (range.hasUpperBound() && range.upperBoundType() == BoundType.CLOSED) {
          end++;
        }
        return new Interval(start, end);
      }
    });
    LOG.info("Converted time ranges " + ranges + " to interval " + intervals);
    return intervals;
  }

  protected static List<Range> extractRanges(RelDataType type, RexNode node,
          boolean withNot) {
    switch (node.getKind()) {
      case EQUALS:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
      case BETWEEN:
      case IN:
        return leafToRanges(type, (RexCall) node, withNot);

      case NOT:
        return extractRanges(type, ((RexCall) node).getOperands().get(0), !withNot);

      case OR:
        RexCall call = (RexCall) node;
        List<Range> intervals = Lists.newArrayList();
        for (RexNode child : call.getOperands()) {
          List<Range> extracted = extractRanges(type, child, withNot);
          if (extracted != null) {
            intervals.addAll(extracted);
          }
        }
        return intervals;

      default:
        return null;
    }
  }

  protected static List<Range> leafToRanges(RelDataType type, RexCall call,
          boolean withNot) {
    switch (call.getKind()) {
      case EQUALS:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
      {
        RexLiteral literal = null;
        if (call.getOperands().get(0) instanceof RexInputRef &&
                call.getOperands().get(1) instanceof RexLiteral) {
          literal = extractLiteral(call.getOperands().get(1));
        } else if (call.getOperands().get(0) instanceof RexInputRef &&
                call.getOperands().get(1).getKind() == SqlKind.CAST) {
          literal = extractLiteral(call.getOperands().get(1));
        } else if (call.getOperands().get(1) instanceof RexInputRef &&
                call.getOperands().get(0) instanceof RexLiteral) {
          literal = extractLiteral(call.getOperands().get(0));
        } else if (call.getOperands().get(1) instanceof RexInputRef &&
                call.getOperands().get(0).getKind() == SqlKind.CAST) {
          literal = extractLiteral(call.getOperands().get(0));
        }
        if (literal == null) {
          return null;
        }
        Comparable value = literalToType(literal, type);
        if (value == null) {
          return null;
        }
        if (call.getKind() == SqlKind.LESS_THAN) {
          return Arrays.<Range> asList(withNot ? Range.atLeast(value) : Range.lessThan(value));
        } else if (call.getKind() == SqlKind.LESS_THAN_OR_EQUAL) {
          return Arrays.<Range> asList(withNot ? Range.greaterThan(value) : Range.atMost(value));
        } else if (call.getKind() == SqlKind.GREATER_THAN) {
          return Arrays.<Range> asList(withNot ? Range.atMost(value) : Range.greaterThan(value));
        } else if (call.getKind() == SqlKind.GREATER_THAN_OR_EQUAL) {
          return Arrays.<Range> asList(withNot ? Range.lessThan(value) : Range.atLeast(value));
        } else { //EQUALS
          if (!withNot) {
            return Arrays.<Range> asList(Range.closed(value, value));
          }
          return Arrays.<Range> asList(Range.lessThan(value), Range.greaterThan(value));
        }
      }
      case BETWEEN:
      {
        RexLiteral literal1 = extractLiteral(call.getOperands().get(2));
        if (literal1 == null) {
          return null;
        }
        RexLiteral literal2 = extractLiteral(call.getOperands().get(3));
        if (literal2 == null) {
          return null;
        }
        Comparable value1 = literalToType(literal1, type);
        Comparable value2 = literalToType(literal2, type);
        if (value1 == null || value2 == null) {
          return null;
        }
        boolean inverted = value1.compareTo(value2) > 0;
        if (!withNot) {
          return Arrays.<Range> asList(
                  inverted ? Range.closed(value2, value1) : Range.closed(value1, value2));
        }
        return Arrays.<Range> asList(Range.lessThan(inverted ? value2 : value1),
                Range.greaterThan(inverted ? value1 : value2));
      }
      case IN:
      {
        List<Range> ranges = Lists.newArrayList();
        for (int i = 1; i < call.getOperands().size(); i++) {
          RexLiteral literal = extractLiteral(call.getOperands().get(i));
          if (literal == null) {
            return null;
          }
          Comparable element = literalToType(literal, type);
          if (element == null) {
            return null;
          }
          if (withNot) {
            ranges.addAll(
                    Arrays.<Range> asList(Range.lessThan(element), Range.greaterThan(element)));
          } else {
            ranges.add(Range.closed(element, element));
          }
        }
        return ranges;
      }
      default:
        return null;
    }
  }

  @SuppressWarnings("incomplete-switch")
  protected static Comparable literalToType(RexLiteral literal, RelDataType type) {
    // Extract
    Object value = null;
    switch (literal.getType().getSqlTypeName()) {
      case DATE:
      case TIME:
      case TIMESTAMP:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_DAY_TIME:
        value = literal.getValue();
        break;
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case DOUBLE:
      case DECIMAL:
      case FLOAT:
      case REAL:
      case VARCHAR:
      case CHAR:
      case BOOLEAN:
        value = literal.getValue3();
    }
    if (value == null) {
      return null;
    }

    // Convert
    switch (type.getSqlTypeName()) {
      case BIGINT:
        return toLong(value);
      case INTEGER:
        return toInt(value);
      case FLOAT:
        return toFloat(value);
      case DOUBLE:
        return toDouble(value);
      case VARCHAR:
      case CHAR:
        return String.valueOf(value);
      case TIMESTAMP:
        return toTimestamp(value);
    }
    return null;
  }

  private static RexLiteral extractLiteral(RexNode node) {
    RexNode target = node;
    if (node.getKind() == SqlKind.CAST) {
      target = ((RexCall)node).getOperands().get(0);
    }
    if (!(target instanceof RexLiteral)) {
      return null;
    }
    return (RexLiteral) target;
  }

  private static Comparable toTimestamp(Object literal) {
    if (literal instanceof Timestamp) {
      return (Timestamp) literal;
    }
    if (literal instanceof Date) {
      return new Timestamp(((Date) literal).getTime());
    }
    if (literal instanceof Number) {
      return new Timestamp(((Number) literal).longValue());
    }
    if (literal instanceof String) {
      String string = (String) literal;
      if (StringUtils.isNumeric(string)) {
        return new Timestamp(Long.valueOf(string));
      }
      try {
        return Timestamp.valueOf(string);
      } catch (NumberFormatException e) {
        // ignore
      }
    }
    return null;
  }

  private static Long toLong(Object literal) {
    if (literal instanceof Number) {
      return ((Number) literal).longValue();
    }
    if (literal instanceof Date) {
      return ((Date) literal).getTime();
    }
    if (literal instanceof Timestamp) {
      return ((Timestamp) literal).getTime();
    }
    if (literal instanceof String) {
      try {
        return Long.valueOf((String) literal);
      } catch (NumberFormatException e) {
        // ignore
      }
      try {
        return DateFormat.getDateInstance().parse((String) literal).getTime();
      } catch (ParseException e) {
        // best effort. ignore
      }
    }
    return null;
  }


  private static Integer toInt(Object literal) {
    if (literal instanceof Number) {
      return ((Number) literal).intValue();
    }
    if (literal instanceof String) {
      try {
        return Integer.valueOf((String) literal);
      } catch (NumberFormatException e) {
        // ignore
      }
    }
    return null;
  }

  private static Float toFloat(Object literal) {
    if (literal instanceof Number) {
      return ((Number) literal).floatValue();
    }
    if (literal instanceof String) {
      try {
        return Float.valueOf((String) literal);
      } catch (NumberFormatException e) {
        // ignore
      }
    }
    return null;
  }

  private static Double toDouble(Object literal) {
    if (literal instanceof Number) {
      return ((Number) literal).doubleValue();
    }
    if (literal instanceof String) {
      try {
        return Double.valueOf((String) literal);
      } catch (NumberFormatException e) {
        // ignore
      }
    }
    return null;
  }

  protected static List<Range> condenseRanges(List<Range> ranges) {
    if (ranges.size() <= 1) {
      return ranges;
    }

    Comparator<Range> startThenEnd = new Comparator<Range>() {
      @Override
      public int compare(Range lhs, Range rhs) {
        int compare = 0;
        if (lhs.hasLowerBound() && rhs.hasLowerBound()) {
          compare = lhs.lowerEndpoint().compareTo(rhs.lowerEndpoint());
        } else if (!lhs.hasLowerBound() && rhs.hasLowerBound()) {
          compare = -1;
        } else if (lhs.hasLowerBound() && !rhs.hasLowerBound()) {
          compare = 1;
        }
        if (compare != 0) {
          return compare;
        }
        if (lhs.hasUpperBound() && rhs.hasUpperBound()) {
          compare = lhs.upperEndpoint().compareTo(rhs.upperEndpoint());
        } else if (!lhs.hasUpperBound() && rhs.hasUpperBound()) {
          compare = -1;
        } else if (lhs.hasUpperBound() && !rhs.hasUpperBound()) {
          compare = 1;
        }
        return compare;
      }
    };

    TreeSet<Range> sortedIntervals = Sets.newTreeSet(startThenEnd);
    sortedIntervals.addAll(ranges);

    List<Range> retVal = Lists.newArrayList();

    Iterator<Range> intervalsIter = sortedIntervals.iterator();
    Range currInterval = intervalsIter.next();
    while (intervalsIter.hasNext()) {
      Range next = intervalsIter.next();
      if (currInterval.encloses(next)) {
        continue;
      }
      if (mergeable(currInterval, next)) {
        currInterval = currInterval.span(next);
      } else {
        retVal.add(currInterval);
        currInterval = next;
      }
    }
    retVal.add(currInterval);

    return retVal;
  }

  protected static boolean mergeable(Range range1, Range range2) {
    Comparable x1 = range1.upperEndpoint();
    Comparable x2 = range2.lowerEndpoint();
    int compare = x1.compareTo(x2);
    return compare > 0 || (compare == 0 && range1.upperBoundType() == BoundType.CLOSED
            && range2.lowerBoundType() == BoundType.CLOSED);
  }

  public static long extractTotalTime(List<Interval> intervals) {
    long totalTime = 0;
    for (Interval interval : intervals) {
      totalTime += (interval.getEndMillis() - interval.getStartMillis());
    }
    return totalTime;
  }

}
