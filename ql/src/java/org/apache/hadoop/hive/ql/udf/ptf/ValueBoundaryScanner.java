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

package org.apache.hadoop.hive.ql.udf.ptf;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.ql.exec.PTFPartition;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.Order;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.BoundarySpec;
import org.apache.hadoop.hive.ql.plan.ptf.BoundaryDef;
import org.apache.hadoop.hive.ql.plan.ptf.OrderDef;
import org.apache.hadoop.hive.ql.plan.ptf.OrderExpressionDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

public abstract class ValueBoundaryScanner {
  BoundaryDef start, end;

  public ValueBoundaryScanner(BoundaryDef start, BoundaryDef end) {
    this.start = start;
    this.end = end;
  }

  public abstract int computeStart(int rowIdx, PTFPartition p) throws HiveException;

  public abstract int computeEnd(int rowIdx, PTFPartition p) throws HiveException;

  public static ValueBoundaryScanner getScanner(WindowFrameDef winFrameDef)
      throws HiveException {
    OrderDef orderDef = winFrameDef.getOrderDef();
    int numOrders = orderDef.getExpressions().size();
    if (numOrders != 1) {
      return new MultiValueBoundaryScanner(winFrameDef.getStart(), winFrameDef.getEnd(), orderDef);
    } else {
      return SingleValueBoundaryScanner.getScanner(winFrameDef.getStart(), winFrameDef.getEnd(), orderDef);
    }
  }
}

/*
 * - starting from the given rowIdx scan in the given direction until a row's expr
 * evaluates to an amt that crosses the 'amt' threshold specified in the BoundaryDef.
 */
abstract class SingleValueBoundaryScanner extends ValueBoundaryScanner {
  OrderExpressionDef expressionDef;

  public SingleValueBoundaryScanner(BoundaryDef start, BoundaryDef end, OrderExpressionDef expressionDef) {
    super(start, end);
    this.expressionDef = expressionDef;
  }

  /*
|  Use | Boundary1.type | Boundary1. amt | Sort Key | Order | Behavior                          |
| Case |                |                |          |       |                                   |
|------+----------------+----------------+----------+-------+-----------------------------------|
|   1. | PRECEDING      | UNB            | ANY      | ANY   | start = 0                         |
|   2. | PRECEDING      | unsigned int   | NULL     | ASC   | start = 0                         |
|   3. |                |                |          | DESC  | scan backwards to row R2          |
|      |                |                |          |       | such that R2.sk is not null       |
|      |                |                |          |       | start = R2.idx + 1                |
|   4. | PRECEDING      | unsigned int   | not NULL | DESC  | scan backwards until row R2       |
|      |                |                |          |       | such that R2.sk - R.sk > amt      |
|      |                |                |          |       | start = R2.idx + 1                |
|   5. | PRECEDING      | unsigned int   | not NULL | ASC   | scan backward until row R2        |
|      |                |                |          |       | such that R.sk - R2.sk > bnd1.amt |
|      |                |                |          |       | start = R2.idx + 1                |
|   6. | CURRENT ROW    |                | NULL     | ANY   | scan backwards until row R2       |
|      |                |                |          |       | such that R2.sk is not null       |
|      |                |                |          |       | start = R2.idx + 1                |
|   7. | CURRENT ROW    |                | not NULL | ANY   | scan backwards until row R2       |
|      |                |                |          |       | such R2.sk != R.sk                |
|      |                |                |          |       | start = R2.idx + 1                |
|   8. | FOLLOWING      | UNB            | ANY      | ANY   | Error                             |
|   9. | FOLLOWING      | unsigned int   | NULL     | DESC  | start = partition.size            |
|  10. |                |                |          | ASC   | scan forward until R2             |
|      |                |                |          |       | such that R2.sk is not null       |
|      |                |                |          |       | start = R2.idx                    |
|  11. | FOLLOWING      | unsigned int   | not NULL | DESC  | scan forward until row R2         |
|      |                |                |          |       | such that R.sk - R2.sk > amt      |
|      |                |                |          |       | start = R2.idx                    |
|  12. |                |                |          | ASC   | scan forward until row R2         |
|      |                |                |          |       | such that R2.sk - R.sk > amt      |
|------+----------------+----------------+----------+-------+-----------------------------------|
   */
  @Override
  public int computeStart(int rowIdx, PTFPartition p) throws HiveException {
    switch(start.getDirection()) {
    case PRECEDING:
      return computeStartPreceding(rowIdx, p);
    case CURRENT:
      return computeStartCurrentRow(rowIdx, p);
    case FOLLOWING:
      default:
        return computeStartFollowing(rowIdx, p);
    }
  }

  protected int computeStartPreceding(int rowIdx, PTFPartition p) throws HiveException {
    int amt = start.getAmt();
    // Use Case 1.
    if ( amt == BoundarySpec.UNBOUNDED_AMOUNT ) {
      return 0;
    }
    Object sortKey = computeValue(p.getAt(rowIdx));

    if ( sortKey == null ) {
      // Use Case 2.
      if ( expressionDef.getOrder() == Order.ASC ) {
        return 0;
      }
      else { // Use Case 3.
        while ( sortKey == null && rowIdx >= 0 ) {
          --rowIdx;
          if ( rowIdx >= 0 ) {
            sortKey = computeValue(p.getAt(rowIdx));
          }
        }
        return rowIdx+1;
      }
    }

    Object rowVal = sortKey;
    int r = rowIdx;

    // Use Case 4.
    if ( expressionDef.getOrder() == Order.DESC ) {
      while (r >= 0 && !isDistanceGreater(rowVal, sortKey, amt) ) {
        r--;
        if ( r >= 0 ) {
          rowVal = computeValue(p.getAt(r));
        }
      }
      return r + 1;
    }
    else { // Use Case 5.
      while (r >= 0 && !isDistanceGreater(sortKey, rowVal, amt) ) {
        r--;
        if ( r >= 0 ) {
          rowVal = computeValue(p.getAt(r));
        }
      }
      return r + 1;
    }
  }

  protected int computeStartCurrentRow(int rowIdx, PTFPartition p) throws HiveException {
    Object sortKey = computeValue(p.getAt(rowIdx));

    // Use Case 6.
    if ( sortKey == null ) {
      while ( sortKey == null && rowIdx >= 0 ) {
        --rowIdx;
        if ( rowIdx >= 0 ) {
          sortKey = computeValue(p.getAt(rowIdx));
        }
      }
      return rowIdx+1;
    }

    Object rowVal = sortKey;
    int r = rowIdx;

    // Use Case 7.
    while (r >= 0 && isEqual(rowVal, sortKey) ) {
      r--;
      if ( r >= 0 ) {
        rowVal = computeValue(p.getAt(r));
      }
    }
    return r + 1;
  }

  protected int computeStartFollowing(int rowIdx, PTFPartition p) throws HiveException {
    int amt = start.getAmt();
    Object sortKey = computeValue(p.getAt(rowIdx));

    Object rowVal = sortKey;
    int r = rowIdx;

    if ( sortKey == null ) {
      // Use Case 9.
      if ( expressionDef.getOrder() == Order.DESC) {
        return p.size();
      }
      else { // Use Case 10.
        while (r < p.size() && rowVal == null ) {
          r++;
          if ( r < p.size() ) {
            rowVal = computeValue(p.getAt(r));
          }
        }
        return r;
      }
    }

    // Use Case 11.
    if ( expressionDef.getOrder() == Order.DESC) {
      while (r < p.size() && !isDistanceGreater(sortKey, rowVal, amt) ) {
        r++;
        if ( r < p.size() ) {
          rowVal = computeValue(p.getAt(r));
        }
      }
      return r;
    }
    else { // Use Case 12.
      while (r < p.size() && !isDistanceGreater(rowVal, sortKey, amt) ) {
        r++;
        if ( r < p.size() ) {
          rowVal = computeValue(p.getAt(r));
        }
      }
      return r;
    }
  }

  /*
|  Use | Boundary2.type | Boundary2.amt | Sort Key | Order | Behavior                          |
| Case |                |               |          |       |                                   |
|------+----------------+---------------+----------+-------+-----------------------------------|
|   1. | PRECEDING      | UNB           | ANY      | ANY   | Error                             |
|   2. | PRECEDING      | unsigned int  | NULL     | DESC  | end = partition.size()            |
|   3. |                |               |          | ASC   | end = 0                           |
|   4. | PRECEDING      | unsigned int  | not null | DESC  | scan backward until row R2        |
|      |                |               |          |       | such that R2.sk - R.sk > bnd.amt  |
|      |                |               |          |       | end = R2.idx + 1                  |
|   5. | PRECEDING      | unsigned int  | not null | ASC   | scan backward until row R2        |
|      |                |               |          |       | such that R.sk -  R2.sk > bnd.amt |
|      |                |               |          |       | end = R2.idx + 1                  |
|   6. | CURRENT ROW    |               | NULL     | ANY   | scan forward until row R2         |
|      |                |               |          |       | such that R2.sk is not null       |
|      |                |               |          |       | end = R2.idx                      |
|   7. | CURRENT ROW    |               | not null | ANY   | scan forward until row R2         |
|      |                |               |          |       | such that R2.sk != R.sk           |
|      |                |               |          |       | end = R2.idx                      |
|   8. | FOLLOWING      | UNB           | ANY      | ANY   | end = partition.size()            |
|   9. | FOLLOWING      | unsigned int  | NULL     | DESC  | end = partition.size()            |
|  10. |                |               |          | ASC   | scan forward until row R2         |
|      |                |               |          |       | such that R2.sk is not null       |
|      |                |               |          |       | end = R2.idx                      |
|  11. | FOLLOWING      | unsigned int  | not NULL | DESC  | scan forward until row R2         |
|      |                |               |          |       | such R.sk - R2.sk > bnd.amt       |
|      |                |               |          |       | end = R2.idx                      |
|  12. |                |               |          | ASC   | scan forward until row R2         |
|      |                |               |          |       | such R2.sk - R2.sk > bnd.amt      |
|      |                |               |          |       | end = R2.idx                      |
|------+----------------+---------------+----------+-------+-----------------------------------|
   */
  @Override
  public int computeEnd(int rowIdx, PTFPartition p) throws HiveException {
    switch(end.getDirection()) {
    case PRECEDING:
      return computeEndPreceding(rowIdx, p);
    case CURRENT:
      return computeEndCurrentRow(rowIdx, p);
    case FOLLOWING:
      default:
        return computeEndFollowing(rowIdx, p);
    }
  }

  protected int computeEndPreceding(int rowIdx, PTFPartition p) throws HiveException {
    int amt = end.getAmt();
    // Use Case 1.
    // amt == UNBOUNDED, is caught during translation

    Object sortKey = computeValue(p.getAt(rowIdx));

    if ( sortKey == null ) {
      // Use Case 2.
      if ( expressionDef.getOrder() == Order.DESC ) {
        return p.size();
      }
      else { // Use Case 3.
        return 0;
      }
    }

    Object rowVal = sortKey;
    int r = rowIdx;

    // Use Case 4.
    if ( expressionDef.getOrder() == Order.DESC ) {
      while (r >= 0 && !isDistanceGreater(rowVal, sortKey, amt) ) {
        r--;
        if ( r >= 0 ) {
          rowVal = computeValue(p.getAt(r));
        }
      }
      return r + 1;
    }
    else { // Use Case 5.
      while (r >= 0 && !isDistanceGreater(sortKey, rowVal, amt) ) {
        r--;
        if ( r >= 0 ) {
          rowVal = computeValue(p.getAt(r));
        }
      }
      return r + 1;
    }
  }

  protected int computeEndCurrentRow(int rowIdx, PTFPartition p) throws HiveException {
    Object sortKey = computeValue(p.getAt(rowIdx));

    // Use Case 6.
    if ( sortKey == null ) {
      while ( sortKey == null && rowIdx < p.size() ) {
        ++rowIdx;
        if ( rowIdx < p.size() ) {
          sortKey = computeValue(p.getAt(rowIdx));
        }
      }
      return rowIdx;
    }

    Object rowVal = sortKey;
    int r = rowIdx;

    // Use Case 7.
    while (r < p.size() && isEqual(sortKey, rowVal) ) {
      r++;
      if ( r < p.size() ) {
        rowVal = computeValue(p.getAt(r));
      }
    }
    return r;
  }

  protected int computeEndFollowing(int rowIdx, PTFPartition p) throws HiveException {
    int amt = end.getAmt();

    // Use Case 8.
    if ( amt == BoundarySpec.UNBOUNDED_AMOUNT ) {
      return p.size();
    }
    Object sortKey = computeValue(p.getAt(rowIdx));

    Object rowVal = sortKey;
    int r = rowIdx;

    if ( sortKey == null ) {
      // Use Case 9.
      if ( expressionDef.getOrder() == Order.DESC) {
        return p.size();
      }
      else { // Use Case 10.
        while (r < p.size() && rowVal == null ) {
          r++;
          if ( r < p.size() ) {
            rowVal = computeValue(p.getAt(r));
          }
        }
        return r;
      }
    }

    // Use Case 11.
    if ( expressionDef.getOrder() == Order.DESC) {
      while (r < p.size() && !isDistanceGreater(sortKey, rowVal, amt) ) {
        r++;
        if ( r < p.size() ) {
          rowVal = computeValue(p.getAt(r));
        }
      }
      return r;
    }
    else { // Use Case 12.
      while (r < p.size() && !isDistanceGreater(rowVal, sortKey, amt) ) {
        r++;
        if ( r < p.size() ) {
          rowVal = computeValue(p.getAt(r));
        }
      }
      return r;
    }
  }

  public Object computeValue(Object row) throws HiveException {
    Object o = expressionDef.getExprEvaluator().evaluate(row);
    return ObjectInspectorUtils.copyToStandardObject(o, expressionDef.getOI());
  }

  /**
   * Checks if the distance of v2 to v1 is greater than the given amt.
   * @return True if the value of v1 - v2 is greater than amt or either value is null.
   */
  public abstract boolean isDistanceGreater(Object v1, Object v2, int amt);

  /**
   * Checks if the values of v1 or v2 are the same.
   * @return True if both values are the same or both are nulls.
   */
  public abstract boolean isEqual(Object v1, Object v2);


  @SuppressWarnings("incomplete-switch")
  public static SingleValueBoundaryScanner getScanner(BoundaryDef start, BoundaryDef end, OrderDef orderDef)
      throws HiveException {
    if (orderDef.getExpressions().size() != 1) {
      throw new HiveException("Internal error: initializing SingleValueBoundaryScanner with"
              + " multiple expression for sorting");
    }
    OrderExpressionDef exprDef = orderDef.getExpressions().get(0);
    PrimitiveObjectInspector pOI = (PrimitiveObjectInspector) exprDef.getOI();
    switch(pOI.getPrimitiveCategory()) {
    case BYTE:
    case INT:
    case LONG:
    case SHORT:
      return new LongValueBoundaryScanner(start, end, exprDef);
    case TIMESTAMP:
      return new TimestampValueBoundaryScanner(start, end, exprDef);
    case TIMESTAMPLOCALTZ:
      return new TimestampLocalTZValueBoundaryScanner(start, end, exprDef);
    case DOUBLE:
    case FLOAT:
      return new DoubleValueBoundaryScanner(start, end, exprDef);
    case DECIMAL:
      return new HiveDecimalValueBoundaryScanner(start, end, exprDef);
    case DATE:
      return new DateValueBoundaryScanner(start, end, exprDef);
    case STRING:
      return new StringValueBoundaryScanner(start, end, exprDef);
    }
    throw new HiveException(
        String.format("Internal Error: attempt to setup a Window for datatype %s",
            pOI.getPrimitiveCategory()));
  }
}

class LongValueBoundaryScanner extends SingleValueBoundaryScanner {
  public LongValueBoundaryScanner(BoundaryDef start, BoundaryDef end, OrderExpressionDef expressionDef) {
    super(start, end,expressionDef);
  }

  @Override
  public boolean isDistanceGreater(Object v1, Object v2, int amt) {
    if (v1 != null && v2 != null) {
      long l1 = PrimitiveObjectInspectorUtils.getLong(v1,
          (PrimitiveObjectInspector) expressionDef.getOI());
      long l2 = PrimitiveObjectInspectorUtils.getLong(v2,
          (PrimitiveObjectInspector) expressionDef.getOI());
      return (l1 -l2) > amt;
    }

    return v1 != null || v2 != null; // True if only one value is null
  }

  @Override
  public boolean isEqual(Object v1, Object v2) {
    if (v1 != null && v2 != null) {
      long l1 = PrimitiveObjectInspectorUtils.getLong(v1,
          (PrimitiveObjectInspector) expressionDef.getOI());
      long l2 = PrimitiveObjectInspectorUtils.getLong(v2,
          (PrimitiveObjectInspector) expressionDef.getOI());
      return l1 == l2;
    }

    return v1 == null && v2 == null; // True if both are null
  }
}

class DoubleValueBoundaryScanner extends SingleValueBoundaryScanner {
  public DoubleValueBoundaryScanner(BoundaryDef start, BoundaryDef end, OrderExpressionDef expressionDef) {
    super(start, end,expressionDef);
  }

  @Override
  public boolean isDistanceGreater(Object v1, Object v2, int amt) {
    if (v1 != null && v2 != null) {
      double d1 = PrimitiveObjectInspectorUtils.getDouble(v1,
          (PrimitiveObjectInspector) expressionDef.getOI());
      double d2 = PrimitiveObjectInspectorUtils.getDouble(v2,
          (PrimitiveObjectInspector) expressionDef.getOI());
      return (d1 -d2) > amt;
    }

    return v1 != null || v2 != null; // True if only one value is null
  }

  @Override
  public boolean isEqual(Object v1, Object v2) {
    if (v1 != null && v2 != null) {
      double d1 = PrimitiveObjectInspectorUtils.getDouble(v1,
          (PrimitiveObjectInspector) expressionDef.getOI());
      double d2 = PrimitiveObjectInspectorUtils.getDouble(v2,
          (PrimitiveObjectInspector) expressionDef.getOI());
      return d1 == d2;
    }

    return v1 == null && v2 == null; // True if both are null
  }
}

class HiveDecimalValueBoundaryScanner extends SingleValueBoundaryScanner {
  public HiveDecimalValueBoundaryScanner(BoundaryDef start, BoundaryDef end, OrderExpressionDef expressionDef) {
    super(start, end,expressionDef);
  }

  @Override
  public boolean isDistanceGreater(Object v1, Object v2, int amt) {
    HiveDecimal d1 = PrimitiveObjectInspectorUtils.getHiveDecimal(v1,
        (PrimitiveObjectInspector) expressionDef.getOI());
    HiveDecimal d2 = PrimitiveObjectInspectorUtils.getHiveDecimal(v2,
        (PrimitiveObjectInspector) expressionDef.getOI());
    if ( d1 != null && d2 != null ) {
      return d1.subtract(d2).intValue() > amt;  // TODO: lossy conversion!
    }

    return d1 != null || d2 != null; // True if only one value is null
  }

  @Override
  public boolean isEqual(Object v1, Object v2) {
    HiveDecimal d1 = PrimitiveObjectInspectorUtils.getHiveDecimal(v1,
        (PrimitiveObjectInspector) expressionDef.getOI());
    HiveDecimal d2 = PrimitiveObjectInspectorUtils.getHiveDecimal(v2,
        (PrimitiveObjectInspector) expressionDef.getOI());
    if ( d1 != null && d2 != null ) {
      return d1.equals(d2);
    }

    return d1 == null && d2 == null; // True if both are null
  }
}

class DateValueBoundaryScanner extends SingleValueBoundaryScanner {
  public DateValueBoundaryScanner(BoundaryDef start, BoundaryDef end, OrderExpressionDef expressionDef) {
    super(start, end,expressionDef);
  }

  @Override
  public boolean isDistanceGreater(Object v1, Object v2, int amt) {
    Date l1 = PrimitiveObjectInspectorUtils.getDate(v1,
        (PrimitiveObjectInspector) expressionDef.getOI());
    Date l2 = PrimitiveObjectInspectorUtils.getDate(v2,
        (PrimitiveObjectInspector) expressionDef.getOI());
    if (l1 != null && l2 != null) {
        return (double)(l1.toEpochMilli() - l2.toEpochMilli())/1000 > (long)amt * 24 * 3600; // Converts amt days to milliseconds
    }
    return l1 != l2; // True if only one date is null
  }

  @Override
  public boolean isEqual(Object v1, Object v2) {
    Date l1 = PrimitiveObjectInspectorUtils.getDate(v1,
        (PrimitiveObjectInspector) expressionDef.getOI());
    Date l2 = PrimitiveObjectInspectorUtils.getDate(v2,
        (PrimitiveObjectInspector) expressionDef.getOI());
    return (l1 == null && l2 == null) || (l1 != null && l1.equals(l2));
  }
}

class TimestampValueBoundaryScanner extends SingleValueBoundaryScanner {
  public TimestampValueBoundaryScanner(BoundaryDef start, BoundaryDef end, OrderExpressionDef expressionDef) {
    super(start, end,expressionDef);
  }

  @Override
  public boolean isDistanceGreater(Object v1, Object v2, int amt) {
    if (v1 != null && v2 != null) {
      long l1 = PrimitiveObjectInspectorUtils.getTimestamp(v1,
          (PrimitiveObjectInspector) expressionDef.getOI()).toEpochMilli();
      long l2 = PrimitiveObjectInspectorUtils.getTimestamp(v2,
          (PrimitiveObjectInspector) expressionDef.getOI()).toEpochMilli();
      return (double)(l1-l2)/1000 > amt; // TODO: lossy conversion, distance is considered in seconds
    }
    return v1 != null || v2 != null; // True if only one value is null
  }

  @Override
  public boolean isEqual(Object v1, Object v2) {
    if (v1 != null && v2 != null) {
      Timestamp l1 = PrimitiveObjectInspectorUtils.getTimestamp(v1,
          (PrimitiveObjectInspector) expressionDef.getOI());
      Timestamp l2 = PrimitiveObjectInspectorUtils.getTimestamp(v2,
          (PrimitiveObjectInspector) expressionDef.getOI());
      return l1.equals(l2);
    }
    return v1 == null && v2 == null; // True if both are null
  }
}

class TimestampLocalTZValueBoundaryScanner extends SingleValueBoundaryScanner {
  public TimestampLocalTZValueBoundaryScanner(BoundaryDef start, BoundaryDef end, OrderExpressionDef expressionDef) {
    super(start, end,expressionDef);
  }

  @Override
  public boolean isDistanceGreater(Object v1, Object v2, int amt) {
    if (v1 != null && v2 != null) {
      long l1 = PrimitiveObjectInspectorUtils.getTimestampLocalTZ(v1,
          (PrimitiveObjectInspector) expressionDef.getOI(), null).getEpochSecond();
      long l2 = PrimitiveObjectInspectorUtils.getTimestampLocalTZ(v2,
          (PrimitiveObjectInspector) expressionDef.getOI(), null).getEpochSecond();
      return (l1 -l2) > amt; // TODO: lossy conversion, distance is considered seconds similar to timestamp
    }
    return v1 != null || v2 != null; // True if only one value is null
  }

  @Override
  public boolean isEqual(Object v1, Object v2) {
    if (v1 != null && v2 != null) {
      TimestampTZ l1 = PrimitiveObjectInspectorUtils.getTimestampLocalTZ(v1,
          (PrimitiveObjectInspector) expressionDef.getOI(), null);
      TimestampTZ l2 = PrimitiveObjectInspectorUtils.getTimestampLocalTZ(v2,
          (PrimitiveObjectInspector) expressionDef.getOI(), null);
      return l1.equals(l2);
    }
    return v1 == null && v2 == null; // True if both are null
  }
}

class StringValueBoundaryScanner extends SingleValueBoundaryScanner {
  public StringValueBoundaryScanner(BoundaryDef start, BoundaryDef end, OrderExpressionDef expressionDef) {
    super(start, end,expressionDef);
  }

  @Override
  public boolean isDistanceGreater(Object v1, Object v2, int amt) {
    String s1 = PrimitiveObjectInspectorUtils.getString(v1,
        (PrimitiveObjectInspector) expressionDef.getOI());
    String s2 = PrimitiveObjectInspectorUtils.getString(v2,
        (PrimitiveObjectInspector) expressionDef.getOI());
    return s1 != null && s2 != null && s1.compareTo(s2) > 0;
  }

  @Override
  public boolean isEqual(Object v1, Object v2) {
    String s1 = PrimitiveObjectInspectorUtils.getString(v1,
        (PrimitiveObjectInspector) expressionDef.getOI());
    String s2 = PrimitiveObjectInspectorUtils.getString(v2,
        (PrimitiveObjectInspector) expressionDef.getOI());
    return (s1 == null && s2 == null) || (s1 != null && s1.equals(s2));
  }
}

/*
 */
 class MultiValueBoundaryScanner extends ValueBoundaryScanner {
  OrderDef orderDef;

  public MultiValueBoundaryScanner(BoundaryDef start, BoundaryDef end, OrderDef orderDef) {
    super(start, end);
    this.orderDef = orderDef;
  }

  /*
|------+----------------+----------------+----------+-------+-----------------------------------|
| Use  | Boundary1.type | Boundary1. amt | Sort Key | Order | Behavior                          |
| Case |                |                |          |       |                                   |
|------+----------------+----------------+----------+-------+-----------------------------------|
|   1. | PRECEDING      | UNB            | ANY      | ANY   | start = 0                         |
|   2. | CURRENT ROW    |                | ANY      | ANY   | scan backwards until row R2       |
|      |                |                |          |       | such R2.sk != R.sk                |
|      |                |                |          |       | start = R2.idx + 1                |
|------+----------------+----------------+----------+-------+-----------------------------------|
   */
  @Override
  public int computeStart(int rowIdx, PTFPartition p) throws HiveException {
    switch(start.getDirection()) {
    case PRECEDING:
      return computeStartPreceding(rowIdx, p);
    case CURRENT:
      return computeStartCurrentRow(rowIdx, p);
    case FOLLOWING:
      default:
        throw new HiveException(
                "FOLLOWING not allowed for starting RANGE with multiple expressions in ORDER BY");
    }
  }

  protected int computeStartPreceding(int rowIdx, PTFPartition p) throws HiveException {
    int amt = start.getAmt();
    if ( amt == BoundarySpec.UNBOUNDED_AMOUNT ) {
      return 0;
    }
    throw new HiveException(
            "PRECEDING needs UNBOUNDED for RANGE with multiple expressions in ORDER BY");
  }

  protected int computeStartCurrentRow(int rowIdx, PTFPartition p) throws HiveException {
    Object[] sortKey = computeValues(p.getAt(rowIdx));
    Object[] rowVal = sortKey;
    int r = rowIdx;

    while (r >= 0 && isEqual(rowVal, sortKey) ) {
      r--;
      if ( r >= 0 ) {
        rowVal = computeValues(p.getAt(r));
      }
    }
    return r + 1;
  }

  /*
|------+----------------+---------------+----------+-------+-----------------------------------|
| Use  | Boundary2.type | Boundary2.amt | Sort Key | Order | Behavior                          |
| Case |                |               |          |       |                                   |
|------+----------------+---------------+----------+-------+-----------------------------------|
|   1. | CURRENT ROW    |               | ANY      | ANY   | scan forward until row R2         |
|      |                |               |          |       | such that R2.sk != R.sk           |
|      |                |               |          |       | end = R2.idx                      |
|   2. | FOLLOWING      | UNB           | ANY      | ANY   | end = partition.size()            |
|------+----------------+---------------+----------+-------+-----------------------------------|
   */
  @Override
  public int computeEnd(int rowIdx, PTFPartition p) throws HiveException {
    switch(end.getDirection()) {
    case PRECEDING:
      throw new HiveException(
              "PRECEDING not allowed for finishing RANGE with multiple expressions in ORDER BY");
    case CURRENT:
      return computeEndCurrentRow(rowIdx, p);
    case FOLLOWING:
      default:
        return computeEndFollowing(rowIdx, p);
    }
  }

  protected int computeEndCurrentRow(int rowIdx, PTFPartition p) throws HiveException {
    Object[] sortKey = computeValues(p.getAt(rowIdx));
    Object[] rowVal = sortKey;
    int r = rowIdx;

    while (r < p.size() && isEqual(sortKey, rowVal) ) {
      r++;
      if ( r < p.size() ) {
        rowVal = computeValues(p.getAt(r));
      }
    }
    return r;
  }

  protected int computeEndFollowing(int rowIdx, PTFPartition p) throws HiveException {
    int amt = end.getAmt();
    if ( amt == BoundarySpec.UNBOUNDED_AMOUNT ) {
      return p.size();
    }
    throw new HiveException(
            "FOLLOWING needs UNBOUNDED for RANGE with multiple expressions in ORDER BY");
  }

  public Object[] computeValues(Object row) throws HiveException {
    Object[] objs = new Object[orderDef.getExpressions().size()];
    for (int i = 0; i < objs.length; i++) {
      Object o = orderDef.getExpressions().get(i).getExprEvaluator().evaluate(row);
      objs[i] = ObjectInspectorUtils.copyToStandardObject(o, orderDef.getExpressions().get(i).getOI());
    }
    return objs;
  }

  public boolean isEqual(Object[] v1, Object[] v2) {
    assert v1.length == v2.length;
    for (int i = 0; i < v1.length; i++) {
      if (v1[i] == null && v2[i] == null) {
        continue;
      }
      if (v1[i] == null || v2[i] == null) {
        return false;
      }
      if (ObjectInspectorUtils.compare(
              v1[i], orderDef.getExpressions().get(i).getOI(),
              v2[i], orderDef.getExpressions().get(i).getOI()) != 0) {
        return false;
      }
    }
    return true;
  }
}

