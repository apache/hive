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
package org.apache.hadoop.hive.ql.exec.vector.wrapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.util.NullOrdering;

/**
 * An implementation of {@link Comparator} to compare {@link VectorHashKeyWrapperBase} instances.
 */
public class VectorHashKeyWrapperGeneralComparator
        implements Comparator<VectorHashKeyWrapperBase>, Serializable {

  /**
   * Compare {@link VectorHashKeyWrapperBase} instances only by one column.
   */
  private static class VectorHashKeyWrapperBaseComparator
          implements Comparator<VectorHashKeyWrapperBase>, Serializable {

    private final int keyIndex;
    private final Comparator<VectorHashKeyWrapperBase> comparator;
    private final int nullResult;

    VectorHashKeyWrapperBaseComparator(int keyIndex, Comparator<VectorHashKeyWrapperBase> comparator, char nullOrder) {
      this.keyIndex = keyIndex;
      this.comparator = comparator;
      switch (NullOrdering.fromSign(nullOrder)) {
      case NULLS_FIRST:
        this.nullResult = 1;
        break;
      default:
        this.nullResult = -1;
      }
    }

    @Override
    public int compare(VectorHashKeyWrapperBase o1, VectorHashKeyWrapperBase o2) {
      boolean isNull1 = o1.isNull(keyIndex);
      boolean isNull2 = o2.isNull(keyIndex);

      if (isNull1 && isNull2) {
        return 0;
      }
      if (isNull1) {
        return -nullResult;
      }
      if (isNull2) {
        return nullResult;
      }
      return comparator.compare(o1, o2);
    }
  }

  private final List<VectorHashKeyWrapperBaseComparator> comparators;

  public VectorHashKeyWrapperGeneralComparator(int numberOfColumns) {
    this.comparators = new ArrayList<>(numberOfColumns);
  }

  public void addColumnComparator(int keyIndex, int columnTypeSpecificIndex, ColumnVector.Type columnVectorType,
                                  char sortOrder, char nullOrder) {
    Comparator<VectorHashKeyWrapperBase> comparator;
    switch (columnVectorType) {
    case LONG:
    case DECIMAL_64:
      comparator = (o1, o2) ->
              Long.compare(o1.getLongValue(columnTypeSpecificIndex), o2.getLongValue(columnTypeSpecificIndex));
      break;
    case DOUBLE:
      comparator = (o1, o2) -> Double.compare(
              o1.getDoubleValue(columnTypeSpecificIndex), o2.getDoubleValue(columnTypeSpecificIndex));
      break;
    case BYTES:
      comparator = (o1, o2) -> StringExpr.compare(
              o1.getBytes(columnTypeSpecificIndex),
              o1.getByteStart(columnTypeSpecificIndex),
              o1.getByteLength(columnTypeSpecificIndex),
              o2.getBytes(columnTypeSpecificIndex),
              o2.getByteStart(columnTypeSpecificIndex),
              o2.getByteLength(columnTypeSpecificIndex));
      break;
    case DECIMAL:
      comparator = (o1, o2) ->
              o1.getDecimal(columnTypeSpecificIndex).compareTo(o2.getDecimal(columnTypeSpecificIndex));
      break;
    case TIMESTAMP:
      comparator = (o1, o2) ->
              o1.getTimestamp(columnTypeSpecificIndex).compareTo(o2.getTimestamp(columnTypeSpecificIndex));
      break;
    case INTERVAL_DAY_TIME:
      comparator = (o1, o2) -> o1.getIntervalDayTime(columnTypeSpecificIndex)
              .compareTo(o2.getIntervalDayTime(columnTypeSpecificIndex));
      break;
    default:
      throw new RuntimeException("Unexpected column vector columnVectorType " + columnVectorType);
    }

    comparators.add(
            new VectorHashKeyWrapperBaseComparator(
                    keyIndex,
                    sortOrder == '-' ? comparator.reversed() : comparator,
                    nullOrder));
  }

  @Override
  public int compare(VectorHashKeyWrapperBase o1, VectorHashKeyWrapperBase o2) {
    for (Comparator<VectorHashKeyWrapperBase> comparator : comparators) {
      int c = comparator.compare(o1, o2);
      if (c != 0) {
        return c;
      }
    }
    return 0;
  }
}
