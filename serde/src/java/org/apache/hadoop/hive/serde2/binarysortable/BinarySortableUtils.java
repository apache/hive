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
package org.apache.hadoop.hive.serde2.binarysortable;

import static org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe.ONE;
import static org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe.ZERO;

import java.util.Objects;
import java.util.Properties;

import org.apache.hadoop.hive.serde.serdeConstants;

/**
 * Utility class for BinarySortable classes.
 */
public final class BinarySortableUtils {
  public static void fillOrderArrays(
          Properties inputProperties,
          boolean[] columnSortOrderIsDesc, byte[] columnNullMarker, byte[] columnNotNullMarker) {
    Objects.requireNonNull(inputProperties, "inputProperties can not be null");
    Objects.requireNonNull(columnSortOrderIsDesc, "columnSortOrderIsDesc can not be null");
    Objects.requireNonNull(columnNullMarker, "columnNullMarker can not be null");
    Objects.requireNonNull(columnNotNullMarker, "columnNotNullMarker can not be null");

    if (columnSortOrderIsDesc.length != columnNullMarker.length ||
            columnSortOrderIsDesc.length != columnNotNullMarker.length) {
      throw new IllegalArgumentException(
              "columnSortOrderIsDesc, columnNullMarker and columnNotNullMarker arrays should have same length.");
    }

    // Get the sort order
    String columnSortOrder = inputProperties.getProperty(serdeConstants.SERIALIZATION_SORT_ORDER);
    if (columnSortOrder != null) {
      if (columnSortOrder.length() < columnSortOrderIsDesc.length) {
        throw new ArrayIndexOutOfBoundsException(
                String.format("From %d columns only %d has sort order specified.",
                        columnSortOrderIsDesc.length, columnSortOrder.length()));
      }
      for (int i = 0; i < columnSortOrderIsDesc.length; i++) {
        columnSortOrderIsDesc[i] = columnSortOrder.charAt(i) == '-';
      }
    }

    // Null first/last
    String columnNullOrder = inputProperties.getProperty(serdeConstants.SERIALIZATION_NULL_SORT_ORDER);
    if (columnNullOrder != null && columnNullOrder.length() < columnNullMarker.length) {
      throw new ArrayIndexOutOfBoundsException(
              String.format("From %d columns only %d has null sort order specified.",
                      columnNullMarker.length, columnNullOrder.length()));
    }
    for (int i = 0; i < columnSortOrderIsDesc.length; i++) {
      if (columnSortOrderIsDesc[i]) {
        // Descending
        if (columnNullOrder != null && columnNullOrder.charAt(i) == 'a') {
          // Null first
          columnNullMarker[i] = ONE;
          columnNotNullMarker[i] = ZERO;
        } else {
          // Null last
          columnNullMarker[i] = ZERO;
          columnNotNullMarker[i] = ONE;
        }
      } else {
        // Ascending
        if (columnNullOrder != null && columnNullOrder.charAt(i) == 'z') {
          // Null last
          columnNullMarker[i] = ONE;
          columnNotNullMarker[i] = ZERO;
        } else {
          // Null first
          columnNullMarker[i] = ZERO;
          columnNotNullMarker[i] = ONE;
        }
      }
    }
  }

  private BinarySortableUtils() {
  }
}
