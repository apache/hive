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
package org.apache.hadoop.hive.metastore.utils;

import java.util.regex.Pattern;

/**
 * Validates the ORDER BY clause carried on a compaction request.
 *
 * The clause originates from the client-settable Thrift field {@code CompactionRequest.orderByClause},
 * is persisted verbatim in the compaction queue and is later concatenated into a query that the
 * compaction worker executes with the compaction session's privileges. It must therefore be treated
 * as untrusted input everywhere it is consumed: only a plain {@code ORDER BY} over column names
 * (optionally qualified, quoted, with ASC/DESC and NULLS FIRST/LAST modifiers) is accepted -
 * exactly the shape HiveServer2 produces for
 * {@code ALTER TABLE ... COMPACT ... ORDER BY col [ASC|DESC] [NULLS FIRST|LAST], ...}.
 * Anything else (subqueries, UNION tails, LIMIT, expressions, comments) is rejected.
 */
public final class CompactionOrderByValidator {

  private static final String IDENTIFIER = "(?:[A-Za-z0-9_]+|`(?:[^`]|``)+`)";
  private static final String COLUMN = IDENTIFIER + "(?:\\s*\\.\\s*" + IDENTIFIER + ")*";
  private static final String ITEM =
      COLUMN + "(?:\\s+(?i:ASC|DESC))?(?:\\s+(?i:NULLS)\\s+(?i:FIRST|LAST))?";
  private static final Pattern ORDER_BY_PATTERN = Pattern.compile(
      "\\s*(?i:ORDER)\\s+(?i:BY)\\s+" + ITEM + "(?:\\s*,\\s*" + ITEM + ")*\\s*");

  private CompactionOrderByValidator() {
    throw new UnsupportedOperationException("CompactionOrderByValidator should not be instantiated");
  }

  /**
   * Validates a compaction request ORDER BY clause.
   *
   * @param orderByClause the clause as stored on the request, e.g. "order by `col` DESC nulls last, col2";
   *                      null or blank is accepted (no reordering requested)
   * @throws IllegalArgumentException if the clause is anything but a plain ORDER BY over column names
   */
  public static void validate(String orderByClause) {
    if (orderByClause == null || orderByClause.trim().isEmpty()) {
      return;
    }
    if (!ORDER_BY_PATTERN.matcher(orderByClause).matches()) {
      throw new IllegalArgumentException("orderByClause must be a plain ORDER BY over column names " +
          "(optionally with ASC/DESC and NULLS FIRST/LAST), got: " + orderByClause);
    }
  }
}
