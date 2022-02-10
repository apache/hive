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

package org.apache.hadoop.hive.ql.parse;

/**
 * Class to fetch hash map containing query tables.
 * This object is used to hold all the tables used in the query. This object
 * is meant to be mutable until the parsing is complete. Once that happens,
 * 'setImmutable' should be called so that any caller using this object
 * will be given an immutable set.  If a caller tries to fetch the table map
 * while this object is being built, an exception will be thrown.
 **/

import org.apache.hadoop.hive.ql.metadata.Table;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;

public class QueryTables implements ParsedQueryTables {
  private final Map<String, Table> tableObjects = new HashMap<>();

  private Map<String, Table> parsedTableObjects;

  public QueryTables(boolean isEmptyMap) {
    if (isEmptyMap) {
      markParsingCompleted();
    }
  }

  public QueryTables() {
    this(false);
  }

  public boolean containsKey(String name) {
    return tableObjects.containsKey(name);
  }

  public void put(String name, Table table) {
    tableObjects.put(name, table);
  }

  public void markParsingCompleted() {
    parsedTableObjects = ImmutableMap.copyOf(tableObjects);
  }

  public Table get(String name) {
    return tableObjects.get(name);
  }

  public Table getParsedTable(String name) {
    if (parsedTableObjects == null) {
      throw new RuntimeException("Cannot call getParsedTable() until parsing is marked " +
          "completed.");
    }
    return parsedTableObjects.get(name);
  }
}
