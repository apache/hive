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

import org.apache.hadoop.hive.ql.metadata.VirtualColumn;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ColumnAccessInfo {
  /**
   * Map of table name to names of accessed columns
   */
  private final Map<String, Set<String>> tableToColumnAccessMap;

  public ColumnAccessInfo() {
    // Must be deterministic order map for consistent q-test output across Java versions
    tableToColumnAccessMap = new LinkedHashMap<String, Set<String>>();
  }

  public void add(String table, String col) {
    Set<String> tableColumns = tableToColumnAccessMap.get(table);
    if (tableColumns == null) {
      // Must be deterministic order set for consistent q-test output across Java versions
      tableColumns = new LinkedHashSet<String>();
      tableToColumnAccessMap.put(table, tableColumns);
    }
    tableColumns.add(col);
  }

  public Map<String, List<String>> getTableToColumnAccessMap() {
    // Must be deterministic order map for consistent q-test output across Java versions
    Map<String, List<String>> mapping = new LinkedHashMap<String, List<String>>();
    for (Map.Entry<String, Set<String>> entry : tableToColumnAccessMap.entrySet()) {
      List<String> sortedCols = new ArrayList<String>(entry.getValue());
      Collections.sort(sortedCols);
      mapping.put(entry.getKey(), sortedCols);
    }
    return mapping;
  }

  /**
   * Strip a virtual column out of the set of columns.  This is useful in cases where we do not
   * want to be checking against the user reading virtual columns, namely update and delete.
   * @param vc
   */
  public void stripVirtualColumn(VirtualColumn vc) {
    for (Map.Entry<String, Set<String>> e : tableToColumnAccessMap.entrySet()) {
      for (String columnName : e.getValue()) {
        if (vc.getName().equalsIgnoreCase(columnName)) {
          e.getValue().remove(columnName);
          break;
        }
      }
    }

  }
}
