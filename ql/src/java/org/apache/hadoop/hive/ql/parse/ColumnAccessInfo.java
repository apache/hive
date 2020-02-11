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

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.SetMultimap;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class ColumnAccessInfo {
  /**
   * Map of table name to names of accessed columns (directly and indirectly -through views-).
   */
  private final SetMultimap<String, ColumnAccess> tableToColumnAccessMap;

  public ColumnAccessInfo() {
    // Must be deterministic order map for consistent q-test output across Java versions
    tableToColumnAccessMap = LinkedHashMultimap.create();
  }

  /**
   * Adds access to column.
   */
  public void add(String table, String col) {
    tableToColumnAccessMap.put(table, new ColumnAccess(col, Access.DIRECT));
  }

  /**
   * Adds indirect access to column (through view).
   */
  public void addIndirect(String table, String col) {
    tableToColumnAccessMap.put(table, new ColumnAccess(col, Access.INDIRECT));
  }

  /**
   * Includes direct access.
   */
  public Map<String, List<String>> getTableToColumnAccessMap() {
    // Must be deterministic order map for consistent q-test output across Java versions
    Map<String, List<String>> mapping = new LinkedHashMap<String, List<String>>();
    for (Map.Entry<String, Collection<ColumnAccess>> entry : tableToColumnAccessMap.asMap().entrySet()) {
      List<String> sortedCols = entry.getValue().stream()
        .filter(ca -> ca.access == Access.DIRECT)
        .map(ca -> ca.columnName)
        .sorted()
        .collect(Collectors.toList());
      if (!sortedCols.isEmpty()) {
        mapping.put(entry.getKey(), sortedCols);
      }
    }
    return mapping;
  }

  /**
   * Includes direct and indirect access.
   */
  public Map<String, List<String>> getTableToColumnAllAccessMap() {
    // Must be deterministic order map for consistent q-test output across Java versions
    Map<String, List<String>> mapping = new LinkedHashMap<String, List<String>>();
    for (Map.Entry<String, Collection<ColumnAccess>> entry : tableToColumnAccessMap.asMap().entrySet()) {
      mapping.put(
        entry.getKey(),
        entry.getValue().stream()
          .map(ca -> ca.columnName)
          .distinct()
          .sorted()
          .collect(Collectors.toList()));
    }
    return mapping;
  }

  /**
   * Strip a virtual column out of the set of columns.  This is useful in cases where we do not
   * want to be checking against the user reading virtual columns, namely update and delete.
   * @param vc
   */
  public void stripVirtualColumn(VirtualColumn vc) {
    for (Map.Entry<String, Collection<ColumnAccess>> e : tableToColumnAccessMap.asMap().entrySet()) {
      for (ColumnAccess columnAccess : e.getValue()) {
        if (vc.getName().equalsIgnoreCase(columnAccess.columnName)) {
          e.getValue().remove(columnAccess);
          break;
        }
      }
    }
  }

  /**
   * Column access information.
   */
  private static class ColumnAccess {
    private final String columnName;
    private final Access access;
    
    private ColumnAccess (String columnName, Access access) {
      this.columnName = Objects.requireNonNull(columnName);
      this.access = Objects.requireNonNull(access);
    }
    
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o instanceof ColumnAccess) {
        ColumnAccess other = (ColumnAccess) o;
        return columnName.equals(other.columnName)
            && access == other.access;
      }
      return false;
    }
    
    @Override
    public int hashCode() {
      return Objects.hash(columnName, access);      
    }
    
  }
  
  private enum Access {
    DIRECT,
    INDIRECT
  }
}
