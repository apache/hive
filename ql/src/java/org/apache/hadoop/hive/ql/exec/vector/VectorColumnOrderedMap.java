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

package org.apache.hadoop.hive.ql.exec.vector;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class collects column information for mapping vector columns, including the hive type name.
 *
 * The column information are kept ordered by a specified column.
 *
 * Call getMapping to collects the results into convenient arrays.
 */
public class VectorColumnOrderedMap {
  protected static transient final Log LOG = LogFactory.getLog(VectorColumnOrderedMap.class);

  protected String name;

  private TreeMap<Integer, Value> orderedTreeMap;

  private class Value {
    int valueColumn;

    String typeName;

    Value(int valueColumn, String typeName) {
      this.valueColumn = valueColumn;
      this.typeName = typeName;
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("(value column: " + valueColumn);
      sb.append(", type name: " + typeName + ")");
      return sb.toString();
    }
  }

  public class Mapping {

    private final int[] orderedColumns;
    private final int[] valueColumns;
    private final String[] typeNames;

    Mapping(int[] orderedColumns, int[] valueColumns, String[] typeNames) {
      this.orderedColumns = orderedColumns;
      this.valueColumns = valueColumns;
      this.typeNames = typeNames;
    }

    public int getCount() {
      return orderedColumns.length;
    }

    public int[] getOrderedColumns() {
      return orderedColumns;
    }

    public int[] getValueColumns() {
      return valueColumns;
    }

    public String[] getTypeNames() {
      return typeNames;
    }
  }

  public VectorColumnOrderedMap(String name) {
    this.name = name;
    orderedTreeMap = new TreeMap<Integer, Value>();
  }

  public void add(int orderedColumn, int valueColumn, String typeName) {
    if (orderedTreeMap.containsKey(orderedColumn)) {
      throw new RuntimeException(
          name + " duplicate column " + orderedColumn +
          " in ordered column map " + orderedTreeMap.toString() +
          " when adding value column " + valueColumn + ", type " + typeName);
    }
    orderedTreeMap.put(orderedColumn, new Value(valueColumn, typeName));
  }

  public boolean orderedColumnsContain(int orderedColumn) {
    return orderedTreeMap.containsKey(orderedColumn);
  }

  public Mapping getMapping() {
    ArrayList<Integer> orderedColumns = new ArrayList<Integer>();
    ArrayList<Integer> valueColumns = new ArrayList<Integer>();
    ArrayList<String> typeNames = new ArrayList<String>();
    for (Map.Entry<Integer, Value> entry : orderedTreeMap.entrySet()) {
      orderedColumns.add(entry.getKey());
      Value value = entry.getValue();
      valueColumns.add(value.valueColumn);
      typeNames.add(value.typeName);
    }
    return new Mapping(
            ArrayUtils.toPrimitive(orderedColumns.toArray(new Integer[0])),
            ArrayUtils.toPrimitive(valueColumns.toArray(new Integer[0])),
            typeNames.toArray(new String[0]));
    
  }
}
