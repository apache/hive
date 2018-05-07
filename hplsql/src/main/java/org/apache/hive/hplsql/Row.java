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

package org.apache.hive.hplsql;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Table row (all columns)
 */
public class Row {
    
  ArrayList<Column> columns = new ArrayList<Column>();
  HashMap<String, Column> columnMap = new HashMap<String, Column>();
  
  /**
   * Constructors
   */
  Row() {}
  
  Row(Row row) {
    for (Column c : row.columns) {
      addColumn(c.name, c.type); 
    }    
  }
  
  /**
   * Add a column with specified data type
   */
  void addColumn(String name, String type) {
    Column column = new Column(name, type);
    columns.add(column);
    columnMap.put(name.toUpperCase(), column);
  }
  
  /**
   * Get the data type by column name
   */
  String getType(String name) {
    Column column = columnMap.get(name.toUpperCase());
    if (column != null) {
      return column.getType();
    }
    return null;
  }
  
  /**
   * Get value by index
   */
  Var getValue(int i) {
    return columns.get(i).getValue();
  }
  
  /**
   * Get value by column name
   */
  Var getValue(String name) {
    Column column = columnMap.get(name.toUpperCase());
    if (column != null) {
      return column.getValue();
    }
    return null;
  }
  
  /**
   * Get columns
   */
  ArrayList<Column> getColumns() {
    return columns;
  }
  
  /**
   * Get column by index
   */
  Column getColumn(int i) {
    return columns.get(i);
  }
  
  /**
   * Get the number of columns
   */
  int size() {
    return columns.size();
  }
}



