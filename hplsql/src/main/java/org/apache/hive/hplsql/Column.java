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

package org.apache.hive.hplsql;

/**
 * Table column
 */
public class Column {
  
  String name;
  String type;
  Var value;
  
  Column(String name, String type) {
    this.name = name;
    this.type = type;
  }
  
  /**
   * Set the column value
   */
  void setValue(Var value) {
    this.value = value;
  }

  /**
   * Get the column name
   */
  String getName() {
    return name;
  }
  
  /**
   * Get the column type
   */
  String getType() {
    return type;
  }
    
  /**
   * Get the column value
   */
  Var getValue() {
    return value;
  }
}



