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
package org.apache.hadoop.hive.ql.util;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class HiveStrictManagedMigrationControlConfig {

  private Map<String, List<String>> databaseIncludeLists = new TreeMap<String, List<String>>();

  public Map<String, List<String>> getDatabaseIncludeLists() {
    return databaseIncludeLists;
  }

  public void setDatabaseIncludeLists(Map<String, List<String>> databaseIncludeLists) {
    this.databaseIncludeLists = databaseIncludeLists;
  }

  public void putAllFromConfig(HiveStrictManagedMigrationControlConfig other) {
    for (String db : other.getDatabaseIncludeLists().keySet()) {
      List<String> theseTables = this.databaseIncludeLists.get(db);
      List<String> otherTables = other.getDatabaseIncludeLists().get(db);
      if (theseTables == null) {
        this.databaseIncludeLists.put(db, otherTables);
      } else {
        if (otherTables != null) {
          theseTables.addAll(otherTables);
        }
      }
    }
  }
}