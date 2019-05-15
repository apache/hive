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

package org.apache.hadoop.hive.ql.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;

/**
 * UniqueConstraintInfo is a metadata structure containing the unique constraints
 * associated with a table.
 */
@SuppressWarnings("serial")
public class UniqueConstraint implements Serializable {

  public class UniqueConstraintCol {
    public String colName;
    public Integer position;

    public UniqueConstraintCol(String colName, Integer position) {
      this.colName = colName;
      this.position = position;
    }
  }

  // Mapping from constraint name to list of unique constraints
  Map<String, List<UniqueConstraintCol>> uniqueConstraints;
  String tableName;
  String databaseName;

  public UniqueConstraint() {}

  public UniqueConstraint(List<SQLUniqueConstraint> uks, String tableName, String databaseName) {
    this.tableName = tableName;
    this.databaseName = databaseName;
    uniqueConstraints = new TreeMap<String, List<UniqueConstraintCol>>();
    if (uks == null) {
      return;
    }
    for (SQLUniqueConstraint uk : uks) {
      if (uk.getTable_db().equalsIgnoreCase(databaseName) &&
          uk.getTable_name().equalsIgnoreCase(tableName)) {
        UniqueConstraintCol currCol = new UniqueConstraintCol(
                uk.getColumn_name(), uk.getKey_seq());
        String constraintName = uk.getUk_name();
        if (uniqueConstraints.containsKey(constraintName)) {
          uniqueConstraints.get(constraintName).add(currCol);
        } else {
          List<UniqueConstraintCol> currList = new ArrayList<UniqueConstraintCol>();
          currList.add(currCol);
          uniqueConstraints.put(constraintName, currList);
        }
      }
    }
  }

  public String getTableName() {
    return tableName;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public Map<String, List<UniqueConstraintCol>> getUniqueConstraints() {
    return uniqueConstraints;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Unique Constraints for " + databaseName + "." + tableName + ":");
    sb.append("[");
    if (uniqueConstraints != null && uniqueConstraints.size() > 0) {
      for (Map.Entry<String, List<UniqueConstraintCol>> me : uniqueConstraints.entrySet()) {
        sb.append(" {Constraint Name: " + me.getKey() + ",");
        List<UniqueConstraintCol> currCol = me.getValue();
        if (currCol != null && currCol.size() > 0) {
          for (UniqueConstraintCol ukc : currCol) {
            sb.append (" (Column Name: " + ukc.colName + ", Key Sequence: " + ukc.position+ "),");
          }
          sb.setLength(sb.length()-1);
        }
        sb.append("},");
      }
      sb.setLength(sb.length()-1);
    }
    sb.append("]");
    return sb.toString();
  }
}
