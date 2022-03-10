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

import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;

/**
 * DefaultConstraintInfo is a metadata structure containing the default constraints
 * associated with a table.
 */
@SuppressWarnings("serial")
public class DefaultConstraint implements Serializable {

  public class DefaultConstraintCol {
    public String colName;
    public String defaultVal;
    public String enable;
    public String validate;
    public String rely;

    public DefaultConstraintCol(String colName, String defaultVal, String enable, String validate, String rely) {
      this.colName = colName;
      this.defaultVal = defaultVal;
      this.enable = enable;
      this.validate = validate;
      this.rely = rely;
    }
  }

  // Mapping from constraint name to list of default constraints
  Map<String, List<DefaultConstraintCol>> defaultConstraints;

  // Mapping from column name to default value
  Map<String, String> colNameToDefaultValueMap;
  String tableName;
  String databaseName;

  public DefaultConstraint() {}

  public DefaultConstraint(List<SQLDefaultConstraint> defaultConstraintList, String tableName, String databaseName) {
    this.tableName = tableName;
    this.databaseName = databaseName;
    defaultConstraints = new TreeMap<>();
    colNameToDefaultValueMap = new TreeMap<>();
    if (defaultConstraintList == null) {
      return;
    }
    for (SQLDefaultConstraint uk : defaultConstraintList) {
      if (uk.getTable_db().equalsIgnoreCase(databaseName) &&
          uk.getTable_name().equalsIgnoreCase(tableName)) {
        String colName = uk.getColumn_name();
        String defVal = uk.getDefault_value();
        colNameToDefaultValueMap.put(colName, defVal);
        String enable = uk.isEnable_cstr()? "ENABLE": "DISABLE";
        String validate = uk.isValidate_cstr()? "VALIDATE": "NOVALIDATE";
        String rely = uk.isRely_cstr()? "RELY": "NORELY";
        DefaultConstraintCol currCol = new DefaultConstraintCol(
                colName, defVal, enable, validate, rely);
        String constraintName = uk.getDc_name();
        if (defaultConstraints.containsKey(constraintName)) {
          defaultConstraints.get(constraintName).add(currCol);
        } else {
          List<DefaultConstraintCol> currList = new ArrayList<DefaultConstraintCol>();
          currList.add(currCol);
          defaultConstraints.put(constraintName, currList);
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

  public Map<String, List<DefaultConstraintCol>> getDefaultConstraints() {
    return defaultConstraints;
  }
  public Map<String, String> getColNameToDefaultValueMap() {
    return colNameToDefaultValueMap;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Default Constraints for " + databaseName + "." + tableName + ":");
    sb.append("[");
    if (defaultConstraints != null && defaultConstraints.size() > 0) {
      for (Map.Entry<String, List<DefaultConstraintCol>> me : defaultConstraints.entrySet()) {
        sb.append(" {Constraint Name: " + me.getKey() + ",");
        List<DefaultConstraintCol> currCol = me.getValue();
        if (currCol != null && currCol.size() > 0) {
          for (DefaultConstraintCol ukc : currCol) {
            sb.append (" (Column Name: " + ukc.colName + ", Default Value: " + ukc.defaultVal + "),");
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

  public static boolean isNotEmpty(DefaultConstraint info) {
    return info != null && !info.getDefaultConstraints().isEmpty();
  }
}
