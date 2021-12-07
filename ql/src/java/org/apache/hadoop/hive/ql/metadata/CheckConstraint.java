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

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;

/**
 * CheckConstraintInfo is a metadata structure containing the Check constraints
 * associated with a table.
 */
@SuppressWarnings("serial")
public class CheckConstraint implements Serializable {

  public static class CheckConstraintCol {
    private final String colName;
    private final String checkExpression;
    private final String enable;
    private final String validate;
    private final String rely;

    public CheckConstraintCol(String colName, String checkExpression, String enable,
                              String validate, String rely) {
      this.colName = colName;
      this.checkExpression = checkExpression;
      this.enable = enable;
      this.validate = validate;
      this.rely = rely;
    }

    public String getColName() {
      return colName;
    }

    public String getCheckExpression() {
      return checkExpression;
    }

    public String getEnable() {
      return enable;
    }

    public String getValidate() {
      return validate;
    }

    public String getRely() {
      return rely;
    }
  }

  // Mapping from constraint name to list of Check constraints
  Map<String, List<CheckConstraintCol>> checkConstraints;

  List<String> checkExpressionList;

  // Mapping from column name to Check expr
  String tableName;
  String databaseName;

  public CheckConstraint() {}

  public CheckConstraint(List<SQLCheckConstraint> checkConstraintsList) {
    checkConstraints = new TreeMap<>();
    checkExpressionList = new ArrayList<>();
    if (checkConstraintsList == null) {
      return;
    }
    if(!checkConstraintsList.isEmpty()) {
      this.tableName = checkConstraintsList.get(0).getTable_name();
      this.databaseName= checkConstraintsList.get(0).getTable_db();
    }
    for (SQLCheckConstraint constraint : checkConstraintsList) {
      String colName = constraint.getColumn_name();
      String check_expression = constraint.getCheck_expression();
      String enable = constraint.isEnable_cstr()? "ENABLE": "DISABLE";
      String validate = constraint.isValidate_cstr()? "VALIDATE": "NOVALIDATE";
      String rely = constraint.isRely_cstr()? "RELY": "NORELY";
      checkExpressionList.add(check_expression);
      CheckConstraintCol currCol = new CheckConstraintCol(
        colName, check_expression, enable, validate, rely);
      String constraintName = constraint.getDc_name();
      if (checkConstraints.containsKey(constraintName)) {
        checkConstraints.get(constraintName).add(currCol);
      } else {
        List<CheckConstraintCol> currList = new ArrayList<CheckConstraintCol>();
        currList.add(currCol);
        checkConstraints.put(constraintName, currList);
      }
    }
  }

  public String getTableName() {
    return tableName;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public List<String> getCheckExpressionList() { return checkExpressionList; }

  public Map<String, List<CheckConstraintCol>> getCheckConstraints() {
    return checkConstraints;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Check Constraints for " + databaseName + "." + tableName + ":");
    sb.append("[");
    if (checkConstraints != null && checkConstraints.size() > 0) {
      for (Map.Entry<String, List<CheckConstraintCol>> me : checkConstraints.entrySet()) {
        sb.append(" {Constraint Name: " + me.getKey() + ",");
        List<CheckConstraintCol> currCol = me.getValue();
        if (currCol != null && currCol.size() > 0) {
          for (CheckConstraintCol ukc : currCol) {
            sb.append (" (Column Name: " + ukc.colName + ", Check Expression : " + ukc.checkExpression+ "),");
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

  public static boolean isNotEmpty(CheckConstraint info) {
    return info != null && !info.getCheckConstraints().isEmpty();
  }
}
