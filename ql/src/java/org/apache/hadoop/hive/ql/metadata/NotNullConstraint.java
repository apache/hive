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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;

/**
 * NotNullConstraintInfo is a metadata structure containing the not null constraints
 * associated with a table.
 */
@SuppressWarnings("serial")
public class NotNullConstraint implements Serializable {

  // Mapping from constraint name to list of not null columns
  Map<String, String> notNullConstraints;
  String databaseName;
  String tableName;
  Map<String, List<String>> enableValidateRely;

  public NotNullConstraint() {}

  public NotNullConstraint(List<SQLNotNullConstraint> nns, String tableName, String databaseName) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.notNullConstraints = new TreeMap<>();
    enableValidateRely = new HashMap<>();
    if (nns ==null) {
      return;
    }
    for (SQLNotNullConstraint pk : nns) {
      if (pk.getTable_db().equalsIgnoreCase(databaseName) &&
          pk.getTable_name().equalsIgnoreCase(tableName)) {
        String enable = pk.isEnable_cstr()? "ENABLE": "DISABLE";
        String validate = pk.isValidate_cstr()? "VALIDATE": "NOVALIDATE";
        String rely = pk.isRely_cstr()? "RELY": "NORELY";
        enableValidateRely.put(pk.getNn_name(), ImmutableList.of(enable, validate, rely));
        notNullConstraints.put(pk.getNn_name(), pk.getColumn_name());
      }
    }
  }

  public String getTableName() {
    return tableName;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public Map<String, String> getNotNullConstraints() {
    return notNullConstraints;
  }

  public Map<String, List<String>> getEnableValidateRely() {
    return enableValidateRely;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Not Null Constraints for " + databaseName + "." + tableName + ":");
    sb.append("[");
    if (notNullConstraints != null && notNullConstraints.size() > 0) {
      for (Map.Entry<String, String> me : notNullConstraints.entrySet()) {
        sb.append(" {Constraint Name: " + me.getKey());
        sb.append(", Column Name: " + me.getValue());
        sb.append("},");
      }
      sb.setLength(sb.length()-1);
    }
    sb.append("]");
    return sb.toString();
  }

  public static boolean isNotEmpty(NotNullConstraint info) {
    return info != null && !info.getNotNullConstraints().isEmpty();
  }
}
