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
package org.apache.hadoop.hive.metastore;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.TableName;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.join;

/**
 * Generate the audit log in a builder manner.
 */
public class MetaStoreAuditLogBuilder {
  // the function
  private final String functionName;
  private final StringBuilder builder;

  private MetaStoreAuditLogBuilder(String functionName) {
    this.functionName = functionName;
    this.builder = new StringBuilder();
  }

  public static MetaStoreAuditLogBuilder functionName(String functionName) {
    requireNonNull(functionName, "functionName is null");
    MetaStoreAuditLogBuilder builder = new MetaStoreAuditLogBuilder(functionName);
    return builder;
  }

  public MetaStoreAuditLogBuilder connectorName(String connectorName) {
    builder.append("connector=").append(connectorName).append(" ");
    return this;
  }

  public MetaStoreAuditLogBuilder catalogName(String catalogName) {
    builder.append("catName=").append(catalogName).append(" ");
    return this;
  }

  public MetaStoreAuditLogBuilder dbName(String dbName) {
    builder.append("db=").append(dbName).append(" ");
    return this;
  }

  public MetaStoreAuditLogBuilder tableName(String tableName) {
    builder.append("tbl=").append(tableName).append(" ");
    return this;
  }

  public MetaStoreAuditLogBuilder packageName(String packageName) {
    builder.append("package=").append(packageName).append(" ");
    return this;
  }

  public MetaStoreAuditLogBuilder typeName(String typeName) {
    builder.append("type=").append(typeName).append(" ");
    return this;
  }

  public MetaStoreAuditLogBuilder pattern(String pattern) {
    builder.append("pat=").append(pattern).append(" ");
    return this;
  }

  public MetaStoreAuditLogBuilder part_name(String part_name) {
    builder.append("part=").append(part_name).append(" ");
    return this;
  }

  public MetaStoreAuditLogBuilder column(String column) {
    builder.append("column=").append(column).append(" ");
    return this;
  }

  public MetaStoreAuditLogBuilder serde(String serde) {
    builder.append("serde=").append(serde).append(" ");
    return this;
  }

  public MetaStoreAuditLogBuilder request(Object value) {
    builder.append("request=").append(value).append(" ");
    return this;
  }

  public MetaStoreAuditLogBuilder ischema(Object value) {
    builder.append("ischema=").append(value).append(" ");
    return this;
  }

  public MetaStoreAuditLogBuilder extraInfo(String key, Object value) {
    builder.append(key).append("=").append(value).append(" ");
    return this;
  }

  public MetaStoreAuditLogBuilder tableNames(List<String> tableNames) {
    builder.append("tbls=").append(join(tableNames, ",")).append(" ");
    return this;
  }

  public MetaStoreAuditLogBuilder partVals(List<String> partVals) {
    builder.append("partVals=").append(join(partVals, ",")).append(" ");
    return this;
  }

  public MetaStoreAuditLogBuilder partNames(Map<String, String> partNames) {
    builder.append("partition=").append(partNames).append(" ");
    return this;
  }

  public MetaStoreAuditLogBuilder constraintName(String constraintName) {
    builder.append("constraint=").append(constraintName).append(" ");
    return this;
  }

  public MetaStoreAuditLogBuilder table(String catName, String dbName, String tblName) {
    builder.append("tbl=").append(TableName.getQualified(catName, dbName, tblName)).append(" ");
    return this;
  }

  public MetaStoreAuditLogBuilder token(String token_str_form) {
    builder.append(" token=").append(token_str_form).append(" ");
    return this;
  }

  public String build() {
    StringBuilder result = new StringBuilder(functionName);
    if (builder.length() > 0) {
      builder.setLength(builder.length() - 1);
      result.append(" : ").append(builder);
    }
    return result.toString();
  }

}
