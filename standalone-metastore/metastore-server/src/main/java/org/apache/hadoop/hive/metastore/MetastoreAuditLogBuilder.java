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
public class MetastoreAuditLogBuilder {
  // the method name
  private final String methodName;
  private final StringBuilder builder;

  private MetastoreAuditLogBuilder(String methodName) {
    this.methodName = methodName;
    this.builder = new StringBuilder();
  }

  public static MetastoreAuditLogBuilder method(String methodName) {
    requireNonNull(methodName, "methodName is null");
    MetastoreAuditLogBuilder builder = new MetastoreAuditLogBuilder(methodName);
    return builder;
  }

  public MetastoreAuditLogBuilder connectorName(String connectorName) {
    builder.append("connector=").append(connectorName).append(" ");
    return this;
  }

  public MetastoreAuditLogBuilder catalogName(String catalogName) {
    builder.append("catName=").append(catalogName).append(" ");
    return this;
  }

  public MetastoreAuditLogBuilder dbName(String dbName) {
    builder.append("db=").append(dbName).append(" ");
    return this;
  }

  public MetastoreAuditLogBuilder tableName(String tableName) {
    builder.append("tbl=").append(tableName).append(" ");
    return this;
  }

  public MetastoreAuditLogBuilder packageName(String packageName) {
    builder.append("package=").append(packageName).append(" ");
    return this;
  }

  public MetastoreAuditLogBuilder typeName(String typeName) {
    builder.append("type=").append(typeName).append(" ");
    return this;
  }

  public MetastoreAuditLogBuilder pattern(String pattern) {
    builder.append("pat=").append(pattern).append(" ");
    return this;
  }

  public MetastoreAuditLogBuilder part_name(String part_name) {
    builder.append("part=").append(part_name).append(" ");
    return this;
  }

  public MetastoreAuditLogBuilder column(String column) {
    builder.append("column=").append(column).append(" ");
    return this;
  }

  public MetastoreAuditLogBuilder serde(String serde) {
    builder.append("serde=").append(serde).append(" ");
    return this;
  }

  public MetastoreAuditLogBuilder request(Object value) {
    builder.append("request=").append(value).append(" ");
    return this;
  }

  public MetastoreAuditLogBuilder ischema(Object value) {
    builder.append("ischema=").append(value).append(" ");
    return this;
  }

  public MetastoreAuditLogBuilder extraInfo(String key, Object value) {
    builder.append(key).append("=").append(value).append(" ");
    return this;
  }

  public MetastoreAuditLogBuilder tableNames(List<String> tableNames) {
    builder.append("tbls=").append(join(tableNames, ",")).append(" ");
    return this;
  }

  public MetastoreAuditLogBuilder partVals(List<String> partVals) {
    builder.append("partVals=").append(join(partVals, ",")).append(" ");
    return this;
  }

  public MetastoreAuditLogBuilder partNames(Map<String, String> partNames) {
    builder.append("partition=").append(partNames).append(" ");
    return this;
  }

  public MetastoreAuditLogBuilder constraintName(String constraintName) {
    builder.append("constraint=").append(constraintName).append(" ");
    return this;
  }

  public MetastoreAuditLogBuilder table(String catName, String dbName, String tblName) {
    builder.append("tbl=").append(TableName.getQualified(catName, dbName, tblName)).append(" ");
    return this;
  }

  public MetastoreAuditLogBuilder token(String token_str_form) {
    builder.append("token=").append(token_str_form).append(" ");
    return this;
  }

  public String build() {
    StringBuilder result = new StringBuilder(methodName);
    if (builder.length() > 0) {
      builder.setLength(builder.length() - 1);
      result.append(" : ").append(builder);
    }
    return result.toString();
  }

}
