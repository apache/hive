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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.security.UserGroupInformation;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.join;

/**
 * Generate the audit log in a builder manner.
 */
public class MetaStoreAuditLog {

  static final Logger auditLog = LoggerFactory.getLogger(
      HiveMetaStore.class.getName() + ".audit");

  private static void logAuditEvent(String cmd) {
    if (cmd == null) {
      return;
    }

    UserGroupInformation ugi;
    try {
      ugi = SecurityUtils.getUGI();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }

    String address = HMSHandler.getIPAddress();
    if (address == null) {
      address = "unknown-ip-addr";
    }

    auditLog.info("ugi={}	ip={}	cmd={}	", ugi.getUserName(), address, cmd);
  }

  static void logAndAudit(MetaStoreAuditLog logBuilder) {
    logAndAudit(logBuilder.build());
  }

  static void logAndAudit(String message) {
    HMSHandler.LOG.debug("{}: {}", HMSHandler.get(), message);
    logAuditEvent(message);
  }

  // the method name
  private final String methodName;
  private final StringBuilder builder;

  private MetaStoreAuditLog(String methodName) {
    this.methodName = methodName;
    this.builder = new StringBuilder();
  }

  public static MetaStoreAuditLog method(String methodName) {
    requireNonNull(methodName, "methodName is null");
    MetaStoreAuditLog builder = new MetaStoreAuditLog(methodName);
    return builder;
  }

  public MetaStoreAuditLog connectorName(String connectorName) {
    builder.append("connector=").append(connectorName).append(" ");
    return this;
  }

  public MetaStoreAuditLog catalogName(String catalogName) {
    builder.append("catName=").append(catalogName).append(" ");
    return this;
  }

  public MetaStoreAuditLog dbName(String dbName) {
    builder.append("db=").append(dbName).append(" ");
    return this;
  }

  public MetaStoreAuditLog tableName(String tableName) {
    builder.append("tbl=").append(tableName).append(" ");
    return this;
  }

  public MetaStoreAuditLog packageName(String packageName) {
    builder.append("package=").append(packageName).append(" ");
    return this;
  }

  public MetaStoreAuditLog typeName(String typeName) {
    builder.append("type=").append(typeName).append(" ");
    return this;
  }

  public MetaStoreAuditLog pattern(String pattern) {
    builder.append("pat=").append(pattern).append(" ");
    return this;
  }

  public MetaStoreAuditLog part_name(String part_name) {
    builder.append("part=").append(part_name).append(" ");
    return this;
  }

  public MetaStoreAuditLog column(String column) {
    builder.append("column=").append(column).append(" ");
    return this;
  }

  public MetaStoreAuditLog serde(String serde) {
    builder.append("serde=").append(serde).append(" ");
    return this;
  }

  public MetaStoreAuditLog request(Object value) {
    builder.append("request=").append(value).append(" ");
    return this;
  }

  public MetaStoreAuditLog ischema(Object value) {
    builder.append("ischema=").append(value).append(" ");
    return this;
  }

  public MetaStoreAuditLog extraInfo(String key, Object value) {
    builder.append(key).append("=").append(value).append(" ");
    return this;
  }

  public MetaStoreAuditLog tableNames(List<String> tableNames) {
    builder.append("tbls=").append(join(tableNames, ",")).append(" ");
    return this;
  }

  public MetaStoreAuditLog partVals(List<String> partVals) {
    builder.append("partVals=").append(join(partVals, ",")).append(" ");
    return this;
  }

  public MetaStoreAuditLog partNames(Map<String, String> partNames) {
    builder.append("partition=").append(partNames).append(" ");
    return this;
  }

  public MetaStoreAuditLog constraintName(String constraintName) {
    builder.append("constraint=").append(constraintName).append(" ");
    return this;
  }

  public MetaStoreAuditLog table(String catName, String dbName, String tblName) {
    builder.append("tbl=").append(TableName.getQualified(catName, dbName, tblName)).append(" ");
    return this;
  }

  public MetaStoreAuditLog token(String token_str_form) {
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
