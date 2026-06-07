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

package org.apache.hadoop.hive.ql.anon.builders;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.ColumnInternalFormat;

public class ExplainAttachPolicyStatementBuilder {

  public enum ResolutionMode { EXPLICIT, STRICTEST }

  private String tableName;
  private String columnName;
  private final List<String> policyNames = new ArrayList<>();
  private String schemaField;
  private String rowLocator;
  private ColumnInternalFormat columnFormat;
  private ResolutionMode resolutionMode;

  public ExplainAttachPolicyStatementBuilder withTableName(final String tableName) {
    this.tableName = tableName;
    return this;
  }

  public ExplainAttachPolicyStatementBuilder withColumnName(final String columnName) {
    this.columnName = columnName;
    return this;
  }

  public ExplainAttachPolicyStatementBuilder withPolicies(final String... names) {
    for (String n : names) {
      policyNames.add(n);
    }
    return this;
  }

  public ExplainAttachPolicyStatementBuilder withBindingOpts(final String schemaField,
      final String rowLocator, final ColumnInternalFormat columnFormat,
      final ResolutionMode resolutionMode) {
    this.schemaField = schemaField;
    this.rowLocator = rowLocator;
    this.columnFormat = columnFormat;
    this.resolutionMode = resolutionMode;
    return this;
  }

  public String build() {
    final StringBuilder sb = new StringBuilder("EXPLAIN ATTACH DATA ERASURE POLICY ");
    sb.append(String.join(", ", policyNames));
    sb.append(" ON TABLE ").append(tableName).append(" COLUMN ").append(columnName);
    if (resolutionMode != null) {
      sb.append(" WITH (SCHEMA FIELD (").append(schemaField).append("), ");
      sb.append("ROW LOCATOR (").append(rowLocator).append("), ");
      sb.append("COLUMN FORMAT (").append(columnFormat.name()).append("))");
      sb.append(" RESOLUTION (").append(resolutionMode.name()).append(")");
    }
    return sb.toString();
  }
}
