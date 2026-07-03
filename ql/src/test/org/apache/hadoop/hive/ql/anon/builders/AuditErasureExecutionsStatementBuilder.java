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

public class AuditErasureExecutionsStatementBuilder {

  private final String tableName;
  private TimeRange timeRange;
  private String byUser;
  private Integer forIdentity;

  public AuditErasureExecutionsStatementBuilder(final String tableName) {
    this.tableName = tableName;
  }

  public AuditErasureExecutionsStatementBuilder withTimeRange(final TimeRange timeRange) {
    this.timeRange = timeRange;
    return this;
  }

  public AuditErasureExecutionsStatementBuilder withByUser(final String byUser) {
    this.byUser = byUser;
    return this;
  }

  public AuditErasureExecutionsStatementBuilder withForIdentity(final int identity) {
    this.forIdentity = identity;
    return this;
  }

  public String build() {
    final StringBuilder sb = new StringBuilder("AUDIT ERASURE EXECUTIONS ON TABLE ")
        .append(tableName);
    if (timeRange != null) {
      sb.append(timeRange.render());
    }
    if (byUser != null) {
      sb.append(" BY USER ").append(byUser);
    }
    if (forIdentity != null) {
      sb.append(" FOR IDENTITY ").append(forIdentity);
    }
    return sb.toString();
  }
}
