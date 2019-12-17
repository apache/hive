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

package org.apache.hadoop.hive.ql.plan;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockType;

import java.util.ArrayList;

/**
 * ExplainLockDesc represents lock entity in query plan.
 */
public class ExplainLockDesc {
  private String catalogName;
  private String dbName;
  private String tableName;
  private String partitionName;
  private LockType lockType;

  public ExplainLockDesc() {
  }

  public ExplainLockDesc(LockComponent component) {
    this.dbName = component.getDbname();
    if (null != component.getTablename()) {
      this.tableName = component.getTablename();
    }
    if (null != component.getPartitionname()) {
      this.partitionName = component.getPartitionname();
    }
    this.lockType = component.getType();
  }

  public String getCatalogName() {
    return catalogName;
  }

  public ExplainLockDesc setCatalogName(String catalogName) {
    this.catalogName = catalogName;
    return this;
  }

  public String getDbName() {
    return dbName;
  }

  public ExplainLockDesc setDbName(String dbName) {
    this.dbName = dbName;
    return this;
  }

  public String getTableName() {
    return tableName;
  }

  public ExplainLockDesc setTableName(String tableName) {
    this.tableName = tableName;
    return this;
  }

  public String getPartitionName() {
    return partitionName;
  }

  public ExplainLockDesc setPartitionName(String partitionName) {
    this.partitionName = partitionName;
    return this;
  }

  public LockType getLockType() {
    return lockType;
  }

  public ExplainLockDesc setLockType(LockType lockType) {
    this.lockType = lockType;
    return this;
  }

  public String getFullName() {
    ArrayList<String> list = new ArrayList<String>();
    if (null != catalogName) {
      list.add(catalogName);
    }
    if (null != dbName) {
      list.add(dbName);
    }
    if (null != tableName) {
      list.add(tableName);
    }
    if (null != partitionName) {
      list.add(partitionName);
    }
    return StringUtils.join(list, '.');
  }

  @Override public String toString() {
    return getFullName() + " -> " + this.getLockType();
  }
}
