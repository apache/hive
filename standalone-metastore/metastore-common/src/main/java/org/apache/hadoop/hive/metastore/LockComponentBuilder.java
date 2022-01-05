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

import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockType;

/**
 * A builder for {@link LockComponent}s
 */
public class LockComponentBuilder {
  private LockComponent component;
  private boolean tableNameSet;
  private boolean partNameSet;

  public LockComponentBuilder() {
    component = new LockComponent();
    tableNameSet = partNameSet = false;
  }

  /**
   * Set the lock to be exclusive.
   * @return reference to this builder
   */
  public LockComponentBuilder setExclusive() {
    component.setType(LockType.EXCLUSIVE);
    return this;
  }

  /**
   * Set the lock to be excl_write.
   * @return reference to this builder
   */
  public LockComponentBuilder setExclWrite() {
    component.setType(LockType.EXCL_WRITE);
    return this;
  }

  /**
   * Set the lock to be shared_write.
   * @return reference to this builder
   */
  public LockComponentBuilder setSharedWrite() {
    component.setType(LockType.SHARED_WRITE);
    return this;
  }

  /**
   * Set the lock to be shared_read.
   * @return reference to this builder
   */
  public LockComponentBuilder setSharedRead() {
    component.setType(LockType.SHARED_READ);
    return this;
  }

  /**
   * Set the database name.
   * @param dbName database name
   * @return reference to this builder
   */
  public LockComponentBuilder setDbName(String dbName) {
    component.setDbname(dbName);
    return this;
  }
  
  public LockComponentBuilder setOperationType(DataOperationType dop) {
    component.setOperationType(dop);
    return this;
  }

  public LockComponentBuilder setIsTransactional(boolean t) {
    component.setIsTransactional(t);
    return this;
  }
  /**
   * Set the table name.
   * @param tableName table name
   * @return reference to this builder
   */
  public LockComponentBuilder setTableName(String tableName) {
    component.setTablename(tableName);
    tableNameSet = true;
    return this;
  }

  /**
   * Set the partition name.
   * @param partitionName partition name
   * @return reference to this builder
   */
  public LockComponentBuilder setPartitionName(String partitionName) {
    component.setPartitionname(partitionName);
    partNameSet = true;
    return this;
  }
  public LockComponentBuilder setIsDynamicPartitionWrite(boolean t) {
    component.setIsDynamicPartitionWrite(t);
    return this;
  }

 /**
   * Get the constructed lock component.
   * @return lock component.
   */
  public LockComponent build() {
    LockLevel level = LockLevel.DB;
    if (tableNameSet) level = LockLevel.TABLE;
    if (partNameSet) level = LockLevel.PARTITION;
    component.setLevel(level);
    return component;
  }

  public LockComponentBuilder setLock(LockType type) {
    component.setType(type);
    return this;
  }
}
