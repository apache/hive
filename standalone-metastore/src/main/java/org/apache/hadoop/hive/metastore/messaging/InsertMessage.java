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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.metastore.messaging;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * HCat message sent when an insert is done to a table or partition.
 */
public abstract class InsertMessage extends EventMessage {

  protected InsertMessage() {
    super(EventType.INSERT);
  }

  /**
   * Getter for the name of the table being insert into.
   * @return Table-name (String).
   */
  public abstract String getTable();

  public abstract String getTableType();

  /**
   * Getter for the replace flag being insert into/overwrite
   * @return Replace flag to represent INSERT INTO or INSERT OVERWRITE (Boolean).
   */
  public abstract boolean isReplace();

  /**
   * Get list of file name and checksum created as a result of this DML operation
   *
   * @return The iterable of files
   */
  public abstract Iterable<String> getFiles();

  /**
   * Get the table object associated with the insert
   *
   * @return The Json format of Table object
   */
  public abstract Table getTableObj() throws Exception;

  /**
   * Get the partition object associated with the insert
   *
   * @return The Json format of Partition object if the table is partitioned else return null.
   */
  public abstract Partition getPtnObj() throws Exception;

  @Override
  public EventMessage checkValid() {
    if (getTable() == null)
      throw new IllegalStateException("Table name unset.");
    return super.checkValid();
  }
}
