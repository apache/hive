/**
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

import java.util.Map;

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

  /**
   * Getter for the replace flag being insert into/overwrite
   * @return Replace flag to represent INSERT INTO or INSERT OVERWRITE (Boolean).
   */
  public abstract boolean isReplace();

  /**
   * Get the map of partition keyvalues.  Will be null if this insert is to a table and not a
   * partition.
   * @return Map of partition keyvalues, or null.
   */
  public abstract Map<String,String> getPartitionKeyValues();

  /**
   * Get list of file name and checksum created as a result of this DML operation
   *
   * @return The iterable of files
   */
  public abstract Iterable<String> getFiles();

  @Override
  public EventMessage checkValid() {
    if (getTable() == null)
      throw new IllegalStateException("Table name unset.");
    return super.checkValid();
  }
}
