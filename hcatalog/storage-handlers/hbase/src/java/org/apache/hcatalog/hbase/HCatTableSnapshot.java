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
package org.apache.hcatalog.hbase;

import java.io.Serializable;
import java.util.Map;


/**
 * The class HCatTableSnapshot represents a snapshot of a hcatalog table.
 * This class is intended to be opaque. This class would used only by the
 * record readers to obtain knowledge about the revisions of a
 * column to be filtered.
 */
public class HCatTableSnapshot implements Serializable {

  private static final long serialVersionUID = 1L;
  private String tableName;
  private String databaseName;
  private Map<String, Long> columnMap;
  private long latestRevision;

  HCatTableSnapshot(String databaseName, String tableName, Map<String, Long> columnMap, long latestRevision) {
    this.tableName = tableName;
    this.databaseName = databaseName;
    this.columnMap = columnMap;
    this.latestRevision = latestRevision;
  }

  /**
   * @return The name of the table in the snapshot.
   */
  public String getTableName() {
    return this.tableName;
  }

  /**
   * @return The name of the database to which the table snapshot belongs.
   */
  public String getDatabaseName() {
    return this.databaseName;
  }

  /**
   * @return The revision number of a column in a snapshot.
   */
  long getRevision(String column) {
    if (columnMap.containsKey(column))
      return this.columnMap.get(column);
    return latestRevision;
  }

  /**
   * The method checks if the snapshot contains information about a data column.
   *
   * @param column The data column of the table
   * @return true, if successful
   */
  boolean containsColumn(String column) {
    return this.columnMap.containsKey(column);
  }

  /**
   * @return latest committed revision when snapshot was taken
   */
  long getLatestRevision() {
    return latestRevision;
  }

  @Override
  public String toString() {
    String snapshot = " Database Name: " + this.databaseName + " Table Name : " + tableName +
      "Latest Revision: " + latestRevision + " Column revision : " + columnMap.toString();
    return snapshot;
  }
}
