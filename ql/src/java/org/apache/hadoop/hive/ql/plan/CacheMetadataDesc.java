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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.plan;


@SuppressWarnings("serial")
public class CacheMetadataDesc extends DDLDesc {
  private final String dbName, tableName, partName;
  private final boolean isAllParts;

  public CacheMetadataDesc(String dbName, String tableName, String partName) {
    this(dbName, tableName, partName, false);
  }

  public CacheMetadataDesc(String dbName, String tableName, boolean isAllParts) {
    this(dbName, tableName, null, isAllParts);
  }

  private CacheMetadataDesc(String dbName, String tableName, String partName, boolean isAllParts) {
    super();
    this.dbName = dbName;
    this.tableName = tableName;
    this.partName = partName;
    this.isAllParts = isAllParts;
  }

  public boolean isAllParts() {
    return isAllParts;
  }

  public String getPartName() {
    return partName;
  }

  public String getDbName() {
    return dbName;
  }

  public String getTableName() {
    return tableName;
  }
}
