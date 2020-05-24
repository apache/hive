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

package org.apache.hadoop.hive.ql.ddl.misc.metadata;

import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for ANALYZE TABLE ... CACHE METADATA commands.
 */
@Explain(displayName = "Cache Metadata", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class CacheMetadataDesc implements DDLDesc {
  private final String dbName;
  private final String tableName;
  private final String partitionName;
  private final boolean isAllPartitions;

  public CacheMetadataDesc(String dbName, String tableName, String partitionName) {
    this(dbName, tableName, partitionName, false);
  }

  public CacheMetadataDesc(String dbName, String tableName, boolean isAllPartitions) {
    this(dbName, tableName, null, isAllPartitions);
  }

  private CacheMetadataDesc(String dbName, String tableName, String partitionName, boolean isAllPartitions) {
    this.dbName = dbName;
    this.tableName = tableName;
    this.partitionName = partitionName;
    this.isAllPartitions = isAllPartitions;
  }

  @Explain(displayName = "db name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getDbName() {
    return dbName;
  }

  @Explain(displayName = "table name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTableName() {
    return tableName;
  }

  @Explain(displayName = "partition name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getPartitionName() {
    return partitionName;
  }

  @Explain(displayName = "all partitions", displayOnlyOnTrue =true,
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public boolean isAllPartitions() {
    return isAllPartitions;
  }
}
