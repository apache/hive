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
package org.apache.hadoop.hive.ql.stats.jdbc;

public final class JDBCStatsSetupConstants {

  public static final String PART_STAT_ID_COLUMN_NAME = "ID";

  public static final String PART_STAT_TIMESTAMP_COLUMN_NAME = "TS";

  // NOTE:
  // For all table names past and future, Hive will not drop old versions of this table, it is up
  // to the administrator
  public static final String PART_STAT_TABLE_NAME = "PARTITION_STATS_V2";

  // supported statistics - column names

  public static final String PART_STAT_ROW_COUNT_COLUMN_NAME = "ROW_COUNT";

  public static final String PART_STAT_RAW_DATA_SIZE_COLUMN_NAME = "RAW_DATA_SIZE";

}
