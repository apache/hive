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
package org.apache.hadoop.hive.metastore.txn.jdbc.commands;

import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionMetricsData;
import org.apache.hadoop.hive.metastore.txn.jdbc.ParameterizedCommand;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.Types;
import java.util.function.Function;

public class RemoveCompactionMetricsDataCommand implements ParameterizedCommand {

  //language=SQL
  private static final String DELETE_COMPACTION_METRICS_CACHE =
      "DELETE FROM \"COMPACTION_METRICS_CACHE\" WHERE \"CMC_DATABASE\" = :db AND \"CMC_TABLE\" = :table " +
          "AND \"CMC_METRIC_TYPE\" = :type AND (:partition IS NULL OR \"CMC_PARTITION\" = :partition)";

  private final String dbName;
  private final String tblName;
  private final String partitionName;
  private final CompactionMetricsData.MetricType type;

  public RemoveCompactionMetricsDataCommand(String dbName, String tblName, String partitionName, CompactionMetricsData.MetricType type) {
    this.dbName = dbName;
    this.tblName = tblName;
    this.partitionName = partitionName;
    this.type = type;
  }

  @Override
  public Function<Integer, Boolean> resultPolicy() {
    return null;
  }

  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) throws MetaException {
    return DELETE_COMPACTION_METRICS_CACHE;
  }

  @Override
  public SqlParameterSource getQueryParameters() {
    return new MapSqlParameterSource()
        .addValue("db", dbName)
        .addValue("table", tblName)
        .addValue("type", type.toString())
        .addValue("partition", partitionName, Types.VARCHAR);
  }
}
