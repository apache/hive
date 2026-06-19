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
package org.apache.hadoop.hive.metastore.txn.jdbc.queries;

import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionMetricsData;
import org.apache.hadoop.hive.metastore.txn.jdbc.QueryHandler;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

public class CompactionMetricsDataHandler implements QueryHandler<CompactionMetricsData> {

  //language=SQL
  private static final String SELECT_COMPACTION_METRICS_CACHE =
      "SELECT \"CMC_METRIC_VALUE\", \"CMC_VERSION\" FROM \"COMPACTION_METRICS_CACHE\" " +
      "WHERE \"CMC_DATABASE\" = :db AND \"CMC_TABLE\" = :table AND \"CMC_METRIC_TYPE\" = :type " +
          "AND (:partition IS NULL OR \"CMC_PARTITION\" = :partition)";

  private final String dbName;
  private final String tblName;
  private final String partitionName;
  private final CompactionMetricsData.MetricType type;

  public CompactionMetricsDataHandler(String dbName, String tblName, String partitionName, CompactionMetricsData.MetricType type) {
    this.dbName = dbName;
    this.tblName = tblName;
    this.partitionName = partitionName;
    this.type = type;
  }

  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) throws MetaException {
    return SELECT_COMPACTION_METRICS_CACHE;
  }

  @Override
  public SqlParameterSource getQueryParameters() {
    return new MapSqlParameterSource()
        .addValue("db", dbName)
        .addValue("table", tblName)
        .addValue("type", type.toString())
        .addValue("partition", partitionName, Types.VARCHAR);
  }

  @Override
  public CompactionMetricsData extractData(ResultSet rs) throws SQLException, DataAccessException {
    CompactionMetricsData.Builder builder = new CompactionMetricsData.Builder();
    if (rs.next()) {
      return builder
          .dbName(dbName)
          .tblName(tblName)
          .partitionName(partitionName)
          .metricType(type)
          .metricValue(rs.getInt(1))
          .version(rs.getInt(2)).build();
    } else {
      return null;
    }
  }
}
