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
package org.apache.hadoop.hive.metastore.txn.jdbc.functions;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionMetricsData;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class TopCompactionMetricsDataPerTypeFunction implements TransactionalFunction<List<CompactionMetricsData>> {

  private static final String NO_SELECT_COMPACTION_METRICS_CACHE_FOR_TYPE_QUERY =
      "\"CMC_DATABASE\", \"CMC_TABLE\", \"CMC_PARTITION\", \"CMC_METRIC_VALUE\", \"CMC_VERSION\" FROM " +
          "\"COMPACTION_METRICS_CACHE\" WHERE \"CMC_METRIC_TYPE\" = :type ORDER BY \"CMC_METRIC_VALUE\" DESC";
  
  private final int limit;

  public TopCompactionMetricsDataPerTypeFunction(int limit) {
    this.limit = limit;
  }

  @Override
  public List<CompactionMetricsData> execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    //TODO: Highly inefficient, should be replaced by a single select
    List<CompactionMetricsData> metricsDataList = new ArrayList<>();
    for (CompactionMetricsData.MetricType type : CompactionMetricsData.MetricType.values()) {
      metricsDataList.addAll(jdbcResource.getJdbcTemplate().query(
          jdbcResource.getSqlGenerator().addLimitClause(limit, NO_SELECT_COMPACTION_METRICS_CACHE_FOR_TYPE_QUERY),
          new MapSqlParameterSource().addValue("type", type.toString()),
          new CompactionMetricsDataMapper(type)));
    }
    return metricsDataList;
  }

  private static class CompactionMetricsDataMapper implements RowMapper<CompactionMetricsData> {

    private final CompactionMetricsData.MetricType type;    
    private final CompactionMetricsData.Builder builder = new CompactionMetricsData.Builder();

    public CompactionMetricsDataMapper(CompactionMetricsData.MetricType type) {
      this.type = type;
    }

    @Override
    public CompactionMetricsData mapRow(ResultSet rs, int rowNum) throws SQLException {
      return builder
          .dbName(rs.getString(1))
          .tblName(rs.getString(2))
          .partitionName(rs.getString(3))
          .metricType(type)
          .metricValue(rs.getInt(4))
          .version(rs.getInt(5))
          .build();
    }
  }

}
