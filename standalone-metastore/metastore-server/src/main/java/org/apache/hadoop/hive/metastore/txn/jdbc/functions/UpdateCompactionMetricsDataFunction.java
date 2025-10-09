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
import org.apache.hadoop.hive.metastore.txn.jdbc.commands.RemoveCompactionMetricsDataCommand;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.CompactionMetricsDataHandler;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.sql.Types;

public class UpdateCompactionMetricsDataFunction implements TransactionalFunction<Boolean> {

  private final CompactionMetricsData data;

  public UpdateCompactionMetricsDataFunction(CompactionMetricsData data) {
    this.data = data;
  }

  @Override
  public Boolean execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    CompactionMetricsData prevMetricsData = jdbcResource.execute(
        new CompactionMetricsDataHandler(data.getDbName(), data.getTblName(), data.getPartitionName(), data.getMetricType()));

    boolean updateRes;
    if (data.getMetricValue() >= data.getThreshold()) {
      if (prevMetricsData != null) {
        updateRes = updateCompactionMetricsData(data, prevMetricsData, jdbcResource.getJdbcTemplate());
      } else {
        updateRes = createCompactionMetricsData(data, jdbcResource.getJdbcTemplate());
      }
    } else {
      if (prevMetricsData != null) {
        int result = jdbcResource.execute(new RemoveCompactionMetricsDataCommand(
            data.getDbName(), data.getTblName(), data.getPartitionName(), data.getMetricType()));
        updateRes = result > 0;
      } else {
        return true;
      }
    }
    return updateRes;
  }

  private boolean updateCompactionMetricsData(CompactionMetricsData data, CompactionMetricsData prevData, NamedParameterJdbcTemplate jdbcTemplate) {
    return jdbcTemplate.update(
        "UPDATE \"COMPACTION_METRICS_CACHE\" SET \"CMC_METRIC_VALUE\" = :value, \"CMC_VERSION\" = :newVersion " +
            "WHERE \"CMC_DATABASE\" = :db AND \"CMC_TABLE\" = :table AND \"CMC_METRIC_TYPE\" = :type " +
            "AND \"CMC_VERSION\" = :oldVersion AND (:partition IS NULL OR \"CMC_PARTITION\" = :partition)",
        new MapSqlParameterSource()
            .addValue("value", data.getMetricValue())
            .addValue("oldVersion", prevData.getVersion())
            .addValue("newVersion", prevData.getVersion() + 1)
            .addValue("db", data.getDbName())
            .addValue("table", data.getTblName())
            .addValue("type", data.getMetricType().toString())
            .addValue("partition", data.getPartitionName(), Types.VARCHAR)) > 0;
  }

  private boolean createCompactionMetricsData(CompactionMetricsData data, NamedParameterJdbcTemplate jdbcTemplate) {
    return jdbcTemplate.update(
        "INSERT INTO \"COMPACTION_METRICS_CACHE\" ( " +
            "\"CMC_DATABASE\", \"CMC_TABLE\", \"CMC_PARTITION\", \"CMC_METRIC_TYPE\", \"CMC_METRIC_VALUE\", " +
            "\"CMC_VERSION\" ) VALUES (:db, :table, :partition, :type, :value, 1)",
        new MapSqlParameterSource()
            .addValue("db", data.getDbName())
            .addValue("table", data.getTblName())
            .addValue("partition", data.getPartitionName(), Types.VARCHAR)
            .addValue("type", data.getMetricType().toString())
            .addValue("value", data.getMetricValue())) > 0;
  }
}