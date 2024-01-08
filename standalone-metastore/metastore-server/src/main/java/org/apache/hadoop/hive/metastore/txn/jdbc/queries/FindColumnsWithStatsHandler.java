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
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.jdbc.QueryHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class FindColumnsWithStatsHandler implements QueryHandler<List<String>> {

  private static final Logger LOG = LoggerFactory.getLogger(FindColumnsWithStatsHandler.class);

  //language=SQL
  private static final String TABLE_SELECT = "SELECT \"COLUMN_NAME\" FROM \"TAB_COL_STATS\" " +
      "WHERE \"DB_NAME\" = :dbName AND \"TABLE_NAME\" = :tableName";
  //language=SQL
  private static final String PARTITION_SELECT = "SELECT \"COLUMN_NAME\" FROM \"PART_COL_STATS\" " +
      "WHERE \"DB_NAME\" = :dbName AND \"TABLE_NAME\" = :tableName AND \"PARTITION_NAME\" = :partName";
  
  private final CompactionInfo ci;

  public FindColumnsWithStatsHandler(CompactionInfo ci) {
    this.ci = ci;
  }

  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) {
    return ci.partName != null ? PARTITION_SELECT : TABLE_SELECT;
  }

  @Override
  public SqlParameterSource getQueryParameters() {
    MapSqlParameterSource params = new MapSqlParameterSource()
        .addValue("dbName", ci.dbname)
        .addValue("tableName", ci.tableName);
    if (ci.partName != null) {
      params.addValue("partName", ci.partName);
    }
    return params;
  }

  @Override
  public List<String> extractData(ResultSet rs) throws SQLException, DataAccessException {
    List<String> columns = new ArrayList<>();
    while (rs.next()) {
      columns.add(rs.getString(1));
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Found columns to update stats: {} on {}{}", columns, ci.tableName,
          (ci.partName == null ? "" : "/" + ci.partName));
    }
    return columns;
  }
}
