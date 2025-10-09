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
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

public class UpdataDatabasePropFunction implements TransactionalFunction<Void> {

  private static final Logger LOG = LoggerFactory.getLogger(UpdataDatabasePropFunction.class);

  private final String database;
  private final long dbId;
  private final String prop;
  private final String propValue;

  public UpdataDatabasePropFunction(String database, long dbId, String prop, String propValue) {
    this.database = database;
    this.dbId = dbId;
    this.prop = prop;
    this.propValue = propValue;
  }

  @Override
  public Void execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    MapSqlParameterSource params = new MapSqlParameterSource()
        .addValue("dbId", dbId)
        .addValue("key", prop)
        .addValue("value", propValue);
    NamedParameterJdbcTemplate jdbcTemplate = jdbcResource.getJdbcTemplate();

    String value = jdbcTemplate.query("SELECT \"PARAM_VALUE\" FROM \"DATABASE_PARAMS\" " +
        "WHERE \"PARAM_KEY\" = :key AND \"DB_ID\" = :dbId", params, rs -> rs.next() ? rs.getString("PARAM_VALUE") : null);

    int count = 1;
    if (value != null) {
      if (value.equals(propValue)) {
        LOG.info("Database property: {} with value: {} already updated for db: {}", prop, propValue, database);
      } else {
        count = jdbcTemplate.update("UPDATE \"DATABASE_PARAMS\" SET \"PARAM_VALUE\" = :value WHERE \"DB_ID\" = :dbId AND " +
            "\"PARAM_KEY\" = :key", params);
      }
    } else {
      count = jdbcTemplate.update("INSERT INTO \"DATABASE_PARAMS\" VALUES (:dbId, :key, :value)", params);
    }
    if (count != 1) {
      //only one row insert or update should happen
      throw new RuntimeException("DATABASE_PARAMS is corrupted for database: " + database);
    }
    return null;
  }
}
