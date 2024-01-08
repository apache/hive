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
import org.apache.hadoop.hive.metastore.txn.entities.TxnStatus;
import org.apache.hadoop.hive.metastore.txn.jdbc.QueryHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.ResultSet;
import java.sql.SQLException;

public class CountOpenTxnsHandler implements QueryHandler<Integer> {

  private static final Logger LOG = LoggerFactory.getLogger(CountOpenTxnsHandler.class);

  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) throws MetaException {
    return "SELECT COUNT(*) FROM \"TXNS\" WHERE \"TXN_STATE\" = :state";
  }

  @Override
  public SqlParameterSource getQueryParameters() {
    return new MapSqlParameterSource().addValue("state", TxnStatus.OPEN.getSqlConst());
  }

  @Override
  public Integer extractData(ResultSet rs) throws SQLException, DataAccessException {
    if (!rs.next()) {
      LOG.error("Transaction database not properly configured, can't find txn_state from TXNS.");
      return -1;
    } else {
      Long numOpen = rs.getLong(1);
      if (numOpen > Integer.MAX_VALUE) {
        LOG.error("Open transaction count above {}, can't count that high!", Integer.MAX_VALUE);
        return -1;
      } else {
        return numOpen.intValue();
      }
    }
  }
  
}
