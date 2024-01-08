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
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.tools.SQLGenerator;
import org.apache.hadoop.hive.metastore.txn.entities.TxnStatus;
import org.apache.hadoop.hive.metastore.txn.jdbc.QueryHandler;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Optional;

public class GetOpenTxnTypeAndLockHandler implements QueryHandler<TxnType> {
  
  private final SQLGenerator sqlGenerator;
  private final long txnId;

  public GetOpenTxnTypeAndLockHandler(SQLGenerator sqlGenerator, long txnId) {
    this.sqlGenerator = sqlGenerator;
    this.txnId = txnId;
  }

  /**
   * Note that by definition select for update is divorced from update, i.e. you executeQuery() to read
   * and then executeUpdate().  One other alternative would be to actually update the row in TXNS but
   * to the same value as before thus forcing db to acquire write lock for duration of the transaction.
   * SELECT ... FOR UPDATE locks the row until the transaction commits or rolls back.
   * Second connection using `SELECT ... FOR UPDATE` will suspend until the lock is released.
   * @return the txnType wrapped in an {@link Optional}
   */
  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) throws MetaException {
    return sqlGenerator.addForUpdateClause(
        "SELECT \"TXN_TYPE\" FROM \"TXNS\" WHERE \"TXN_ID\" = :id AND \"TXN_STATE\" = :state");
  }

  @Override
  public SqlParameterSource getQueryParameters() {
    return new MapSqlParameterSource()
        .addValue("id", txnId)
        .addValue("state", TxnStatus.OPEN.getSqlConst(), Types.CHAR);
  }

  @Override
  public TxnType extractData(ResultSet rs) throws SQLException, DataAccessException {
    return rs.next() ? TxnType.findByValue(rs.getInt(1)) : null;
  }

}
