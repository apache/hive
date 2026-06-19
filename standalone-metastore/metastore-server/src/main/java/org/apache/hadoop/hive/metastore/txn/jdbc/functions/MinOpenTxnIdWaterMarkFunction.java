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
import org.apache.hadoop.hive.metastore.txn.entities.TxnStatus;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.OpenTxnTimeoutLowBoundaryTxnIdHandler;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.sql.ResultSet;
import java.sql.Types;
import java.util.Objects;

public class MinOpenTxnIdWaterMarkFunction implements TransactionalFunction<Long> {

  private static final Logger LOG = LoggerFactory.getLogger(MinOpenTxnIdWaterMarkFunction.class);
  
  private final long openTxnTimeOutMillis;

  public MinOpenTxnIdWaterMarkFunction(long openTxnTimeOutMillis) {
    this.openTxnTimeOutMillis = openTxnTimeOutMillis;
  }

  @Override
  public Long execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    /**
     * We try to find the highest transactionId below everything was committed or aborted.
     * For that we look for the lowest open transaction in the TXNS and the TxnMinTimeout boundary,
     * because it is guaranteed there won't be open transactions below that.
     */
    long minOpenTxn = Objects.requireNonNull(jdbcResource.getJdbcTemplate().query(
        "SELECT MIN(\"TXN_ID\") FROM \"TXNS\" WHERE \"TXN_STATE\"= :status",
        new MapSqlParameterSource().addValue("status", TxnStatus.OPEN.getSqlConst(), Types.CHAR),
        (ResultSet rs) -> {
          if (!rs.next()) {
            throw new IllegalStateException("Scalar query returned no rows?!?!!");
          }
          long id = rs.getLong(1);
          if (rs.wasNull()) {
            id = Long.MAX_VALUE;
          }
          return id;
        }));
    long lowWaterMark = jdbcResource.execute(new OpenTxnTimeoutLowBoundaryTxnIdHandler(openTxnTimeOutMillis));
    LOG.debug("MinOpenTxnIdWaterMark calculated with minOpenTxn {}, lowWaterMark {}", minOpenTxn, lowWaterMark);
    return Long.min(minOpenTxn, lowWaterMark + 1);
  }
}
