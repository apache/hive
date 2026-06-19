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
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.txn.entities.TxnStatus;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

public class EnsureValidTxnFunction implements TransactionalFunction<Void> {

  private final long txnId;

  public EnsureValidTxnFunction(long txnId) {
    this.txnId = txnId;
  }

  @Override
  public Void execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException, NoSuchTxnException, TxnAbortedException {
    SqlParameterSource paramSource = new MapSqlParameterSource().addValue("txnId", txnId);
    // We need to check whether this transaction is valid and open
    TxnStatus status = jdbcResource.getJdbcTemplate().query("SELECT \"TXN_STATE\" FROM \"TXNS\" WHERE \"TXN_ID\" = :txnId",
        paramSource, rs -> rs.next() ? TxnStatus.fromString(rs.getString("TXN_STATE")) : null);

    if (status == null) {
      // todo: add LIMIT 1 instead of count - should be more efficient
      boolean alreadyCommitted = Boolean.TRUE.equals(jdbcResource.getJdbcTemplate().query("SELECT COUNT(*) FROM \"COMPLETED_TXN_COMPONENTS\" WHERE \"CTC_TXNID\" = :txnId",
          paramSource, rs -> {
            // todo: strictly speaking you can commit an empty txn, thus 2nd conjunct is wrong but
            // only possible for for multi-stmt txns
            return rs.next() && rs.getInt(1) > 0;
          }));

      if (alreadyCommitted) {
        // makes the message more informative - helps to find bugs in client code
        throw new NoSuchTxnException("Transaction " + JavaUtils.txnIdToString(txnId)
            + " is already committed.");
      }
      throw new NoSuchTxnException("No such transaction " + JavaUtils.txnIdToString(txnId));
    } else {
      if (status == TxnStatus.ABORTED) {
        throw new TxnAbortedException("Transaction " + JavaUtils.txnIdToString(txnId)
            + " already aborted");
        // todo: add time of abort, which is not currently tracked. Requires schema change
      }
    }
    return null;
  }

}
