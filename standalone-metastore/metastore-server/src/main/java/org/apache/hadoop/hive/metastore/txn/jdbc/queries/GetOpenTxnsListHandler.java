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
import org.apache.hadoop.hive.metastore.txn.entities.OpenTxn;
import org.apache.hadoop.hive.metastore.txn.entities.OpenTxnList;
import org.apache.hadoop.hive.metastore.txn.entities.TxnStatus;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.jdbc.QueryHandler;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class GetOpenTxnsListHandler implements QueryHandler<OpenTxnList> {

  private static final Logger LOG = LoggerFactory.getLogger(GetOpenTxnsListHandler.class);

  //language=SQL
  private static final String OPEN_TXNS_QUERY = "SELECT \"TXN_ID\", \"TXN_STATE\", \"TXN_TYPE\", "
      + "(%s - \"TXN_STARTED\") FROM \"TXNS\" ORDER BY \"TXN_ID\"";
  //language=SQL
  private static final String OPEN_TXNS_INFO_QUERY = "SELECT \"TXN_ID\", \"TXN_STATE\", \"TXN_TYPE\", "
      + "(%s - \"TXN_STARTED\"), \"TXN_USER\", \"TXN_HOST\", \"TXN_STARTED\", \"TXN_LAST_HEARTBEAT\" "
      + "FROM \"TXNS\" ORDER BY \"TXN_ID\"";
  
  private final boolean infoFields;
  private final long openTxnTimeOutMillis;

  public GetOpenTxnsListHandler(boolean infoFields, long openTxnTimeOutMillis) {
    this.infoFields = infoFields;
    this.openTxnTimeOutMillis = openTxnTimeOutMillis;
  }

  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) throws MetaException {
    return String.format(infoFields ? OPEN_TXNS_INFO_QUERY : OPEN_TXNS_QUERY, TxnUtils.getEpochFn(databaseProduct));  
  }

  @Override
  public SqlParameterSource getQueryParameters() {
    return null;
  }

  // We need to figure out the HighWaterMark and the list of open transactions.
  /*
   * This method need guarantees from
   * {@link #openTxns(OpenTxnRequest)} and  {@link #commitTxn(CommitTxnRequest)}.
   * It will look at the TXNS table and find each transaction between the max(txn_id) as HighWaterMark
   * and the max(txn_id) before the TXN_OPENTXN_TIMEOUT period as LowWaterMark.
   * Every transaction that is not found between these will be considered as open, since it may appear later.
   * openTxns must ensure, that no new transaction will be opened with txn_id below LWM and
   * commitTxn must ensure, that no committed transaction will be removed before the time period expires.
   */
  @Override
  public OpenTxnList extractData(ResultSet rs) throws SQLException, DataAccessException {
    /*
     * We can use the maximum txn_id from the TXNS table as high water mark, since the commitTxn and the Initiator
     * guarantees, that the transaction with the highest txn_id will never be removed from the TXNS table.
     * If there is a pending openTxns, that is already acquired it's sequenceId but not yet committed the insert
     * into the TXNS table, will have either a lower txn_id than HWM and will be listed in the openTxn list,
     * or will have a higher txn_id and don't effect this getOpenTxns() call.
     */
    long hwm = 0;
    long openTxnLowBoundary = 0;
    List<OpenTxn> txnInfos = new ArrayList<>();

    while (rs.next()) {
      long txnId = rs.getLong(1);
      long age = rs.getLong(4);
      hwm = txnId;
      if (age < openTxnTimeOutMillis) {
        // We will consider every gap as an open transaction from the previous txnId
        openTxnLowBoundary++;
        while (txnId > openTxnLowBoundary) {
          // Add an empty open transaction for every missing value
          txnInfos.add(new OpenTxn(openTxnLowBoundary, TxnStatus.OPEN, TxnType.DEFAULT));
          LOG.debug("Open transaction added for missing value in TXNS {}",
              JavaUtils.txnIdToString(openTxnLowBoundary));
          openTxnLowBoundary++;
        }
      } else {
        openTxnLowBoundary = txnId;
      }
      TxnStatus state = TxnStatus.fromString(rs.getString(2));
      if (state == TxnStatus.COMMITTED) {
        // This is only here, to avoid adding this txnId as possible gap
        continue;
      }
      OpenTxn txnInfo = new OpenTxn(txnId, state, TxnType.findByValue(rs.getInt(3)));
      if (infoFields) {
        txnInfo.setUser(rs.getString(5));
        txnInfo.setHost(rs.getString(6));
        txnInfo.setStartedTime(rs.getLong(7));
        txnInfo.setLastHeartBeatTime(rs.getLong(8));
      }
      txnInfos.add(txnInfo);
    }
    LOG.debug("Got OpenTxnList with hwm: {} and openTxnList size {}.", hwm, txnInfos.size());
    return new OpenTxnList(hwm, txnInfos);
  }
}
