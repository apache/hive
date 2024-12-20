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

import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class GetValidWriteIdsForTableFunction implements TransactionalFunction<TableValidWriteIds> {

  private final ValidTxnList validTxnList;
  private final String fullTableName;

  public GetValidWriteIdsForTableFunction(ValidTxnList validTxnList, String fullTableName) {
    this.validTxnList = validTxnList;
    this.fullTableName = fullTableName;
  }

  // Method to get the Valid write ids list for the given table
  // Input fullTableName is expected to be of format <db_name>.<table_name>
  @Override
  public TableValidWriteIds execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    String[] names = TxnUtils.getDbTableName(fullTableName);
    assert (names.length == 2);

    // Find the writeId high watermark based upon txnId high watermark. If found, then, need to
    // traverse through all write Ids less than writeId HWM to make exceptions list.
    // The writeHWM = min(NEXT_WRITE_ID.nwi_next-1, max(TXN_TO_WRITE_ID.t2w_writeid under txnHwm))
    long writeIdHwm = Objects.requireNonNull(jdbcResource.getJdbcTemplate().query(
        "SELECT MAX(\"T2W_WRITEID\") FROM \"TXN_TO_WRITE_ID\" WHERE \"T2W_TXNID\" <= :txnHwm "
            + " AND \"T2W_DATABASE\" = :db AND \"T2W_TABLE\" = :table",
        new MapSqlParameterSource()
            .addValue("txnHwm", validTxnList.getHighWatermark())
            .addValue("db", names[0])
            .addValue("table", names[1]), new HwmExtractor()));

    // If no writeIds allocated by txns under txnHwm, then find writeHwm from NEXT_WRITE_ID.
    if (writeIdHwm <= 0) {
      // Need to subtract 1 as nwi_next would be the next write id to be allocated but we need highest
      // allocated write id.
      writeIdHwm = Objects.requireNonNull(jdbcResource.getJdbcTemplate().query(
          "SELECT \"NWI_NEXT\" -1 FROM \"NEXT_WRITE_ID\" WHERE \"NWI_DATABASE\" = :db AND \"NWI_TABLE\" = :table",
          new MapSqlParameterSource()
              .addValue("db", names[0])
              .addValue("table", names[1]), new HwmExtractor()));
    }

    final List<Long> invalidWriteIdList = new ArrayList<>();
    final BitSet abortedBits = new BitSet();
    final AtomicLong minOpenWriteId = new AtomicLong(Long.MAX_VALUE);
    final AtomicBoolean foundValidUncompactedWrite = new AtomicBoolean(false);

    // As writeIdHwm is known, query all writeIds under the writeId HWM.
    // If any writeId under HWM is allocated by txn > txnId HWM or belongs to open/aborted txns,
    // then will be added to invalid list. The results should be sorted in ascending order based
    // on write id. The sorting is needed as exceptions list in ValidWriteIdList would be looked-up
    // using binary search.
    jdbcResource.getJdbcTemplate().query(
        "SELECT \"T2W_TXNID\", \"T2W_WRITEID\" FROM \"TXN_TO_WRITE_ID\" WHERE \"T2W_WRITEID\" <= :writeIdHwm" +
            " AND \"T2W_DATABASE\" = :db AND \"T2W_TABLE\" = :table ORDER BY \"T2W_WRITEID\" ASC",
        new MapSqlParameterSource()
            .addValue("writeIdHwm", writeIdHwm)
            .addValue("db", names[0])
            .addValue("table", names[1]), rs -> {
          while (rs.next()) {
            long txnId = rs.getLong(1);
            long writeId = rs.getLong(2);
            if (validTxnList.isTxnValid(txnId)) {
              // Skip if the transaction under evaluation is already committed.
              foundValidUncompactedWrite.set(true);
              continue;
            }
            // The current txn is either in open or aborted state.
            // Mark the write ids state as per the txn state.
            invalidWriteIdList.add(writeId);
            if (validTxnList.isTxnAborted(txnId)) {
              abortedBits.set(invalidWriteIdList.size() - 1);
            } else {
              minOpenWriteId.set(Math.min(minOpenWriteId.get(), writeId));
            }
          }
          return null;
        });

    // If we have compacted writes and some invalid writes on the table,
    // return the lowest invalid write as a writeIdHwm and set it as invalid.
    if (!foundValidUncompactedWrite.get()) {
      long writeId = invalidWriteIdList.isEmpty() ? -1 : invalidWriteIdList.get(0);
      invalidWriteIdList.clear();
      abortedBits.clear();

      if (writeId != -1) {
        invalidWriteIdList.add(writeId);
        writeIdHwm = writeId;
        if (writeId != minOpenWriteId.get()) {
          abortedBits.set(0);
        }
      }
    }
    ByteBuffer byteBuffer = ByteBuffer.wrap(abortedBits.toByteArray());
    TableValidWriteIds owi = new TableValidWriteIds(fullTableName, writeIdHwm, invalidWriteIdList, byteBuffer);
    if (minOpenWriteId.get() < Long.MAX_VALUE) {
      owi.setMinOpenWriteId(minOpenWriteId.get());
    }
    return owi;
  }

  private static class HwmExtractor implements ResultSetExtractor<Long> {

    @Override
    public Long extractData(ResultSet rs) throws SQLException, DataAccessException {
      if (rs.next()) {
        return rs.getLong(1);
      } else {
        // Need to initialize to 0 to make sure if nobody modified this table, then current txn
        // shouldn't read any data.
        // If there is a conversion from non-acid to acid table, then by default 0 would be assigned as
        // writeId for data from non-acid table and so writeIdHwm=0 would ensure those data are readable by any txns.
        return 0L;
      }
    }
  }

}
