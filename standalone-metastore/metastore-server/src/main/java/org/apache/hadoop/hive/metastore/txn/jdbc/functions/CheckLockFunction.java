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

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.txn.MetaWrapperException;
import org.apache.hadoop.hive.metastore.txn.TxnErrorMsg;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.entities.LockInfo;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.GetLocksByLockId;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.metastore.utils.LockTypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.metastore.txn.TxnUtils.getEpochFn;
import static org.apache.hadoop.hive.metastore.txn.entities.LockInfo.LOCK_ACQUIRED;

public class CheckLockFunction implements TransactionalFunction<LockResponse> {

  private static final Logger LOG = LoggerFactory.getLogger(CheckLockFunction.class);

  private static final String EXCL_CTAS_ERR_MSG =
      "Failed to initiate a concurrent CTAS operation with the same table name, lockInfo : %s";
  private static final String ZERO_WAIT_READ_ERR_MSG = "Unable to acquire read lock due to an existing exclusive lock {%s}";
  
  private final long extLockId;
  private final long txnId;
  private final boolean zeroWaitReadEnabled;
  private final boolean isExclusiveCTAS;

  public CheckLockFunction(long extLockId, long txnId, 
                           boolean zeroWaitReadEnabled, boolean isExclusiveCTAS) {
    this.extLockId = extLockId;
    this.txnId = txnId;
    this.zeroWaitReadEnabled = zeroWaitReadEnabled;
    this.isExclusiveCTAS = isExclusiveCTAS;
  }

  @SuppressWarnings("squid:S2583")
  @Override
  public LockResponse execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException, NoSuchTxnException, TxnAbortedException, NoSuchLockException {
    LockResponse response = new LockResponse();
    /**
     * todo: Longer term we should pass this from client somehow - this would be an optimization;  once
     * that is in place make sure to build and test "writeSet" below using OperationType not LockType
     * With Static Partitions we assume that the query modifies exactly the partitions it locked.  (not entirely
     * realistic since Update/Delete may have some predicate that filters out all records out of
     * some partition(s), but plausible).  For DP, we acquire locks very wide (all known partitions),
     * but for most queries only a fraction will actually be updated.  #addDynamicPartitions() tells
     * us exactly which ones were written to.  Thus using this trick to kill a query early for
     * DP queries may be too restrictive.
     */
    boolean isPartOfDynamicPartitionInsert = true;
    List<LockInfo> locksBeingChecked = getLocksFromLockId(jdbcResource, extLockId); //being acquired now
    response.setLockid(extLockId);

    //This is the set of entities that the statement represented by extLockId wants to update
    List<LockInfo> writeSet = new ArrayList<>();

    for (LockInfo info : locksBeingChecked) {
      if (!isPartOfDynamicPartitionInsert && info.getType() == LockType.SHARED_WRITE) {
        writeSet.add(info);
      }
    }
    if (!writeSet.isEmpty()) {
      if (writeSet.get(0).getTxnId() == 0) {
        //Write operation always start a txn
        throw new IllegalStateException("Found Write lock for " + JavaUtils.lockIdToString(extLockId) + " but no txnid");
      }


      Object[] args = new Object[writeSet.size() * 4 + 1];
      int index = 0;
      args[index++] = writeSet.get(0).getTxnId();
      StringBuilder sb = new StringBuilder(" \"WS_DATABASE\", \"WS_TABLE\", \"WS_PARTITION\", " +
          "\"WS_TXNID\", \"WS_COMMIT_ID\" " +
          "FROM \"WRITE_SET\" WHERE WS_COMMIT_ID >= ? AND (");//see commitTxn() for more info on this inequality
      for (int i = 0; i < writeSet.size(); i++) {
        sb.append("(\"WS_DATABASE\" = ? AND \"WS_TABLE\" = ? AND (\"WS_PARTITION\" = ? OR ? IS NULL)");
        if (i < writeSet.size() - 1) {
          sb.append(" OR ");
        }
        sb.append(")");
        LockInfo info = writeSet.get(i);
        args[index++] = info.getDb();
        args[index++] = info.getTable();
        args[index++] = info.getPartition();
        args[index++] = info.getPartition();
      }

      WriteSetInfo wsInfo = jdbcResource.getJdbcTemplate().getJdbcTemplate().query(sb.toString(), args, (ResultSet rs) -> {
        WriteSetInfo info = null;
        if (rs.next()) {
          info = new WriteSetInfo();
          info.database = rs.getString("WS_DATABASE");
          info.table = rs.getString("WS_TABLE");
          info.partition = rs.getString("WS_PARTITION");
          info.txnId = rs.getLong("WS_TXNID");
          info.commitId = rs.getLong("WS_COMMIT_ID");
        }
        return info;
      });

      if (wsInfo != null) {
        /**
         * if here, it means we found an already committed txn which overlaps with the current one and
         * it updated the same resource the current txn wants to update.  By First-committer-wins
         * rule, current txn will not be allowed to commit so  may as well kill it now;  This is just an
         * optimization to prevent wasting cluster resources to run a query which is known to be DOA.
         * {@link #commitTxn(CommitTxnRequest)} has the primary responsibility to ensure this.
         * checkLock() runs at READ_COMMITTED, so you could have another (Hive) txn running commitTxn()
         * in parallel and thus writing to WRITE_SET.  commitTxn() logic is properly mutexed to ensure
         * that we don't "miss" any WW conflicts. We could've mutexed the checkLock() and commitTxn()
         * as well but this reduces concurrency for very little gain.
         * Note that update/delete (which runs as dynamic partition insert) acquires a lock on the table,
         * but WRITE_SET has entries for actual partitions updated. Thus this optimization will "miss"
         * the WW conflict, but it will be caught in commitTxn() where actual partitions written are known.
         * This is OK since we want 2 concurrent updates that update different sets of partitions to both commit.
         */
        String resourceName = wsInfo.database + '/' + wsInfo.table;
        if (wsInfo.partition != null) {
          resourceName += '/' + wsInfo.partition;
        }

        String msg = "Aborting " + JavaUtils.txnIdToString(writeSet.get(0).getTxnId()) +
            " since a concurrent committed transaction [" + JavaUtils.txnIdToString(wsInfo.txnId) + "," + wsInfo.commitId +
            "] has already updated resource '" + resourceName + "'";
        LOG.info(msg);
        int count = new AbortTxnsFunction(Collections.singletonList(writeSet.get(0).getTxnId()),
            false, false, false, TxnErrorMsg.ABORT_CONCURRENT).execute(jdbcResource);
        if (count != 1) {
          throw new IllegalStateException(msg + " FAILED!");
        }
        throw new TxnAbortedException(msg);
      }
    }

    String queryStr =
        " \"EX\".*, \"REQ\".\"HL_LOCK_INT_ID\" \"LOCK_INT_ID\", \"REQ\".\"HL_LOCK_TYPE\" \"LOCK_TYPE\" FROM (" +
            " SELECT \"HL_LOCK_EXT_ID\", \"HL_LOCK_INT_ID\", \"HL_TXNID\", \"HL_DB\", \"HL_TABLE\", \"HL_PARTITION\"," +
            " \"HL_LOCK_STATE\", \"HL_LOCK_TYPE\" FROM \"HIVE_LOCKS\"" +
            " WHERE \"HL_LOCK_EXT_ID\" < " + extLockId + ") \"EX\"" +
            " INNER JOIN (" +
            " SELECT \"HL_LOCK_INT_ID\", \"HL_TXNID\", \"HL_DB\", \"HL_TABLE\", \"HL_PARTITION\"," +
            " \"HL_LOCK_TYPE\" FROM \"HIVE_LOCKS\"" +
            " WHERE \"HL_LOCK_EXT_ID\" = " + extLockId + ") \"REQ\"" +
            " ON \"EX\".\"HL_DB\" = \"REQ\".\"HL_DB\"" +
            " AND (\"EX\".\"HL_TABLE\" IS NULL OR \"REQ\".\"HL_TABLE\" IS NULL" +
            " OR \"EX\".\"HL_TABLE\" = \"REQ\".\"HL_TABLE\"" +
            " AND (\"EX\".\"HL_PARTITION\" IS NULL OR \"REQ\".\"HL_PARTITION\" IS NULL" +
            " OR \"EX\".\"HL_PARTITION\" = \"REQ\".\"HL_PARTITION\"))" +
        /*different locks from same txn should not conflict with each other,
          txnId=0 means it's a select or IUD which does not write to ACID table*/
            " WHERE (\"REQ\".\"HL_TXNID\" = 0 OR \"EX\".\"HL_TXNID\" != \"REQ\".\"HL_TXNID\")" +
            " AND ";

      /**EXCLUSIVE lock on partition should prevent SHARED_READ on the table, however there is no reason
        for an EXCLUSIVE on a table to prevent SHARED_READ on a database. Similarly, EXCLUSIVE on a partition
        should not conflict with SHARED_READ on a database.
        SHARED_READ is usually acquired on a database to make sure it's not dropped, while some operation
        is performed on that db (e.g. show tables, created table, etc).
        EXCLUSIVE on an object may mean it's being dropped or overwritten.*/
    String[] whereStr = {
        // shared-read
        " \"REQ\".\"HL_LOCK_TYPE\"=" + LockTypeUtil.sharedRead() + " AND \"EX\".\"HL_LOCK_TYPE\"=" +
            LockTypeUtil.exclusive() + " AND NOT (\"EX\".\"HL_TABLE\" IS NOT NULL AND \"REQ\".\"HL_TABLE\" IS NULL)",
        // exclusive
        " \"REQ\".\"HL_LOCK_TYPE\"=" + LockTypeUtil.exclusive() +
            " AND NOT (\"EX\".\"HL_TABLE\" IS NULL AND \"EX\".\"HL_LOCK_TYPE\"=" +
            LockTypeUtil.sharedRead() + " AND \"REQ\".\"HL_TABLE\" IS NOT NULL)",
        // shared-write
        " \"REQ\".\"HL_LOCK_TYPE\"=" + LockTypeUtil.sharedWrite() + " AND \"EX\".\"HL_LOCK_TYPE\" IN (" +
            LockTypeUtil.exclWrite() + "," + LockTypeUtil.exclusive() + ")",
        // excl-write
        " \"REQ\".\"HL_LOCK_TYPE\"=" + LockTypeUtil.exclWrite() + " AND \"EX\".\"HL_LOCK_TYPE\"!=" +
            LockTypeUtil.sharedRead()
    };

    List<String> subQuery = new ArrayList<>();
    for (String subCond : whereStr) {
      subQuery.add("(" + jdbcResource.getSqlGenerator().addLimitClause(1, queryStr + subCond) + ")");
    }
    String query = String.join(" UNION ALL ", subQuery);

    Boolean success = Objects.requireNonNull(jdbcResource.getJdbcTemplate().query(query, new MapSqlParameterSource(), (ResultSet rs) -> {
      if (rs.next()) {
        try {
          // We acquire all locks for a given query atomically; if 1 blocks, all remain in Waiting state.
          LockInfo blockedBy = new LockInfo(rs);
          long intLockId = rs.getLong("LOCK_INT_ID");
          char lockChar = rs.getString("LOCK_TYPE").charAt(0);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Failure to acquire lock({} intLockId:{} {}), blocked by ({})", JavaUtils.lockIdToString(extLockId),
                intLockId, JavaUtils.txnIdToString(txnId), blockedBy);
          }

          LockType lockType = LockTypeUtil.getLockTypeFromEncoding(lockChar)
              .orElseThrow(() -> new MetaException("Unknown lock type: " + lockChar));

          if ((zeroWaitReadEnabled && LockType.SHARED_READ == lockType || isExclusiveCTAS) && TxnUtils.isValidTxn(txnId)) {
            jdbcResource.getJdbcTemplate().update("DELETE FROM \"HIVE_LOCKS\" WHERE \"HL_LOCK_EXT_ID\" = :extLockId",
                new MapSqlParameterSource().addValue("extLockId", extLockId));

            response.setErrorMessage(String.format(
                isExclusiveCTAS ? EXCL_CTAS_ERR_MSG : ZERO_WAIT_READ_ERR_MSG, blockedBy));
            response.setState(LockState.NOT_ACQUIRED);
            return false;
          }

          int updCnt = jdbcResource.getJdbcTemplate().update("UPDATE \"HIVE_LOCKS\"" +
                  " SET \"HL_BLOCKEDBY_EXT_ID\" = :blockedByExtLockId, \"HL_BLOCKEDBY_INT_ID\" = :blockedByIntLockId " +
                  " WHERE \"HL_LOCK_EXT_ID\" = :extLockId AND \"HL_LOCK_INT_ID\" = :intLockId",
              new MapSqlParameterSource()
                  .addValue("blockedByExtLockId", blockedBy.getExtLockId())
                  .addValue("blockedByIntLockId", blockedBy.getIntLockId())
                  .addValue("extLockId", extLockId)
                  .addValue("intLockId", intLockId));

          if (updCnt != 1) {
            LOG.error("Failure to update lock (extLockId={}, intLockId={}) with the blocking lock's IDs " +
                "(extLockId={}, intLockId={})", extLockId, intLockId, blockedBy.getExtLockId(), blockedBy.getIntLockId());
            throw new RuntimeException("This should never happen: " + JavaUtils.txnIdToString(txnId) + " "
                + JavaUtils.lockIdToString(extLockId) + " " + intLockId);
          }
          response.setState(LockState.WAITING);
          return false;
        } catch (MetaException e) {
          throw new MetaWrapperException(e);
        }
      }
      return true;
    }), "This never should be null, it's just to suppress warnings");

    if (!success) {
      return response;
    }

    // If here, there were no locks that would block any item from 'locksBeingChecked' - acquire them all
    acquire(jdbcResource, locksBeingChecked);

    // We acquired all the locks, so commit and return acquired.
    LOG.debug("Successfully acquired locks: {}", locksBeingChecked);
    response.setState(LockState.ACQUIRED);
    return response;
  }

  // NEVER call this function without first calling heartbeat(long, long)
  private List<LockInfo> getLocksFromLockId(MultiDataSourceJdbcResource jdbcResource, long extLockId) throws MetaException {
    List<LockInfo> locks = jdbcResource.execute(new GetLocksByLockId(extLockId, -1, jdbcResource.getSqlGenerator()));
    if (locks.isEmpty()) {
      throw new MetaException("This should never happen!  We already " +
          "checked the lock(" + JavaUtils.lockIdToString(extLockId) + ") existed but now we can't find it!");
    }
    LOG.debug("Found {} locks for extLockId={}. Locks: {}", locks.size(), extLockId, locks);
    return locks;
  }

  private void acquire(MultiDataSourceJdbcResource jdbcResource, List<LockInfo> locksBeingChecked)
      throws NoSuchLockException, MetaException {
    if (CollectionUtils.isEmpty(locksBeingChecked)) {
      return;
    }
    long txnId = locksBeingChecked.get(0).getTxnId();
    long extLockId = locksBeingChecked.get(0).getExtLockId();
    int rc = jdbcResource.getJdbcTemplate().update("UPDATE \"HIVE_LOCKS\" SET \"HL_LOCK_STATE\" = :state, " +
        //if lock is part of txn, heartbeat info is in txn record
        "\"HL_LAST_HEARTBEAT\" = " + (TxnUtils.isValidTxn(txnId) ? 0 : getEpochFn(jdbcResource.getDatabaseProduct())) +
        ",\"HL_ACQUIRED_AT\" = " + getEpochFn(jdbcResource.getDatabaseProduct()) +
        ",\"HL_BLOCKEDBY_EXT_ID\"=NULL,\"HL_BLOCKEDBY_INT_ID\"=NULL" +
        " WHERE \"HL_LOCK_EXT_ID\" = :extLockId", 
        new MapSqlParameterSource()
            .addValue("state", Character.toString(LOCK_ACQUIRED), Types.CHAR)
            .addValue("extLockId", extLockId));
    
    if (rc < locksBeingChecked.size()) {
      LOG.error("Failure to acquire all locks (acquired: {}, total needed: {}).", rc, locksBeingChecked.size());
      /*select all locks for this ext ID and see which ones are missing*/
      Set<String> notFoundIds = locksBeingChecked.stream()
          .map(lockInfo -> Long.toString(lockInfo.getIntLockId()))
          .collect(Collectors.toSet());
      List<String> foundIds = Objects.requireNonNull(jdbcResource.getJdbcTemplate().query(
          "SELECT \"HL_LOCK_INT_ID\" FROM \"HIVE_LOCKS\" WHERE \"HL_LOCK_EXT_ID\" = :extLockId",
          new MapSqlParameterSource().addValue("extLockId", extLockId), rs -> {
            List<String> ids = new ArrayList<>();
            while (rs.next()) {
              ids.add(rs.getString("HL_LOCK_INT_ID"));
            }
            return ids;
          }), "This never should be null, it's just to suppress warnings");
      
      foundIds.forEach(notFoundIds::remove);
      String errorMsg = String.format("No such lock(s): (%s: %s) %s",
          JavaUtils.lockIdToString(extLockId), String.join(", ", notFoundIds), JavaUtils.txnIdToString(txnId));
      throw new NoSuchLockException(errorMsg);
    }
  }

  static class WriteSetInfo {
    String database;
    String table;
    String partition;
    Long txnId;
    Long commitId;
  }

}
