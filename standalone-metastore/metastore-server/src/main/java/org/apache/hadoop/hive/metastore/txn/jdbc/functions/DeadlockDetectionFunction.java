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

import org.apache.hadoop.hive.metastore.TransactionalMetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.txn.TxnErrorMsg;
import org.apache.hadoop.hive.metastore.txn.entities.LockInfo;
import org.apache.hadoop.hive.metastore.txn.entities.TxnWriteDetails;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionContext;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.GetWriteIdsMappingForTxnIdsHandler;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.hive.metastore.txn.TxnHandler.notifyCommitOrAbortEvent;

/**
 * Single-snapshot scan: loads the wait-for graph from {@code HIVE_LOCKS} in one query, runs
 * Tarjan's SCC, and aborts the youngest txn (wait-die) in each cycle with
 * {@link TxnErrorMsg#ABORT_DEADLOCK}. Only multi-statement DEFAULT transactions can form
 * cycles (all other types hold at most one exclusive resource).
 */
public class DeadlockDetectionFunction implements TransactionalFunction<Integer> {

  private static final Logger LOG = LoggerFactory.getLogger(DeadlockDetectionFunction.class);

  /**
   * Hard cap on edges loaded per scan. Real cycles are 2-5 txns; a graph this large means
   * a metastore-wide pile-up, not a single deadlock. The scan is skipped and
   * {@link MetricsConstants#TOTAL_NUM_DEADLOCK_DETECTOR_GRAPH_TOO_LARGE} fires.
   */
  private static final int MAX_GRAPH_SIZE = 10_000;

  /**
   * Loads WAITER -> BLOCKER edges. Read-side complement of {@code CheckLockFunction}: it
   * writes both {@code HL_BLOCKEDBY_*} columns on the waiter row when it parks the lock,
   * and NULLs them on acquire — so a NOT-NULL JOIN on the {@code (ext_id, int_id)} PK
   * naturally drops acquired locks. No {@code TXN_STATE} filter: HIVE_LOCKS rows only
   * exist for OPEN txns. {@code DISTINCT} dedups DB-side to avoid shipping the duplicate
   * edges a multi-statement waiter generates.
   */
  // No SELECT keyword: addLimitClause prepends one and applies the dialect-correct row cap.
  private static final String LOAD_WAIT_EDGES_SQL_BODY = """
      DISTINCT "WAITER"."HL_TXNID"  AS "WAITER_TXN",
               "BLOCKER"."HL_TXNID" AS "BLOCKER_TXN"
        FROM "HIVE_LOCKS" "WAITER"
        INNER JOIN "HIVE_LOCKS" "BLOCKER"
                ON "BLOCKER"."HL_LOCK_EXT_ID" = "WAITER"."HL_BLOCKEDBY_EXT_ID"
               AND "BLOCKER"."HL_LOCK_INT_ID" = "WAITER"."HL_BLOCKEDBY_INT_ID"
       WHERE "WAITER"."HL_LOCK_STATE" = :waitingState
         AND "WAITER"."HL_TXNID" <> "BLOCKER"."HL_TXNID"
      """;

  private final List<TransactionalMetaStoreEventListener> transactionalListeners;

  public DeadlockDetectionFunction(
      List<TransactionalMetaStoreEventListener> transactionalListeners) {
    this.transactionalListeners = transactionalListeners;
  }

  @Override
  public Integer execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    Map<Long, Set<Long>> graph = new HashMap<>();
    boolean truncated;
    try {
      truncated = loadGraph(jdbcResource, graph);
    } catch (Exception e) {
      LOG.warn("Deadlock detection scan failed: {}", e.getMessage(), e);
      Metrics.getOrCreateCounter(MetricsConstants.TOTAL_NUM_DEADLOCK_DETECTOR_SCAN_FAILURES).inc();
      return 0;
    }
    if (graph.isEmpty()) {
      return 0;
    }
    if (truncated) {
      LOG.warn("Deadlock detector skipping scan: wait-for graph exceeds {} edges. "
          + "A pile-up this large is unlikely to be a single deadlock and needs operator attention.",
          MAX_GRAPH_SIZE);
      Metrics.getOrCreateCounter(MetricsConstants.TOTAL_NUM_DEADLOCK_DETECTOR_GRAPH_TOO_LARGE).inc();
      return 0;
    }

    List<List<Long>> sccs = tarjanSCCs(graph);
    TransactionContext txContext = jdbcResource.getTransactionManager().getActiveTransaction();
    int totalAborted = 0;
    for (List<Long> scc : sccs) {
      // Tarjan emits a singleton SCC for every acyclic node; size < 2 means "not in a cycle".
      if (scc.size() < 2) {
        continue;
      }
      long victim = pickVictim(scc);
      // Per-cycle savepoint isolates a failing abort/notify from earlier successful aborts
      // in this scan.
      Object savepoint;
      try {
        savepoint = txContext.createSavepoint();
      } catch (Exception e) {
        // Must throw, not return: a bare return would commit earlier aborts while
        // reporting 0, desynchronizing DB state from fired listeners.
        Metrics.getOrCreateCounter(MetricsConstants.TOTAL_NUM_DEADLOCK_DETECTOR_SCAN_FAILURES).inc();
        throw new MetaException("Failed to create savepoint for deadlock cycle "
            + scc + ": " + e.getMessage());
      }
      try {
        if (abortVictim(jdbcResource, victim, scc)) {
          totalAborted++;
        }
        txContext.releaseSavepoint(savepoint);
      } catch (Exception e) {
        // Let rollbackToSavepoint propagate on failure: outer @Transactional must abort the
        // whole scan rather than continue on a connection in undefined state. Matches the
        // unguarded pattern in CommitTxnFunction / PerformTimeoutsFunction / HeartbeatTxnRange.
        txContext.rollbackToSavepoint(savepoint);
        LOG.warn("Deadlock victim {} abort or notify failed; rolled back this cycle: {}",
            JavaUtils.txnIdToString(victim), e.getMessage(), e);
        Metrics.getOrCreateCounter(MetricsConstants.TOTAL_NUM_DEADLOCK_DETECTOR_SCAN_FAILURES).inc();
      }
    }
    return totalAborted;
  }

  /**
   * Returns true iff the UPDATE actually flipped OPEN -> ABORTED. Throws on abort/notify
   * failure; caller must hold a savepoint.
   */
  private boolean abortVictim(MultiDataSourceJdbcResource jdbcResource, long victim,
                              List<Long> scc) throws MetaException {
    LOG.info("Deadlock detected. Cycle: {}. Victim: {}",
        scc, JavaUtils.txnIdToString(victim));
    // checkHeartbeat=false: deadlock victims are healthy (heartbeater still pinging);
    // the heartbeat-aware UPDATE in PerformTimeouts would match zero rows here.
    int aborted = new AbortTxnsFunction(Collections.singletonList(victim),
        false, false, false, TxnErrorMsg.ABORT_DEADLOCK).execute(jdbcResource);
    if (aborted != 1) {
      LOG.info("Deadlock victim {} was already in a non-OPEN state when abort was attempted.",
          JavaUtils.txnIdToString(victim));
      return false;
    }
    notifyAbort(jdbcResource, victim);
    Metrics.getOrCreateCounter(MetricsConstants.TOTAL_NUM_DEADLOCKED_TXNS).inc();
    return true;
  }

  /**
   * Loads up to {@link #MAX_GRAPH_SIZE} edges. Returns true iff truncated (server-side cap:
   * ask for max+1 rows so an extra row signals truncation, stopping the DB from materializing
   * a full DISTINCT self-join under contention).
   */
  private boolean loadGraph(MultiDataSourceJdbcResource jdbcResource,
                            Map<Long, Set<Long>> graph) throws MetaException {
    String sql = jdbcResource.getSqlGenerator()
        .addLimitClause(MAX_GRAPH_SIZE + 1, LOAD_WAIT_EDGES_SQL_BODY);
    final boolean[] truncated = {false};
    MapSqlParameterSource params = new MapSqlParameterSource()
        .addValue("waitingState", String.valueOf(LockInfo.LOCK_WAITING), Types.CHAR);
    jdbcResource.getJdbcTemplate().query(sql, params, (ResultSet rs) -> {
      int loaded = 0;
      while (rs.next()) {
        if (loaded >= MAX_GRAPH_SIZE) {
          truncated[0] = true;
          return null;
        }
        long waiter = rs.getLong("WAITER_TXN");
        long blocker = rs.getLong("BLOCKER_TXN");
        graph.computeIfAbsent(waiter, k -> new HashSet<>()).add(blocker);
        graph.computeIfAbsent(blocker, k -> new HashSet<>());
        loaded++;
      }
      return null;
    });
    return truncated[0];
  }

  /** Iterative (not recursive) Tarjan's SCC: a long wait chain must not blow the JVM stack. */
  private static List<List<Long>> tarjanSCCs(Map<Long, Set<Long>> graph) {
    Map<Long, Integer> index = new HashMap<>();
    Map<Long, Integer> lowlink = new HashMap<>();
    Set<Long> onStack = new HashSet<>();
    Deque<Long> sccStack = new ArrayDeque<>();
    List<List<Long>> result = new ArrayList<>();
    int[] counter = {0};
    Deque<DfsFrame> callStack = new ArrayDeque<>();

    for (Long start : graph.keySet()) {
      if (index.containsKey(start)) {
        continue;
      }
      pushFrame(callStack, start, graph, index, lowlink, onStack, sccStack, counter);
      while (!callStack.isEmpty()) {
        DfsFrame frame = callStack.peek();
        boolean recursed = false;
        while (frame.blockers().hasNext()) {
          long blocker = frame.blockers().next();
          if (!index.containsKey(blocker)) {
            pushFrame(callStack, blocker, graph, index, lowlink, onStack, sccStack, counter);
            recursed = true;
            break;
          } else if (onStack.contains(blocker)) {
            lowlink.put(frame.txnId(), Math.min(lowlink.get(frame.txnId()), index.get(blocker)));
          }
        }
        if (recursed) {
          continue;
        }
        if (lowlink.get(frame.txnId()).equals(index.get(frame.txnId()))) {
          List<Long> scc = new ArrayList<>();
          long popped;
          do {
            popped = sccStack.pop();
            onStack.remove(popped);
            scc.add(popped);
          } while (popped != frame.txnId());
          result.add(scc);
        }
        callStack.pop();
        if (!callStack.isEmpty()) {
          DfsFrame parent = callStack.peek();
          lowlink.put(parent.txnId(),
              Math.min(lowlink.get(parent.txnId()), lowlink.get(frame.txnId())));
        }
      }
    }
    return result;
  }

  private static void pushFrame(Deque<DfsFrame> callStack, long txnId, Map<Long, Set<Long>> graph,
                                Map<Long, Integer> index, Map<Long, Integer> lowlink,
                                Set<Long> onStack, Deque<Long> sccStack, int[] counter) {
    index.put(txnId, counter[0]);
    lowlink.put(txnId, counter[0]);
    counter[0]++;
    sccStack.push(txnId);
    onStack.add(txnId);
    callStack.push(new DfsFrame(txnId, graph.getOrDefault(txnId, Collections.emptySet()).iterator()));
  }

  /** Youngest member of the cycle (highest txn ID) — wait-die victim selection. */
  private static long pickVictim(List<Long> scc) {
    return Collections.max(scc);
  }

  private void notifyAbort(MultiDataSourceJdbcResource jdbcResource, long victim)
      throws MetaException {
    if (transactionalListeners == null || transactionalListeners.isEmpty()) {
      return;
    }
    List<TxnWriteDetails> writeDetails = jdbcResource.execute(
        new GetWriteIdsMappingForTxnIdsHandler(Collections.singleton(victim)));
    notifyCommitOrAbortEvent(victim, EventMessage.EventType.ABORT_TXN, TxnType.DEFAULT,
        jdbcResource.getConnection(), writeDetails, transactionalListeners);
  }

  /** Heap-backed stack frame for the iterative Tarjan walker. */
  private record DfsFrame(long txnId, Iterator<Long> blockers) {}
}