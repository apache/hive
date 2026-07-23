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
 * Tarjan's SCC, re-verifies each victim on a fresh read, and aborts the youngest hard
 * blocker (wait-die) in each cycle with {@link TxnErrorMsg#ABORT_DEADLOCK}. See
 * {@link #pickVictim} for the hard/soft edge distinction and victim eligibility.
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
   * Loads WAITER -> BLOCKER edges in one snapshot, each with a HARD_BLOCKER flag ("does
   * the waiter also hold an acquired lock" — see {@link #pickVictim}). Read-side complement
   * of {@code CheckLockFunction}: it stamps the {@code HL_BLOCKEDBY_*} columns on the waiter
   * row when it parks the lock and NULLs them on acquire, so the JOIN on the
   * {@code (ext_id, int_id)} PK matches waiting locks only. No {@code TXN_STATE} filter:
   * HIVE_LOCKS rows only exist for OPEN txns. {@code HL_TXNID > 0} excludes txn-less locks
   * (external/Iceberg reads outside a txn block): they all share id 0, so unrelated sessions
   * would otherwise collapse into one vertex and fabricate cycles.
   */
  // No SELECT keyword: addLimitClause prepends one and applies the dialect-correct row cap.
  private static final String LOAD_WAIT_EDGES_SQL_BODY = """
      "WAITER"."HL_TXNID" AS "WAITER_TXN",
      "BLOCKER"."HL_TXNID" AS "BLOCKER_TXN",
      "BLOCKER"."HL_DB",
      "BLOCKER"."HL_TABLE",
      "BLOCKER"."HL_PARTITION",
      CASE WHEN EXISTS (
          SELECT 1
          FROM "HIVE_LOCKS" "HELD"
          WHERE "HELD"."HL_TXNID" = "WAITER"."HL_TXNID"
            AND "HELD"."HL_LOCK_STATE" = :acquiredState
        ) THEN 1 ELSE 0 END AS "HARD_BLOCKER"
      FROM "HIVE_LOCKS" "WAITER"
      INNER JOIN "HIVE_LOCKS" "BLOCKER"
        ON "BLOCKER"."HL_LOCK_EXT_ID" = "WAITER"."HL_BLOCKEDBY_EXT_ID"
        AND "BLOCKER"."HL_LOCK_INT_ID" = "WAITER"."HL_BLOCKEDBY_INT_ID"
      WHERE "WAITER"."HL_LOCK_STATE" = :waitingState
        AND "WAITER"."HL_TXNID" > 0
        AND "BLOCKER"."HL_TXNID" > 0
        AND "WAITER"."HL_TXNID" <> "BLOCKER"."HL_TXNID"
      """;

  private final List<TransactionalMetaStoreEventListener> transactionalListeners;

  public DeadlockDetectionFunction(List<TransactionalMetaStoreEventListener> transactionalListeners) {
    this.transactionalListeners = transactionalListeners;
  }

  @Override
  public Integer execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    Map<Long, Set<Long>> graph = new HashMap<>();
    Set<Long> hardBlockers = new HashSet<>();
    Map<Long, Map<Long, String>> edgeResources = new HashMap<>();
    boolean truncated;
    try {
      truncated = loadGraph(jdbcResource, graph, hardBlockers, edgeResources);
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
      Long victim = pickVictim(scc, hardBlockers);
      if (victim == null) {
        // Unreachable in a consistent snapshot (see pickVictim); defensive skip.
        LOG.warn("Deadlock cycle {} has no hard blocker; skipping.", scc);
        continue;
      }
      // The cycle may have dissolved since the graph was loaded (client abort, lock-wait
      // timeout, concurrent txn timeout): abort only a victim that is still deadlocked.
      if (!stillDeadlocked(jdbcResource, victim)) {
        LOG.info("Deadlock cycle {} dissolved before abort; sparing {}.",
            scc, JavaUtils.txnIdToString(victim));
        continue;
      }
      // Per-cycle savepoint isolates a failing abort/notify from earlier successful aborts
      // in this scan.
      Object savepoint;
      try {
        savepoint = txContext.createSavepoint();
      } catch (Exception e) {
        // Must throw, not return: a bare return would commit earlier aborts while
        // reporting 0, desynchronizing DB state from fired listeners. The service
        // counts the scan failure.
        LOG.error("Failed to create savepoint for deadlock cycle {}", scc, e);
        throw new MetaException("Failed to create savepoint for deadlock cycle "
            + scc + ": " + e.getMessage());
      }
      try {
        if (abortVictim(jdbcResource, victim, formatCycle(scc, graph, edgeResources))) {
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
  private boolean abortVictim(MultiDataSourceJdbcResource jdbcResource, long victim, String cycle)
      throws MetaException {
    LOG.info("Deadlock detected. Cycle: {}. Victim: {}",
        cycle, JavaUtils.txnIdToString(victim));
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
   * Loads up to {@link #MAX_GRAPH_SIZE} edges. Returns true iff truncated (asks for max+1
   * rows so an extra row signals that the graph is incomplete).
   */
  private boolean loadGraph(
      MultiDataSourceJdbcResource jdbcResource, Map<Long, Set<Long>> graph, Set<Long> hardBlockers,
      Map<Long, Map<Long, String>> edgeResources) throws MetaException {
    String sql = jdbcResource.getSqlGenerator()
        .addLimitClause(MAX_GRAPH_SIZE + 1, LOAD_WAIT_EDGES_SQL_BODY);
    MapSqlParameterSource params = new MapSqlParameterSource()
        .addValue("waitingState", String.valueOf(LockInfo.LOCK_WAITING), Types.CHAR)
        .addValue("acquiredState", String.valueOf(LockInfo.LOCK_ACQUIRED), Types.CHAR);
    Boolean truncated = jdbcResource.getJdbcTemplate().query(sql, params, (ResultSet rs) -> {
      int loaded = 0;
      while (rs.next()) {
        if (loaded >= MAX_GRAPH_SIZE) {
          return true;
        }
        long waiter = rs.getLong("WAITER_TXN");
        long blocker = rs.getLong("BLOCKER_TXN");
        graph.computeIfAbsent(waiter, k -> new HashSet<>()).add(blocker);
        graph.computeIfAbsent(blocker, k -> new HashSet<>());
        if (rs.getInt("HARD_BLOCKER") == 1) {
          hardBlockers.add(waiter);
        }
        StringBuilder resource = new StringBuilder(rs.getString("HL_DB"));
        String table = rs.getString("HL_TABLE");
        if (table != null) {
          resource.append('.').append(table);
        }
        String partition = rs.getString("HL_PARTITION");
        if (partition != null) {
          resource.append('/').append(partition);
        }
        edgeResources.computeIfAbsent(waiter, k -> new HashMap<>())
            .putIfAbsent(blocker, resource.toString());
        loaded++;
      }
      return false;
    });
    return Boolean.TRUE.equals(truncated);
  }

  /**
   * Re-reads the wait-for graph and returns true iff the victim is still a hard blocker
   * inside a cycle. Errs toward sparing: a truncated or failed re-read skips this victim
   * and leaves a genuine cycle to the next scan.
   */
  private boolean stillDeadlocked(MultiDataSourceJdbcResource jdbcResource, long victim) {
    Map<Long, Set<Long>> graph = new HashMap<>();
    Set<Long> hardBlockers = new HashSet<>();
    try {
      if (loadGraph(jdbcResource, graph, hardBlockers, new HashMap<>())
          || !hardBlockers.contains(victim)) {
        return false;
      }
    } catch (Exception e) {
      LOG.warn("Deadlock re-verification for {} failed; sparing it this scan: {}",
          JavaUtils.txnIdToString(victim), e.getMessage(), e);
      return false;
    }
    return tarjanSCCs(graph).stream().anyMatch(scc -> scc.size() >= 2 && scc.contains(victim));
  }

  /**
   * Renders the intra-cycle wait edges (waiter -> blocker) with the contended resource, e.g.
   * {@code txnid:21 -[default.t2]-> txnid:22, txnid:22 -[default.t1/p=1]-> txnid:21}.
   */
  private static String formatCycle(
      List<Long> scc, Map<Long, Set<Long>> graph, Map<Long, Map<Long, String>> edgeResources) {
    Set<Long> members = new HashSet<>(scc);
    StringBuilder sb = new StringBuilder();
    scc.stream().sorted().forEach(waiter -> {
      for (long blocker : graph.getOrDefault(waiter, Collections.emptySet())) {
        if (!members.contains(blocker)) {
          continue;
        }
        if (!sb.isEmpty()) {
          sb.append(", ");
        }
        sb.append(JavaUtils.txnIdToString(waiter))
            .append(" -[").append(edgeResources.get(waiter).get(blocker)).append("]-> ")
            .append(JavaUtils.txnIdToString(blocker));
      }
    });
    return sb.toString();
  }

  /** Iterative (not recursive) Tarjan's SCC: a long wait chain must not blow the JVM stack. */
  private static List<List<Long>> tarjanSCCs(Map<Long, Set<Long>> graph) {
    Map<Long, Integer> index = new HashMap<>();
    Map<Long, Integer> lowlink = new HashMap<>();
    Set<Long> onStack = new HashSet<>();
    Deque<Long> sccStack = new ArrayDeque<>();
    List<List<Long>> result = new ArrayList<>();
    Deque<DfsFrame> callStack = new ArrayDeque<>();

    for (Long start : graph.keySet()) {
      if (index.containsKey(start)) {
        continue;
      }
      pushFrame(callStack, start, graph, index, lowlink, onStack, sccStack);
      while (!callStack.isEmpty()) {
        DfsFrame frame = callStack.peek();
        boolean recursed = false;
        while (frame.blockers().hasNext()) {
          long blocker = frame.blockers().next();
          if (!index.containsKey(blocker)) {
            pushFrame(callStack, blocker, graph, index, lowlink, onStack, sccStack);
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

  private static void pushFrame(
      Deque<DfsFrame> callStack, long txnId, Map<Long, Set<Long>> graph, Map<Long, Integer> index,
      Map<Long, Integer> lowlink, Set<Long> onStack, Deque<Long> sccStack) {
    // index only grows, so its size is the next DFS visit number.
    int visit = index.size();
    index.put(txnId, visit);
    lowlink.put(txnId, visit);
    sccStack.push(txnId);
    onStack.add(txnId);
    callStack.push(
        new DfsFrame(txnId, graph.getOrDefault(txnId, Collections.emptySet()).iterator()));
  }

  /**
   * Youngest hard blocker in the cycle (highest txn ID) — wait-die victim selection.
   * A wait edge is <i>hard</i> when its blocker lock is acquired and <i>soft</i> when the
   * blocker lock is itself still waiting (FIFO queue order). Only a hard blocker — a txn
   * holding an acquired lock while it waits — is eligible: aborting it releases what the
   * cycle waits on, and every cycle contains at least one (lock IDs are monotonic, so a
   * cycle must pass through a txn owning two lock requests). A soft-only waiter holds
   * nothing and is never chosen. Returns {@code null} only if the SCC has no hard blocker,
   * unreachable in a consistent snapshot.
   */
  private static Long pickVictim(List<Long> scc, Set<Long> hardBlockers) {
    return scc.stream()
        .filter(hardBlockers::contains)
        .max(Long::compareTo)
        .orElse(null);
  }

  private void notifyAbort(MultiDataSourceJdbcResource jdbcResource, long victim)
      throws MetaException {
    if (transactionalListeners == null || transactionalListeners.isEmpty()) {
      return;
    }
    List<TxnWriteDetails> writeDetails = jdbcResource.execute(
        new GetWriteIdsMappingForTxnIdsHandler(Collections.singleton(victim)));
    // The victim is always a hard blocker = multi-statement txn (pickVictim), and only
    // explicit multi-statement txns are TxnType.DEFAULT, so DEFAULT is exact.
    notifyCommitOrAbortEvent(victim, EventMessage.EventType.ABORT_TXN, TxnType.DEFAULT,
        jdbcResource.getConnection(), writeDetails, transactionalListeners);
  }

  /** Heap-backed stack frame for the iterative Tarjan walker. */
  private record DfsFrame(long txnId, Iterator<Long> blockers) {}
}