/**
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
package org.apache.hadoop.hive.metastore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidWriteIds;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.RawStore.FullTableName;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.model.MTableWrite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;

public class MmCleanerThread extends Thread implements MetaStoreThread {
  private final static Logger LOG = LoggerFactory.getLogger(MmCleanerThread.class);
  private HiveConf conf;
  private int threadId;
  private AtomicBoolean stop;
  private long intervalMs;
  private long heartbeatTimeoutMs, absTimeoutMs, abortedGraceMs;
  /** Time override for tests. Only used for MM timestamp logic, not for the thread timing. */
  private Supplier<Long> timeOverride = null;

  public MmCleanerThread(long intervalMs) {
    this.intervalMs = intervalMs;
  }

  @VisibleForTesting
  void overrideTime(Supplier<Long> timeOverride) {
    this.timeOverride = timeOverride;
  }

  private long getTimeMs() {
    return timeOverride == null ? System.currentTimeMillis() : timeOverride.get();
  }

  @Override
  public void setHiveConf(HiveConf conf) {
    this.conf = conf;
    heartbeatTimeoutMs = HiveConf.getTimeVar(
        conf, ConfVars.HIVE_METASTORE_MM_HEARTBEAT_TIMEOUT, TimeUnit.MILLISECONDS);
    absTimeoutMs = HiveConf.getTimeVar(
        conf, ConfVars.HIVE_METASTORE_MM_ABSOLUTE_TIMEOUT, TimeUnit.MILLISECONDS);
    abortedGraceMs = HiveConf.getTimeVar(
        conf, ConfVars.HIVE_METASTORE_MM_ABORTED_GRACE_PERIOD, TimeUnit.MILLISECONDS);
    if (heartbeatTimeoutMs > absTimeoutMs) {
      throw new RuntimeException("Heartbeat timeout " + heartbeatTimeoutMs
          + " cannot be larger than the absolute timeout " + absTimeoutMs);
    }
  }

  @Override
  public void setThreadId(int threadId) {
    this.threadId = threadId;
  }

  @Override
  public void init(AtomicBoolean stop, AtomicBoolean looped) throws MetaException {
    this.stop = stop;
    setPriority(MIN_PRIORITY);
    setDaemon(true);
  }

  @Override
  public void run() {
    // Only get RS here, when we are already on the thread.
    RawStore rs = getRs();
    while (true) {
      if (checkStop()) return;
      long endTimeNs = System.nanoTime() + intervalMs * 1000000L;

      runOneIteration(rs);

      if (checkStop()) return;
      long waitTimeMs = (endTimeNs - System.nanoTime()) / 1000000L;
      if (waitTimeMs <= 0) continue;
      try {
        Thread.sleep(waitTimeMs);
      } catch (InterruptedException e) {
        LOG.error("Thread was interrupted and will now exit");
        return;
      }
    }
  }

  private RawStore getRs() {
    try {
      return RawStoreProxy.getProxy(conf, conf,
          conf.getVar(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL), threadId);
    } catch (MetaException e) {
      LOG.error("Failed to get RawStore; the thread will now die", e);
      throw new RuntimeException(e);
    }
  }

  private boolean checkStop() {
    if (!stop.get()) return false;
    LOG.info("Stopping due to an external request");
    return true;
  }

  @VisibleForTesting
  void runOneIteration(RawStore rs) {
    // We only get the names here; we want to get and process each table in a separate DB txn.
    List<FullTableName> mmTables = null;
    try {
      mmTables = rs.getAllMmTablesForCleanup();
    } catch (MetaException e) {
      LOG.error("Failed to get tables", e);
      return;
    }
    for (FullTableName tableName : mmTables) {
      try {
        processOneTable(tableName, rs);
      } catch (MetaException e) {
        LOG.error("Failed to process " + tableName, e);
      }
    }
  }

  private void processOneTable(FullTableName table, RawStore rs) throws MetaException {
    // 1. Time out writes that have been running for a while.
    //    a) Heartbeat timeouts (not enabled right now as heartbeat is not implemented).
    //    b) Absolute timeouts.
    //    c) Gaps that have the next ID and the derived absolute timeout. This is a small special
    //       case that can happen if we increment next ID but fail to insert the write ID record,
    //       which we do in separate txns to avoid making the conflict-prone increment txn longer.
    LOG.info("Processing table " + table);
    Table t = rs.getTable(table.dbName, table.tblName);
    HashSet<Long> removeWriteIds = new HashSet<>(), cleanupOnlyWriteIds = new HashSet<>();
    getWritesThatReadyForCleanUp(t, table, rs, removeWriteIds, cleanupOnlyWriteIds);

    // 2. Delete the aborted writes' files from the FS.
    deleteAbortedWriteIdFiles(table, rs, t, removeWriteIds);
    deleteAbortedWriteIdFiles(table, rs, t, cleanupOnlyWriteIds);
    // removeWriteIds-s now only contains the writes that were fully cleaned up after.

    // 3. Advance the watermark.
    advanceWatermark(table, rs, removeWriteIds);
  }

  private void getWritesThatReadyForCleanUp(Table t, FullTableName table, RawStore rs,
      HashSet<Long> removeWriteIds, HashSet<Long> cleanupOnlyWriteIds) throws MetaException {
    // We will generally ignore errors here. First, we expect some conflicts; second, we will get
    // the final view of things after we do (or try, at any rate) all the updates.
    long watermarkId = t.isSetMmWatermarkWriteId() ? t.getMmWatermarkWriteId() : -1,
        nextWriteId = t.isSetMmNextWriteId() ? t.getMmNextWriteId() : 0;
    long now = getTimeMs(), earliestOkHeartbeatMs = now - heartbeatTimeoutMs,
        earliestOkCreateMs = now - absTimeoutMs, latestAbortedMs = now - abortedGraceMs;

    List<MTableWrite> writes = rs.getTableWrites(
        table.dbName, table.tblName, watermarkId, nextWriteId);
    ListIterator<MTableWrite> iter = writes.listIterator(writes.size());
    long expectedId = -1, nextCreated = -1;
    // We will go in reverse order and add aborted writes for the gaps that have a following
    // write ID that would imply that the previous one (created earlier) would have already
    // expired, had it been open and not updated.
    while (iter.hasPrevious()) {
      MTableWrite write = iter.previous();
      addTimedOutMissingWriteIds(rs, table.dbName, table.tblName, write.getWriteId(),
          nextCreated, expectedId, earliestOkHeartbeatMs, cleanupOnlyWriteIds, now);
      expectedId = write.getWriteId() - 1;
      nextCreated = write.getCreated();
      char state = write.getState().charAt(0);
      if (state == HiveMetaStore.MM_WRITE_ABORTED) {
        if (write.getLastHeartbeat() < latestAbortedMs) {
          removeWriteIds.add(write.getWriteId());
        } else {
          cleanupOnlyWriteIds.add(write.getWriteId());
        }
      } else if (state == HiveMetaStore.MM_WRITE_OPEN && write.getCreated() < earliestOkCreateMs) {
        // TODO: also check for heartbeat here.
        if (expireTimedOutWriteId(rs, table.dbName, table.tblName, write.getWriteId(),
            now, earliestOkCreateMs, earliestOkHeartbeatMs, cleanupOnlyWriteIds)) {
          cleanupOnlyWriteIds.add(write.getWriteId());
        }
      }
    }
    addTimedOutMissingWriteIds(rs, table.dbName, table.tblName, watermarkId,
        nextCreated, expectedId, earliestOkHeartbeatMs, cleanupOnlyWriteIds, now);
  }

  private void advanceWatermark(
      FullTableName table, RawStore rs, HashSet<Long> cleanedUpWriteIds) {
    if (!rs.openTransaction()) {
      LOG.error("Cannot open transaction");
      return;
    }
    boolean success = false;
    try {
      Table t = rs.getTable(table.dbName, table.tblName);
      if (t == null) {
        return;
      }
      long watermarkId = t.getMmWatermarkWriteId();
      List<Long> writeIds = rs.getTableWriteIds(table.dbName, table.tblName, watermarkId,
          t.getMmNextWriteId(), HiveMetaStore.MM_WRITE_COMMITTED);
      long expectedId = watermarkId + 1;
      boolean hasGap = false;
      Iterator<Long> idIter = writeIds.iterator();
      while (idIter.hasNext()) {
        long next = idIter.next();
        if (next < expectedId) continue;
        while (next > expectedId) {
          if (!cleanedUpWriteIds.contains(expectedId)) {
            hasGap = true;
            break;
          }
          ++expectedId;
        }
        if (hasGap) break;
        ++expectedId;
      }
      // Make sure we also advance over the trailing aborted ones.
      if (!hasGap) {
        while (cleanedUpWriteIds.contains(expectedId)) {
          ++expectedId;
        }
      }
      long newWatermarkId = expectedId - 1;
      if (newWatermarkId > watermarkId) {
        t.setMmWatermarkWriteId(newWatermarkId);
        rs.alterTable(table.dbName, table.tblName, t);
        rs.deleteTableWrites(table.dbName, table.tblName, -1, expectedId);
      }
      success = true;
    } catch (Exception ex) {
      // TODO: should we try a couple times on conflicts? Aborted writes cannot be unaborted.
      LOG.error("Failed to advance watermark", ex);
      rs.rollbackTransaction();
    }
    if (success) {
      tryCommit(rs);
    }
  }

  private void deleteAbortedWriteIdFiles(
      FullTableName table, RawStore rs, Table t, HashSet<Long> cleanUpWriteIds) {
    if (cleanUpWriteIds.isEmpty()) return;
    if (t.getPartitionKeysSize() > 0) {
      for (String location : rs.getAllPartitionLocations(table.dbName, table.tblName)) {
        deleteAbortedWriteIdFiles(location, cleanUpWriteIds);
      }
    } else {
      deleteAbortedWriteIdFiles(t.getSd().getLocation(), cleanUpWriteIds);
    }
  }

  private void deleteAbortedWriteIdFiles(String location, HashSet<Long> abortedWriteIds) {
    LOG.info("Looking for " + abortedWriteIds.size() + " aborted write output in " + location);
    Path path = new Path(location);
    FileSystem fs;
    FileStatus[] files;
    try {
      fs = path.getFileSystem(conf);
      if (!fs.exists(path)) {
        LOG.warn(path + " does not exist; assuming that the cleanup is not needed.");
        return;
      }
      // TODO# this doesn't account for list bucketing. Do nothing now, ACID will solve all problems.
      files = fs.listStatus(path);
    } catch (Exception ex) {
      LOG.error("Failed to get files for " + path + "; cannot ensure cleanup for any writes");
      abortedWriteIds.clear();
      return;
    }
    for (FileStatus file : files) {
      Path childPath = file.getPath();
      if (!file.isDirectory()) {
        LOG.warn("Skipping a non-directory file " + childPath);
        continue;
      }
      Long writeId = ValidWriteIds.extractWriteId(childPath);
      if (writeId == null) {
        LOG.warn("Skipping an unknown directory " + childPath);
        continue;
      }
      if (!abortedWriteIds.contains(writeId.longValue())) continue;
      try {
        if (!fs.delete(childPath, true)) throw new IOException("delete returned false");
      } catch (Exception ex) {
        LOG.error("Couldn't delete " + childPath + "; not cleaning up " + writeId, ex);
        abortedWriteIds.remove(writeId.longValue());
      }
    }
  }

  private boolean expireTimedOutWriteId(RawStore rs, String dbName,
      String tblName, long writeId, long now, long earliestOkCreatedMs,
      long earliestOkHeartbeatMs, HashSet<Long> cleanupOnlyWriteIds) {
    if (!rs.openTransaction()) {
      return false;
    }
    try {
      MTableWrite tw = rs.getTableWrite(dbName, tblName, writeId);
      if (tw == null) {
        // The write have been updated since the time when we thought it has expired.
        tryCommit(rs);
        return true;
      }
      char state = tw.getState().charAt(0);
      if (state != HiveMetaStore.MM_WRITE_OPEN
          || (tw.getCreated() > earliestOkCreatedMs
              && tw.getLastHeartbeat() > earliestOkHeartbeatMs)) {
        tryCommit(rs);
        return true; // The write has been updated since the time when we thought it has expired.
      }
      tw.setState(String.valueOf(HiveMetaStore.MM_WRITE_ABORTED));
      tw.setLastHeartbeat(now);
      rs.updateTableWrite(tw);
    } catch (Exception ex) {
      LOG.error("Failed to update an expired table write", ex);
      rs.rollbackTransaction();
      return false;
    }
    boolean result = tryCommit(rs);
    if (result) {
      cleanupOnlyWriteIds.add(writeId);
    }
    return result;
  }

  private boolean tryCommit(RawStore rs) {
    try {
      return rs.commitTransaction();
    } catch (Exception ex) {
      LOG.error("Failed to commit transaction", ex);
      return false;
    }
  }

  private boolean addTimedOutMissingWriteIds(RawStore rs, String dbName, String tblName,
      long foundPrevId, long nextCreated, long expectedId, long earliestOkHeartbeatMs,
      HashSet<Long> cleanupOnlyWriteIds, long now) throws MetaException {
    // Assume all missing ones are created at the same time as the next present write ID.
    // We also assume missing writes never had any heartbeats.
    if (nextCreated >= earliestOkHeartbeatMs || expectedId < 0) return true;
    Table t = null;
    List<Long> localCleanupOnlyWriteIds = new ArrayList<>();
    while (foundPrevId < expectedId) {
      if (t == null && !rs.openTransaction()) {
        LOG.error("Cannot open transaction; skipping");
        return false;
      }
      try {
        if (t == null) {
          t = rs.getTable(dbName, tblName);
        }
        // We don't need to double check if the write exists; the unique index will cause an error.
        rs.createTableWrite(t, expectedId, HiveMetaStore.MM_WRITE_ABORTED, now);
      } catch (Exception ex) {
        // TODO: don't log conflict exceptions?.. although we barely ever expect them.
        LOG.error("Failed to create a missing table write", ex);
        rs.rollbackTransaction();
        return false;
      }
      localCleanupOnlyWriteIds.add(expectedId);
      --expectedId;
    }
    boolean result = (t == null || tryCommit(rs));
    if (result) {
      cleanupOnlyWriteIds.addAll(localCleanupOnlyWriteIds);
    }
    return result;
  }
}
