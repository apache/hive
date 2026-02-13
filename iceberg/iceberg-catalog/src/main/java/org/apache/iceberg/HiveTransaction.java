/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg;

import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.hive.HiveTableOperations;
import org.apache.iceberg.hive.StagingTableOperations;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transaction implementation that stages metadata changes for atomic batch HMS updates across
 * multiple tables.
 *
 * <p>Extends BaseTransaction to leverage Iceberg's retry and conflict resolution logic while
 * capturing metadata locations instead of publishing directly to HMS.
 */
public class HiveTransaction extends BaseTransaction {

  private static final Logger LOG = LoggerFactory.getLogger(HiveTransaction.class);

  private final HiveTableOperations hiveOps;
  private final StagingTableOperations stagingOps;

  public HiveTransaction(Table table, HiveTableOperations ops) {
    this(table, ops, ops.toStagingOps());
  }

  private HiveTransaction(Table table, HiveTableOperations ops, StagingTableOperations stagingOps) {
    super(table.name(), stagingOps, TransactionType.SIMPLE, ops.current());
    this.hiveOps = ops;
    this.stagingOps = stagingOps;
  }

  public HiveTableOperations ops() {
    return hiveOps;
  }

  public StagingTableOperations stagingOps() {
    return stagingOps;
  }

  /**
   * Cleans up all artifacts produced by the staged commit: new manifests, manifest lists,
   * the metadata JSON file, and uncommitted files.
   *
   * <p>Called by the coordinator when the batch HMS update fails after staging succeeded.
   */
  public void cleanUpOnCommitFailure() {
    // clean up manifests and manifest lists from new snapshots
    cleanAllUpdates();

    // delete the staged metadata JSON file
    deleteMetadataFile();

    // delete uncommitted files tracked by the base transaction
    deleteUncommittedFiles();
  }

  /**
   * Deletes manifest files and manifest lists produced by new snapshots in this transaction.
   * Uses metadata diff (current vs start) to identify new snapshots and their artifacts.
   */
  private void cleanAllUpdates() {
    FileIO io = stagingOps.io();

    // Collect all manifest paths from the base metadata — these must NOT be deleted
    Set<String> baseManifestPaths = Sets.newHashSet();
    for (Snapshot snapshot : startMetadata().snapshots()) {
      try {
        snapshot.allManifests(io).forEach(m -> baseManifestPaths.add(m.path()));
      } catch (RuntimeException e) {
        LOG.warn("Failed to read base manifests for cleanup", e);
      }
    }

    // Find new snapshots added by this transaction and clean their artifacts
    Set<Long> baseSnapshotIds = startMetadata().snapshots().stream()
        .map(Snapshot::snapshotId)
        .collect(Collectors.toSet());

    for (Snapshot snapshot : currentMetadata().snapshots()) {
      if (baseSnapshotIds.contains(snapshot.snapshotId())) {
        continue;
      }

      // Delete new manifest files (not from base)
      try {
        for (ManifestFile manifest : snapshot.allManifests(io)) {
          if (!baseManifestPaths.contains(manifest.path())) {
            io.deleteFile(manifest.path());
          }
        }
      } catch (RuntimeException e) {
        LOG.warn("Failed to clean manifests for snapshot {}", snapshot.snapshotId(), e);
      }

      // Delete the manifest list
      try {
        io.deleteFile(snapshot.manifestListLocation());
      } catch (RuntimeException e) {
        LOG.warn("Failed to clean manifest list {}", snapshot.manifestListLocation(), e);
      }
    }
  }

  /**
   * Deletes the staged metadata JSON file written by StagingTableOperations.doCommit().
   */
  private void deleteMetadataFile() {
    String metadataLocation = stagingOps.metadataLocation();
    if (metadataLocation != null) {
      try {
        stagingOps.io().deleteFile(metadataLocation);
      } catch (RuntimeException e) {
        LOG.warn("Failed to clean metadata file {}", metadataLocation, e);
      }
    }
  }

  /**
   * Deletes uncommitted files tracked during the transaction (e.g. replaced data files).
   */
  private void deleteUncommittedFiles() {
    Tasks.foreach(deletedFiles())
        .suppressFailureWhenFinished()
        .onFailure((file, exc) -> LOG.warn("Failed to delete uncommitted file: {}", file, exc))
        .run(stagingOps.io()::deleteFile);
  }
}
