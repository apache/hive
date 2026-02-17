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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import org.apache.iceberg.hive.HiveTableOperations;
import org.apache.iceberg.hive.StagingTableOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transaction implementation that stages metadata changes for atomic batch HMS updates across
 * multiple tables.
 *
 * <p>Extends BaseTransaction to reuse its update tracking and commit machinery while
 * capturing metadata locations instead of publishing directly to HMS.
 */
public class HiveTransaction extends BaseTransaction {

  private static final Logger LOG = LoggerFactory.getLogger(HiveTransaction.class);

  private static final MethodHandle CLEANUP_HANDLE = initCleanupHandle();

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

  @Override
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
    try {
      CLEANUP_HANDLE.invoke(this);
    } catch (Throwable t) {
      throw new IllegalStateException("Failed to invoke cleanUpOnCommitFailure", t);
    }
    // delete the staged metadata JSON file
    deleteMetadataFile();
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

  private static MethodHandle initCleanupHandle() {
    try {
      MethodHandles.Lookup lookup =
          MethodHandles.privateLookupIn(BaseTransaction.class, MethodHandles.lookup());
      return lookup.findSpecial(
          BaseTransaction.class,
          "cleanUpOnCommitFailure",
          MethodType.methodType(void.class),
          BaseTransaction.class
      );
    } catch (Exception e) {
      throw new ExceptionInInitializerError(e);
    }
  }

}
