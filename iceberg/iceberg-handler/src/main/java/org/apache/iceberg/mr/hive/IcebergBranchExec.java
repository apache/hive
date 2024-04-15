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

package org.apache.iceberg.mr.hive;

import java.util.Optional;
import org.apache.hadoop.hive.ql.parse.AlterTableSnapshotRefSpec;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.util.SnapshotUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergBranchExec {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergBranchExec.class);

  private IcebergBranchExec() {
  }

  /**
   * Create a branch on the iceberg table
   * @param table the iceberg table
   * @param createBranchSpec Get the basic parameters needed to create a branch
   */
  public static void createBranch(Table table, AlterTableSnapshotRefSpec.CreateSnapshotRefSpec createBranchSpec) {
    String branchName = createBranchSpec.getRefName();
    Long snapshotId = null;
    if (createBranchSpec.getSnapshotId() != null) {
      snapshotId = createBranchSpec.getSnapshotId();
    } else if (createBranchSpec.getAsOfTime() != null) {
      snapshotId = SnapshotUtil.snapshotIdAsOfTime(table, createBranchSpec.getAsOfTime());
    } else if (createBranchSpec.getAsOfTag() != null) {
      String tagName = createBranchSpec.getAsOfTag();
      SnapshotRef snapshotRef = table.refs().get(tagName);
      if (snapshotRef != null && snapshotRef.isTag()) {
        snapshotId = snapshotRef.snapshotId();
      } else {
        throw new IllegalArgumentException(String.format("Tag %s does not exist", tagName));
      }
    } else {
      snapshotId = Optional.ofNullable(table.currentSnapshot()).map(snapshot -> snapshot.snapshotId()).orElse(null);
    }
    ManageSnapshots manageSnapshots = table.manageSnapshots();
    if (snapshotId != null) {
      LOG.info("Creating a branch {} on an iceberg table {} with snapshotId {}", branchName, table.name(), snapshotId);
      manageSnapshots.createBranch(branchName, snapshotId);
    } else {
      LOG.info("Creating a branch {} on an empty iceberg table {}", branchName, table.name());
      manageSnapshots.createBranch(branchName);
    }
    if (createBranchSpec.getMaxRefAgeMs() != null) {
      manageSnapshots.setMaxRefAgeMs(branchName, createBranchSpec.getMaxRefAgeMs());
    }
    if (createBranchSpec.getMinSnapshotsToKeep() != null) {
      manageSnapshots.setMinSnapshotsToKeep(branchName, createBranchSpec.getMinSnapshotsToKeep());
    }
    if (createBranchSpec.getMaxSnapshotAgeMs() != null) {
      manageSnapshots.setMaxSnapshotAgeMs(branchName, createBranchSpec.getMaxSnapshotAgeMs());
    }

    manageSnapshots.commit();
  }

  public static void dropBranch(Table table, AlterTableSnapshotRefSpec.DropSnapshotRefSpec dropBranchSpec) {
    String branchName = dropBranchSpec.getRefName();
    boolean ifExists = dropBranchSpec.getIfExists();

    SnapshotRef snapshotRef = table.refs().get(branchName);
    if (snapshotRef != null || !ifExists) {
      LOG.info("Dropping branch {} on iceberg table {}", branchName, table.name());
      table.manageSnapshots().removeBranch(branchName).commit();
    }
  }

  public static void renameBranch(Table table, AlterTableSnapshotRefSpec.RenameSnapshotrefSpec renameSnapshotrefSpec) {
    String sourceBranch = renameSnapshotrefSpec.getSourceBranchName();
    String targetBranch = renameSnapshotrefSpec.getTargetBranchName();

    LOG.info("Renaming branch {} to {} on iceberg table {}", sourceBranch, targetBranch, table.name());
    table.manageSnapshots().renameBranch(sourceBranch, targetBranch).commit();
  }

  public static void replaceBranch(Table table,
      AlterTableSnapshotRefSpec.ReplaceSnapshotrefSpec replaceSnapshotrefSpec) {
    String sourceBranch = replaceSnapshotrefSpec.getSourceBranchName();
    ManageSnapshots manageSnapshots;
    if (replaceSnapshotrefSpec.isReplaceBySnapshot()) {
      long targetSnapshot = replaceSnapshotrefSpec.getTargetSnapshot();
      LOG.info("Replacing branch {} with snapshot {} on iceberg table {}", sourceBranch, targetSnapshot, table.name());
      manageSnapshots = table.manageSnapshots().replaceBranch(sourceBranch, targetSnapshot);
    } else {
      String targetBranch = replaceSnapshotrefSpec.getTargetBranchName();
      LOG.info("Replacing branch {} with branch {} on iceberg table {}", sourceBranch, targetBranch, table.name());
      manageSnapshots = table.manageSnapshots().replaceBranch(sourceBranch, targetBranch);
    }
    if (replaceSnapshotrefSpec.getMaxRefAgeMs() > 0) {
      manageSnapshots.setMaxRefAgeMs(sourceBranch, replaceSnapshotrefSpec.getMaxRefAgeMs());
    }
    if (replaceSnapshotrefSpec.getMaxSnapshotAgeMs() > 0) {
      manageSnapshots.setMaxSnapshotAgeMs(sourceBranch, replaceSnapshotrefSpec.getMaxSnapshotAgeMs());
    }
    if (replaceSnapshotrefSpec.getMinSnapshotsToKeep() > 0) {
      manageSnapshots.setMinSnapshotsToKeep(sourceBranch, replaceSnapshotrefSpec.getMinSnapshotsToKeep());
    }
    manageSnapshots.commit();
  }
}
