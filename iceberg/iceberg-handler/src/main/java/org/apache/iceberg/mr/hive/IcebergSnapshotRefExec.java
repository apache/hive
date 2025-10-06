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
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.util.SnapshotUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergSnapshotRefExec {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergSnapshotRefExec.class);

  private IcebergSnapshotRefExec() {
  }

  /**
   * Create a branch on the iceberg table
   * @param table the iceberg table
   * @param createBranchSpec Get the basic parameters needed to create a branch
   */
  public static void createBranch(Table table, AlterTableSnapshotRefSpec.CreateSnapshotRefSpec createBranchSpec) {
    String branchName = createBranchSpec.getRefName();
    boolean refExistsAsBranch = refExistsAsBranch(table, branchName);
    if (createBranchSpec.isIfNotExists() && refExistsAsBranch) {
      return;
    }
    boolean isReplace = createBranchSpec.isReplace() && refExistsAsBranch;
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
      snapshotId = Optional.ofNullable(table.currentSnapshot()).map(Snapshot::snapshotId).orElse(null);
    }
    ManageSnapshots manageSnapshots = table.manageSnapshots();
    if (snapshotId != null) {
      createOrReplaceBranch(manageSnapshots, table, isReplace, branchName, snapshotId);
    } else if (isReplace) {
      throw new IllegalArgumentException(
          "Cannot complete replace branch operation on " + branchName + ", main has no snapshot");
    } else {
      LOG.info("Creating a branch {} on an empty iceberg table {}", branchName, table.name());
      manageSnapshots.createBranch(branchName);
    }

    setCreateBranchOptionalParams(createBranchSpec, manageSnapshots, branchName);
    manageSnapshots.commit();
  }

  private static void setCreateBranchOptionalParams(AlterTableSnapshotRefSpec.CreateSnapshotRefSpec createBranchSpec,
      ManageSnapshots manageSnapshots, String branchName) {
    if (createBranchSpec.getMaxRefAgeMs() != null) {
      manageSnapshots.setMaxRefAgeMs(branchName, createBranchSpec.getMaxRefAgeMs());
    }
    if (createBranchSpec.getMinSnapshotsToKeep() != null) {
      manageSnapshots.setMinSnapshotsToKeep(branchName, createBranchSpec.getMinSnapshotsToKeep());
    }
    if (createBranchSpec.getMaxSnapshotAgeMs() != null) {
      manageSnapshots.setMaxSnapshotAgeMs(branchName, createBranchSpec.getMaxSnapshotAgeMs());
    }
  }

  private static void createOrReplaceBranch(ManageSnapshots manageSnapshots, Table table, boolean isReplace,
      String branchName, Long snapshotId) {
    if (isReplace) {
      LOG.info("Replacing branch {} on an iceberg table {} with snapshotId {}", branchName, table.name(), snapshotId);
      manageSnapshots.replaceBranch(branchName, snapshotId);
    } else {
      LOG.info("Creating a branch {} on an iceberg table {} with snapshotId {}", branchName, table.name(), snapshotId);
      manageSnapshots.createBranch(branchName, snapshotId);
    }
  }

  private static boolean refExistsAsTag(Table table, String tagName) {
    SnapshotRef tagRef = table.refs().get(tagName);
    if (tagRef != null) {
      if (tagRef.isTag()) {
        return true;
      } else {
        throw new IllegalArgumentException(
            "Cannot complete create tag operation on " + tagName + ", as it exists as Branch");
      }
    }
    return false;
  }

  private static boolean refExistsAsBranch(Table table, String branchName) {
    SnapshotRef branchRef = table.refs().get(branchName);
    if (branchRef != null) {
      if (branchRef.isBranch()) {
        return true;
      } else {
        throw new IllegalArgumentException(
            "Cannot complete replace branch operation on " + branchName + ", as it exists as Tag");
      }
    }
    return false;
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
    String sourceBranch = replaceSnapshotrefSpec.getSourceRefName();
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
    setOptionalReplaceParams(replaceSnapshotrefSpec, manageSnapshots, sourceBranch);
    manageSnapshots.commit();
  }

  public static void createTag(Table table, AlterTableSnapshotRefSpec.CreateSnapshotRefSpec createTagSpec) {
    String tagName = createTagSpec.getRefName();
    boolean refExistsAsTag = refExistsAsTag(table, tagName);
    if (createTagSpec.isIfNotExists() && refExistsAsTag) {
      return;
    }
    boolean isReplace = createTagSpec.isReplace() && refExistsAsTag;
    Long snapshotId = null;
    if (createTagSpec.getSnapshotId() != null) {
      snapshotId = createTagSpec.getSnapshotId();
    } else if (createTagSpec.getAsOfTime() != null) {
      snapshotId = SnapshotUtil.snapshotIdAsOfTime(table, createTagSpec.getAsOfTime());
    } else {
      snapshotId = table.currentSnapshot().snapshotId();
    }
    ManageSnapshots manageSnapshots = table.manageSnapshots();
    if (isReplace) {
      LOG.info("Replacing tag {} on iceberg table {} with snapshotId {}", tagName, table.name(), snapshotId);
      manageSnapshots.replaceTag(tagName, snapshotId);
    } else {
      LOG.info("Creating tag {} on iceberg table {} with snapshotId {}", tagName, table.name(), snapshotId);
      manageSnapshots.createTag(tagName, snapshotId);
    }
    if (createTagSpec.getMaxRefAgeMs() != null) {
      manageSnapshots.setMaxRefAgeMs(tagName, createTagSpec.getMaxRefAgeMs());
    }

    manageSnapshots.commit();
  }

  public static void dropTag(Table table, AlterTableSnapshotRefSpec.DropSnapshotRefSpec dropTagSpec) {
    String tagName = dropTagSpec.getRefName();
    boolean ifExists = dropTagSpec.getIfExists();

    SnapshotRef snapshotRef = table.refs().get(tagName);
    if (snapshotRef != null || !ifExists) {
      LOG.info("Dropping tag {} on iceberg table {}", tagName, table.name());
      table.manageSnapshots().removeTag(tagName).commit();
    }
  }

  public static void replaceTag(Table table, AlterTableSnapshotRefSpec.ReplaceSnapshotrefSpec replaceTagRefSpec) {
    String sourceTag = replaceTagRefSpec.getSourceRefName();
    long targetSnapshot = replaceTagRefSpec.getTargetSnapshot();
    LOG.info("Replacing tag {} with snapshot {} on iceberg table {}", sourceTag, targetSnapshot, table.name());
    ManageSnapshots manageSnapshots = table.manageSnapshots().replaceTag(sourceTag, targetSnapshot);
    setOptionalReplaceParams(replaceTagRefSpec, manageSnapshots, sourceTag);
    manageSnapshots.commit();
  }

  static void setOptionalReplaceParams(AlterTableSnapshotRefSpec.ReplaceSnapshotrefSpec replaceSnapshotrefSpec,
      ManageSnapshots manageSnapshots, String sourceBranch) {
    if (replaceSnapshotrefSpec.getMaxRefAgeMs() > 0) {
      manageSnapshots.setMaxRefAgeMs(sourceBranch, replaceSnapshotrefSpec.getMaxRefAgeMs());
    }
    if (replaceSnapshotrefSpec.getMaxSnapshotAgeMs() > 0) {
      manageSnapshots.setMaxSnapshotAgeMs(sourceBranch, replaceSnapshotrefSpec.getMaxSnapshotAgeMs());
    }
    if (replaceSnapshotrefSpec.getMinSnapshotsToKeep() > 0) {
      manageSnapshots.setMinSnapshotsToKeep(sourceBranch, replaceSnapshotrefSpec.getMinSnapshotsToKeep());
    }
  }
}
