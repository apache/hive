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

import org.apache.hadoop.hive.ql.parse.AlterTableMetaRefSpec;
import org.apache.iceberg.ManageSnapshots;
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
   * @param createMetaRefSpec Get the basic parameters needed to create a branch
   */
  public static void createBranch(Table table, AlterTableMetaRefSpec.CreateBranchSpec createMetaRefSpec) {
    String branchName = createMetaRefSpec.getBranchName();
    Long snapshotId = null;
    if (createMetaRefSpec.getSnapshotId() != null) {
      snapshotId = createMetaRefSpec.getSnapshotId();
    } else if (createMetaRefSpec.getAsOfTime() != null) {
      snapshotId = SnapshotUtil.snapshotIdAsOfTime(table, createMetaRefSpec.getAsOfTime());
    } else {
      snapshotId = table.currentSnapshot().snapshotId();
    }
    LOG.info("Creating branch {} on iceberg table {} with snapshotId {}", branchName, table.name(), snapshotId);
    ManageSnapshots manageSnapshots = table.manageSnapshots();
    manageSnapshots.createBranch(branchName, snapshotId);
    if (createMetaRefSpec.getMaxRefAgeMs() != null) {
      manageSnapshots.setMaxRefAgeMs(branchName, createMetaRefSpec.getMaxRefAgeMs());
    }
    if (createMetaRefSpec.getMinSnapshotsToKeep() != null) {
      manageSnapshots.setMinSnapshotsToKeep(branchName, createMetaRefSpec.getMinSnapshotsToKeep());
    }
    if (createMetaRefSpec.getMaxSnapshotAgeMs() != null) {
      manageSnapshots.setMaxSnapshotAgeMs(branchName, createMetaRefSpec.getMaxSnapshotAgeMs());
    }

    manageSnapshots.commit();
  }
}
