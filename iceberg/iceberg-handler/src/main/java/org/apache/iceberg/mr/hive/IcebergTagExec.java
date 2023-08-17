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

import org.apache.hadoop.hive.ql.parse.AlterTableSnapshotRefSpec;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.util.SnapshotUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergTagExec {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergTagExec.class);

  private IcebergTagExec() {
  }

  public static void createTag(Table table, AlterTableSnapshotRefSpec.CreateSnapshotRefSpec createTagSpec) {
    String tagName = createTagSpec.getRefName();
    Long snapshotId = null;
    if (createTagSpec.getSnapshotId() != null) {
      snapshotId = createTagSpec.getSnapshotId();
    } else if (createTagSpec.getAsOfTime() != null) {
      snapshotId = SnapshotUtil.snapshotIdAsOfTime(table, createTagSpec.getAsOfTime());
    } else {
      snapshotId = table.currentSnapshot().snapshotId();
    }
    LOG.info("Creating tag {} on iceberg table {} with snapshotId {}", tagName, table.name(), snapshotId);
    ManageSnapshots manageSnapshots = table.manageSnapshots();
    manageSnapshots.createTag(tagName, snapshotId);
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
}
