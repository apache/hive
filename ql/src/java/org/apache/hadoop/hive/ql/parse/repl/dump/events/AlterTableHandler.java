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
package org.apache.hadoop.hive.ql.parse.repl.dump.events;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.repl.ReplScope;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.AlterTableMessage;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.EximUtil;

import org.apache.hadoop.hive.ql.parse.repl.DumpType;

import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;

import java.util.Set;

class AlterTableHandler extends AbstractEventHandler<AlterTableMessage> {
  private final org.apache.hadoop.hive.metastore.api.Table before;
  private final org.apache.hadoop.hive.metastore.api.Table after;
  private final boolean isTruncateOp;
  private Scenario scenario;

  private enum Scenario {
    ALTER {
      @Override
      DumpType dumpType() {
        return DumpType.EVENT_ALTER_TABLE;
      }
    },
    RENAME {
      @Override
      DumpType dumpType() {
        return DumpType.EVENT_RENAME_TABLE;
      }
    },
    TRUNCATE {
      @Override
      DumpType dumpType() {
        return DumpType.EVENT_TRUNCATE_TABLE;
      }
    },
    DROP {
      @Override
      DumpType dumpType() {
        return DumpType.EVENT_RENAME_DROP_TABLE;
      }
    };


    abstract DumpType dumpType();
  }

  AlterTableHandler(NotificationEvent event) throws Exception {
    super(event);
    before = eventMessage.getTableObjBefore();
    after = eventMessage.getTableObjAfter();
    isTruncateOp = eventMessage.getIsTruncateOp();
    scenario = scenarioType(before, after);
  }

  @Override
  AlterTableMessage eventMessage(String stringRepresentation) {
    return deserializer.getAlterTableMessage(stringRepresentation);
  }

  private Scenario scenarioType(org.apache.hadoop.hive.metastore.api.Table before,
      org.apache.hadoop.hive.metastore.api.Table after) {
    if (before.getDbName().equals(after.getDbName())
        && before.getTableName().equals(after.getTableName())) {
      return isTruncateOp ? Scenario.TRUNCATE : Scenario.ALTER;
    } else {
      return Scenario.RENAME;
    }
  }

  private boolean isSetForBootstrapByReplaceHandler(Context withinContext, String tblName) {
    return (withinContext.oldReplScope != null)
            && !(ReplUtils.tableIncludedInReplScope(withinContext.oldReplScope, tblName))
            && (ReplUtils.tableIncludedInReplScope(withinContext.replScope, tblName));
  }

  // Return true, if event needs to be dumped, else return false.
  private boolean handleForTableLevelReplication(Context withinContext, String tblName) {
    // For alter, if the table does not satisfy the new policy then ignore the event. In case of replace
    // policy, if the table does not satisfy the old policy, then ignore the event. As, if the table satisfy the new
    // policy, then the table will be bootstrapped by replace handler anb if the table does not satisfy the new policy,
    // then anyways the table should be ignored.
    if (!ReplUtils.tableIncludedInReplScope(withinContext.replScope, tblName)) {
      // In case of replace, it will be dropped during load. In normal case just ignore the alter event.
      LOG.debug("Table " + tblName + " does not satisfy the policy");
      return false;
    } else if ((withinContext.oldReplScope != null)
            && (!ReplUtils.tableIncludedInReplScope(withinContext.oldReplScope, tblName))) {
      LOG.debug("Table " + tblName + " is set for bootstrap");
      return false;
    } else {
      // Table satisfies both old (if its there) and current policy, dump the alter event.
      return true;
    }
  }

  // Return true, if event needs to be dumped, else return false.
  private boolean handleForTableLevelReplicationForRename(Context withinContext, String oldName, String newName) {
    // If the table is renamed after being added to the list of tables to be bootstrapped, then remove it from the
    // list of tables to be bootstrapped.
    boolean oldTableIsPresent = withinContext.removeFromListOfTablesForBootstrap(oldName);

    ReplScope oldPolicy = withinContext.oldReplScope == null ? withinContext.replScope : withinContext.oldReplScope;
    if (ReplUtils.tableIncludedInReplScope(oldPolicy, oldName)) {
      // If old table satisfies the filter, but the new table does not, then the old table should be dropped.
      // This should be done, only if the old table is not in the list of tables to be bootstrapped which is a multi
      // rename case. In case of multi rename, only the first rename should do the drop.
      if (!ReplUtils.tableIncludedInReplScope(withinContext.replScope, newName)) {
        if (oldTableIsPresent) {
          // If the old table was present in the list of tables to be bootstrapped, then just ignore the event.
          return false;
        } else {
          scenario = Scenario.DROP;
          LOG.info("Table " + oldName + " will be dropped as the table is renamed to " + newName);
          return true;
        }
      }

      // If the old table was in the list of tables to be bootstrapped which is a multi rename case, the old table
      // is removed from the list of tables to be bootstrapped and new one is added.
      if (oldTableIsPresent) {
        withinContext.addToListOfTablesForBootstrap(newName);
        return false;
      }

      // All the subsequent events on this table newName are going to be skipped, so the rename is also skipped.
      if (isSetForBootstrapByReplaceHandler(withinContext, newName)) {
        // If the old table satisfies the new policy and is not in the list of tables to be bootstrapped
        // (as per previous check based on oldTableIsPresent), then drop it.
        if (ReplUtils.tableIncludedInReplScope(withinContext.replScope, oldName)) {
          scenario = Scenario.DROP;
          LOG.info("Table " + oldName + " will be dropped as the table " + newName + " will be bootstrapped.");
          return true;
        }
        LOG.info("Table " + newName + " is set to be bootstrapped by replace policy handler.");
        return false;
      }

      // If both old and new table satisfies the filter and old table is present at target, then dump the rename event.
      LOG.info("Both old " + oldName + " and new table " + newName + " satisfies the filter");
      return true;
    } else  {
      // if the old table does not satisfies the filter, but the new one satisfies, then the new table should be
      // added to the list of tables to be bootstrapped and don't dump the event.
      if (ReplUtils.tableIncludedInReplScope(withinContext.replScope, newName)) {
        LOG.info("Table " + newName + " is added for bootstrap " + " during rename from " + oldName);
        withinContext.addToListOfTablesForBootstrap(newName);
        return false;
      }

      // In case of replace policy, even if the old table matches the new policy, none of the events including create
      // table will be replayed as the old table is set of bootstrap by replace handler. So rename event can be skipped.
      if (ReplUtils.tableIncludedInReplScope(withinContext.replScope, oldName)) {
        LOG.info("Table " + oldName + " is set for bootstrap. So all events are skipped for the table.");
        return false;
      }

      // if both old and new table does not satisfies the filter, then don't dump the event.
      LOG.info("Both old and new table does not satisfies the filter");
      return false;
    }
  }

  @Override
  public void handle(Context withinContext) throws Exception {
    LOG.info("Processing#{} ALTER_TABLE message : {}", fromEventId(), eventMessageAsJSON);

    Table qlMdTableBefore = new Table(before);
    Set<String> bootstrapTableList;
    ReplScope oldReplScope;
    if (Scenario.RENAME == scenario) {
      // Handling for table level replication is done in handleForTableLevelReplication method.
      bootstrapTableList = null;
      oldReplScope = null;
    } else {
      bootstrapTableList = withinContext.getTablesForBootstrap();
      oldReplScope = withinContext.oldReplScope;
    }

    if (!Utils
        .shouldReplicate(withinContext.replicationSpec, qlMdTableBefore,
            true, bootstrapTableList, oldReplScope, withinContext.hiveConf)) {
      return;
    }

    if (withinContext.hiveConf.getBoolVar(HiveConf.ConfVars.REPL_BOOTSTRAP_ACID_TABLES)) {
      if (!AcidUtils.isTransactionalTable(before) && AcidUtils.isTransactionalTable(after)) {
        LOG.info("The table " + after.getTableName() + " is converted to ACID table." +
                " It will be replicated with bootstrap load as hive.repl.bootstrap.acid.tables is set to true.");
        return;
      }
    }

    // If the tables are filtered based on name or policy is replaced, then needs to handle differently.
    if (!withinContext.replScope.includeAllTables() || withinContext.oldReplScope != null) {
      String oldName = before.getTableName();
      String newName = after.getTableName();
      boolean needDump;
      if (Scenario.RENAME == scenario) {
        needDump = handleForTableLevelReplicationForRename(withinContext, oldName, newName);
      } else {
        needDump = handleForTableLevelReplication(withinContext, oldName);
      }
      if (!needDump) {
        LOG.info("Alter event for table " + oldName + " is skipped from dumping");
        return;
      }
    }

    if (Scenario.ALTER == scenario) {
      withinContext.replicationSpec.setIsMetadataOnly(true);
      Table qlMdTableAfter = new Table(after);
      Path metaDataPath = new Path(withinContext.eventRoot, EximUtil.METADATA_NAME);

      // If we are not dumping metadata about a table, we shouldn't be dumping basic statistics
      // as well, since that won't be accurate. So reset them to what they would look like for an
      // empty table.
      if (withinContext.hiveConf.getBoolVar(HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY)) {
        qlMdTableAfter.setStatsStateLikeNewTable();
      }

      EximUtil.createExportDump(
          metaDataPath.getFileSystem(withinContext.hiveConf),
          metaDataPath,
          qlMdTableAfter,
          null,
          withinContext.replicationSpec,
          withinContext.hiveConf);
    }
 
    DumpMetaData dmd = withinContext.createDmd(this);
    dmd.setPayload(eventMessageAsJSON);
    dmd.write();
  }

  @Override
  public DumpType dumpType() {
    return scenario.dumpType();
  }
}
