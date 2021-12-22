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
    if (before.getDbName().equalsIgnoreCase(after.getDbName())
        && before.getTableName().equalsIgnoreCase(after.getTableName())) {
      return isTruncateOp ? Scenario.TRUNCATE : Scenario.ALTER;
    } else {
      return Scenario.RENAME;
    }
  }

  // Return true, if event needs to be dumped, else return false.
  private boolean handleRenameForReplacePolicy(Context withinContext, String oldName, String newName) {
    // If the table is renamed after being added to the list of tables to be bootstrapped, then remove it from the
    // list of tables to be bootstrapped.
    boolean oldTableInBootstrapList = withinContext.removeFromListOfTablesForBootstrap(oldName);

    // If the new table satisfies the new policy, then add it to the list of table to be bootstrapped.
    if (ReplUtils.tableIncludedInReplScope(withinContext.replScope, newName)) {
      LOG.info("Table " + newName + " is added for bootstrap " + " during rename from " + oldName);
      withinContext.addToListOfTablesForBootstrap(newName);
    }

    if (ReplUtils.tableIncludedInReplScope(withinContext.oldReplScope, oldName)) {
      // If the old table was in the list of tables to be bootstrapped which is a multi rename case, the old table
      // is removed from the list of tables, else drop event is dumped for the old table. This is done even if the
      // table is not present at target. This makes the logic simple and has no side effect as drop is idempotent.
      if (oldTableInBootstrapList) {
        return false;
      }

      // To keep the logic simple, rename with replace policy is always drop and create.
      scenario = Scenario.DROP;
      LOG.info("Table " + oldName + " will be dropped as the table is renamed to " + newName);
      return true;
    }

    // If the old table does not satisfy the old policy then event can be skipped. in case the new table satisfies the
    // new policy, it is already added to the list of tables to be bootstrapped.
    return false;
  }

  // return true, if event needs to be dumped, else return false.
  private boolean handleRenameForTableLevelReplication(Context withinContext, String oldName, String newName) {
    if (ReplUtils.tableIncludedInReplScope(withinContext.replScope, oldName)) {
      // If the table is renamed after being added to the list of tables to be bootstrapped, then remove it from the
      // list of tables to be bootstrapped.
      boolean oldTableInBootstrapList = withinContext.removeFromListOfTablesForBootstrap(oldName);

      // If old table satisfies the policy, but the new table does not, then the old table should be dropped.
      // This should be done, only if the old table is not in the list of tables to be bootstrapped which is a multi
      // rename case. In case of multi rename, only the first rename should do the drop.
      if (!ReplUtils.tableIncludedInReplScope(withinContext.replScope, newName)) {
        if (oldTableInBootstrapList) {
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
      if (oldTableInBootstrapList) {
        withinContext.addToListOfTablesForBootstrap(newName);
        return false;
      }

      // If both old and new table satisfies the policy and old table is present at target, then dump the rename event.
      LOG.info("both old and new table satisfies the policy");
      return true;
    } else  {
      // if the old table does not satisfies the policy, but the new one satisfies, then the new table should be
      // added to the list of tables to be bootstrapped and don't dump the event.
      if (ReplUtils.tableIncludedInReplScope(withinContext.replScope, newName)) {
        LOG.info("Table " + newName + " is added for bootstrap " + " during rename from " + oldName);
        withinContext.addToListOfTablesForBootstrap(newName);
        return false;
      }

      // if both old and new table does not satisfies the policy, then don't dump the event.
      LOG.info("both old and new table does not satisfies the policy");
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
      // Handling for table level replication is not done in shouldReplicate method for rename events. Its done in
      // handleRenameForReplacePolicy and handleRenameForTableLevelReplication method.
      bootstrapTableList = null;
      oldReplScope = null;
    } else {
      // This check was ignored for alter table event during event filter.
      if (!ReplUtils.tableIncludedInReplScope(withinContext.replScope, before.getTableName())) {
        LOG.debug("Table " + before.getTableName() + " does not satisfy the policy");
        return;
      }
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

    if (Scenario.RENAME == scenario) {
      String oldName = before.getTableName();
      String newName = after.getTableName();
      boolean needDump = true;
      if (withinContext.oldReplScope != null && !withinContext.oldReplScope.equals(withinContext.replScope)) {
        needDump = handleRenameForReplacePolicy(withinContext, oldName, newName);
      } else if (!withinContext.replScope.includeAllTables()) {
        needDump = handleRenameForTableLevelReplication(withinContext, oldName, newName);
      }
      if (!needDump) {
        LOG.info("Rename event for table " + oldName + " to " + newName + " is skipped from dumping");
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
      if (Utils.shouldDumpMetaDataOnly(withinContext.hiveConf)
              || Utils.shouldDumpMetaDataOnlyForExternalTables(qlMdTableAfter, withinContext.hiveConf)) {
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
