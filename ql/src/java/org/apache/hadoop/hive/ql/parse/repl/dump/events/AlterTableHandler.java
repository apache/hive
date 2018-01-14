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
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.AlterTableMessage;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.EximUtil;

import org.apache.hadoop.hive.ql.parse.repl.DumpType;

import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;

class AlterTableHandler extends AbstractEventHandler {
  private final org.apache.hadoop.hive.metastore.api.Table before;
  private final org.apache.hadoop.hive.metastore.api.Table after;
  private final boolean isTruncateOp;
  private final Scenario scenario;

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
    };

    abstract DumpType dumpType();
  }

  AlterTableHandler(NotificationEvent event) throws Exception {
    super(event);
    AlterTableMessage atm = deserializer.getAlterTableMessage(event.getMessage());
    before = atm.getTableObjBefore();
    after = atm.getTableObjAfter();
    isTruncateOp = atm.getIsTruncateOp();
    scenario = scenarioType(before, after);
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

  @Override
  public void handle(Context withinContext) throws Exception {
    LOG.info("Processing#{} ALTER_TABLE message : {}", fromEventId(), event.getMessage());

    Table qlMdTableBefore = new Table(before);
    if (!Utils
        .shouldReplicate(withinContext.replicationSpec, qlMdTableBefore, withinContext.hiveConf)) {
      return;
    }

    if (Scenario.ALTER == scenario) {
      withinContext.replicationSpec.setIsMetadataOnly(true);
      Table qlMdTableAfter = new Table(after);
      Path metaDataPath = new Path(withinContext.eventRoot, EximUtil.METADATA_NAME);
      EximUtil.createExportDump(
          metaDataPath.getFileSystem(withinContext.hiveConf),
          metaDataPath,
          qlMdTableAfter,
          null,
          withinContext.replicationSpec,
          withinContext.hiveConf);
    }
 
    DumpMetaData dmd = withinContext.createDmd(this);
    dmd.setPayload(event.getMessage());
    dmd.write();
  }

  @Override
  public DumpType dumpType() {
    return scenario.dumpType();
  }
}
