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
package org.apache.hadoop.hive.ql.parse.repl.events;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.AlterTableMessage;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.EximUtil;

import static org.apache.hadoop.hive.ql.parse.ReplicationSemanticAnalyzer.DUMPTYPE;
import static org.apache.hadoop.hive.ql.parse.ReplicationSemanticAnalyzer.DumpMetaData;

public class AlterTableHandler extends AbstractHandler {
  private final org.apache.hadoop.hive.metastore.api.Table before;
  private final org.apache.hadoop.hive.metastore.api.Table after;
  private final Scenario scenario;

  private enum Scenario {
    ALTER {
      @Override
      DUMPTYPE dumpType() {
        return DUMPTYPE.EVENT_ALTER_TABLE;
      }
    },
    RENAME {
      @Override
      DUMPTYPE dumpType() {
        return DUMPTYPE.EVENT_RENAME_TABLE;
      }
    };

    abstract DUMPTYPE dumpType();
  }

  AlterTableHandler(NotificationEvent event) throws Exception {
    super(event);
    AlterTableMessage atm = deserializer.getAlterTableMessage(event.getMessage());
    before = atm.getTableObjBefore();
    after = atm.getTableObjAfter();
    scenario = scenarioType(before, after);
  }

  private static Scenario scenarioType(org.apache.hadoop.hive.metastore.api.Table before,
      org.apache.hadoop.hive.metastore.api.Table after) {
    return before.getDbName().equals(after.getDbName())
        && before.getTableName().equals(after.getTableName())
        ? Scenario.ALTER
        : Scenario.RENAME;
  }

  @Override
  public void handle(Context withinContext) throws Exception {
    {
      LOG.info("Processing#{} ALTER_TABLE message : {}", fromEventId(), event.getMessage());
      if (Scenario.ALTER == scenario) {
        withinContext.replicationSpec.setIsMetadataOnly(true);
        Table qlMdTableAfter = new Table(after);
        Path metaDataPath = new Path(withinContext.eventRoot, EximUtil.METADATA_NAME);
        EximUtil.createExportDump(
            metaDataPath.getFileSystem(withinContext.hiveConf),
            metaDataPath,
            qlMdTableAfter,
            null,
            withinContext.replicationSpec);
      }
      DumpMetaData dmd = withinContext.createDmd(this);
      dmd.setPayload(event.getMessage());
      dmd.write();
    }
  }

  @Override
  public DUMPTYPE dumpType() {
    return scenario.dumpType();
  }
}
