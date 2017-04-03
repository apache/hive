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
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionMessage;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.EximUtil;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.hadoop.hive.ql.parse.ReplicationSemanticAnalyzer.DUMPTYPE;
import static org.apache.hadoop.hive.ql.parse.ReplicationSemanticAnalyzer.DumpMetaData;

public class AlterPartitionHandler extends AbstractHandler {
  private final org.apache.hadoop.hive.metastore.api.Partition after;
  private final org.apache.hadoop.hive.metastore.api.Table tableObject;
  private final Scenario scenario;

  AlterPartitionHandler(NotificationEvent event) throws Exception {
    super(event);
    AlterPartitionMessage apm = deserializer.getAlterPartitionMessage(event.getMessage());
    tableObject = apm.getTableObj();
    org.apache.hadoop.hive.metastore.api.Partition before = apm.getPtnObjBefore();
    after = apm.getPtnObjAfter();
    scenario = scenarioType(before, after);
  }

  private enum Scenario {
    ALTER {
      @Override
      DUMPTYPE dumpType() {
        return DUMPTYPE.EVENT_ALTER_PARTITION;
      }
    },
    RENAME {
      @Override
      DUMPTYPE dumpType() {
        return DUMPTYPE.EVENT_RENAME_PARTITION;
      }
    };

    abstract DUMPTYPE dumpType();
  }

  private static Scenario scenarioType(org.apache.hadoop.hive.metastore.api.Partition before,
      org.apache.hadoop.hive.metastore.api.Partition after) {
    Iterator<String> beforeValIter = before.getValuesIterator();
    Iterator<String> afterValIter = after.getValuesIterator();
    while(beforeValIter.hasNext()) {
      if (!beforeValIter.next().equals(afterValIter.next())) {
        return Scenario.RENAME;
      }
    }
    return Scenario.ALTER;
  }

  @Override
  public void handle(Context withinContext) throws Exception {
    LOG.info("Processing#{} ALTER_PARTITION message : {}", fromEventId(), event.getMessage());

    if (Scenario.ALTER == scenario) {
      withinContext.replicationSpec.setIsMetadataOnly(true);
      Table qlMdTable = new Table(tableObject);
      List<Partition> qlPtns = new ArrayList<>();
      qlPtns.add(new Partition(qlMdTable, after));
      Path metaDataPath = new Path(withinContext.eventRoot, EximUtil.METADATA_NAME);
      EximUtil.createExportDump(
          metaDataPath.getFileSystem(withinContext.hiveConf),
          metaDataPath,
          qlMdTable,
          qlPtns,
          withinContext.replicationSpec);
    }
    DumpMetaData dmd = withinContext.createDmd(this);
    dmd.setPayload(event.getMessage());
    dmd.write();
  }

  @Override
  public DUMPTYPE dumpType() {
    return scenario.dumpType();
  }
}
