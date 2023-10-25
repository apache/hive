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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionsMessage;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;

public class AlterPartitionsHandler extends AbstractEventHandler<AlterPartitionsMessage> {
  private final TableName tableName;
  private final List<List<String>> part_vals;
  private final boolean isTruncateOp;

  AlterPartitionsHandler(NotificationEvent event) {
    super(event);
    AlterPartitionsMessage apm = eventMessage;
    tableName = TableName.fromString(event.getCatName(), apm.getDB(), apm.getTable());
    part_vals = apm.getPartitionValues();
    isTruncateOp = apm.getIsTruncateOp();
  }

  @Override
  AlterPartitionsMessage eventMessage(String stringRepresentation) {
    return deserializer.getAlterPartitionsMessage(stringRepresentation);
  }

  @Override
  public void handle(Context withinContext) throws Exception {
    LOG.info("Processing#{} ALTER_PARTITIONS message : {}", fromEventId(), eventMessageAsJSON);

    // We do not dump partitions during metadata only bootstrap dump (See TableExport
    // .getPartitions(), for bootstrap dump we pass tableSpec with TABLE_ONLY set.). So don't
    // dump partition related events for metadata-only dump.
    if (withinContext.hiveConf.getBoolVar(HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY)) {
      return;
    }

    Table qlMdTable = withinContext.db.getTable(tableName);
    if (!Utils.shouldReplicate(withinContext.replicationSpec, qlMdTable, true,
        withinContext.getTablesForBootstrap(), withinContext.oldReplScope,  withinContext.hiveConf)) {
      return;
    }

    if (!isTruncateOp) {
      withinContext.replicationSpec.setIsMetadataOnly(true);
      List<String> partNames = new ArrayList<>(part_vals.size());
      for (List<String> vals : part_vals) {
        partNames.add(Warehouse.makePartName(qlMdTable.getPartCols(), vals));
      }
      List<Partition> partitions = withinContext.db.getPartitionsByNames(qlMdTable, partNames);
      Path metaDataPath = new Path(withinContext.eventRoot, EximUtil.METADATA_NAME);
      EximUtil.createExportDump(
          metaDataPath.getFileSystem(withinContext.hiveConf),
          metaDataPath,
          qlMdTable,
          partitions,
          withinContext.replicationSpec,
          withinContext.hiveConf);
    }
    DumpMetaData dmd = withinContext.createDmd(this);
    dmd.setPayload(eventMessageAsJSON);
    dmd.write();
  }

  @Override
  public DumpType dumpType() {
    return isTruncateOp ? DumpType.EVENT_TRUNCATE_PARTITION : DumpType.EVENT_ALTER_PARTITION;
  }
}
