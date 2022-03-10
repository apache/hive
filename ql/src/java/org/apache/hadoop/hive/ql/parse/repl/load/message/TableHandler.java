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
package org.apache.hadoop.hive.ql.parse.repl.load.message;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterTableMessage;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.repl.ReplExternalTables;
import org.apache.hadoop.hive.ql.exec.repl.ReplLoadTask;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.ImportSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;
import org.apache.hadoop.hive.ql.parse.repl.load.MetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class TableHandler extends AbstractMessageHandler {
  private static final long DEFAULT_WRITE_ID = 0L;
  private static final Logger LOG = LoggerFactory.getLogger(TableHandler.class);

  @Override
  public List<Task<?>> handle(Context context) throws SemanticException {
    try {
      List<Task<?>> importTasks = new ArrayList<>();
      boolean isExternal = false, isLocationSet = false;
      String parsedLocation = null;

      DumpType eventType = context.dmd.getDumpType();
      Tuple tuple = extract(context);
      MetaData rv = EximUtil.getMetaDataFromLocation(context.location, context.hiveConf);

      if (tuple.isExternalTable) {
        isLocationSet = true;
        isExternal = true;
        Table table = new Table(rv.getTable());
        parsedLocation = ReplExternalTables.externalTableLocation(context.hiveConf, table.getSd().getLocation());
      }

      context.nestedContext.setConf(context.hiveConf);
      EximUtil.SemanticAnalyzerWrapperContext x =
          new EximUtil.SemanticAnalyzerWrapperContext(
              context.hiveConf, context.db, readEntitySet, writeEntitySet, importTasks, context.log,
              context.nestedContext);
      x.setEventType(eventType);

      // REPL LOAD is not partition level. It is always DB or table level. So, passing null for partition specs.
      if (TableType.VIRTUAL_VIEW.name().equals(rv.getTable().getTableType())) {
        importTasks.add(ReplLoadTask.createViewTask(rv, context.dbName, context.hiveConf,
                context.getDumpDirectory(), context.getMetricCollector()));
      } else {
        ImportSemanticAnalyzer.prepareImport(false, isLocationSet, isExternal, false,
            (context.precursor != null), parsedLocation, null, context.dbName,
            null, context.location, x, updatedMetadata, context.getTxnMgr(), tuple.writeId, rv,
                context.getDumpDirectory(), context.getMetricCollector());
      }

      Task<?> openTxnTask = x.getOpenTxnTask();
      if (openTxnTask != null && !importTasks.isEmpty()) {
        for (Task<?> t : importTasks) {
          openTxnTask.addDependentTask(t);
        }
        importTasks.add(openTxnTask);
      }

      return importTasks;
    } catch (RuntimeException e){
      throw e;
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  private Tuple extract(Context context) throws SemanticException {
    try {
      String tableType = null;
      long writeId = DEFAULT_WRITE_ID;
      switch (context.dmd.getDumpType()) {
      case EVENT_CREATE_TABLE:
      case EVENT_ADD_PARTITION:
        Path metadataPath = new Path(context.location, EximUtil.METADATA_NAME);
        MetaData rv = EximUtil.readMetaData(
            metadataPath.getFileSystem(context.hiveConf),
            metadataPath
        );
        tableType = rv.getTable().getTableType();
        break;
      case EVENT_ALTER_TABLE:
        AlterTableMessage alterTableMessage =
            deserializer.getAlterTableMessage(context.dmd.getPayload());
        tableType = alterTableMessage.getTableObjAfter().getTableType();
        writeId = alterTableMessage.getWriteId();
        break;
      case EVENT_ALTER_PARTITION:
        AlterPartitionMessage msg = deserializer.getAlterPartitionMessage(context.dmd.getPayload());
        tableType = msg.getTableObj().getTableType();
        writeId = msg.getWriteId();
        break;
      default:
        break;
      }
      boolean isExternalTable = tableType != null
          && TableType.EXTERNAL_TABLE.equals(Enum.valueOf(TableType.class, tableType));
      return new Tuple(isExternalTable, writeId);
    } catch (Exception e) {
      LOG.error("failed to determine if the table associated with the event is external or not", e);
      throw new SemanticException(e);
    }
  }

  private static final class Tuple {
    private final boolean isExternalTable;
    private final long writeId;

    private Tuple(boolean isExternalTable, long writeId) {
      this.isExternalTable = isExternalTable;
      this.writeId = writeId;
    }
  }
}
