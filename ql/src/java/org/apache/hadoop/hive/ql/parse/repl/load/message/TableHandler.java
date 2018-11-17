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

import org.apache.hadoop.hive.metastore.messaging.AlterPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterTableMessage;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.ImportSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ReplTxnWork;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.hive.ql.parse.repl.DumpType.EVENT_ALTER_PARTITION;
import static org.apache.hadoop.hive.ql.parse.repl.DumpType.EVENT_ALTER_TABLE;

public class TableHandler extends AbstractMessageHandler {
  @Override
  public List<Task<? extends Serializable>> handle(Context context) throws SemanticException {
    try {
      List<Task<? extends Serializable>> importTasks = new ArrayList<>();
      long writeId = 0;

      if (context.dmd.getDumpType().equals(EVENT_ALTER_TABLE)) {
        AlterTableMessage message = deserializer.getAlterTableMessage(context.dmd.getPayload());
        writeId = message.getWriteId();
      } else if (context.dmd.getDumpType().equals(EVENT_ALTER_PARTITION)) {
        AlterPartitionMessage message = deserializer.getAlterPartitionMessage(context.dmd.getPayload());
        writeId = message.getWriteId();
      }

      context.nestedContext.setConf(context.hiveConf);
      EximUtil.SemanticAnalyzerWrapperContext x =
          new EximUtil.SemanticAnalyzerWrapperContext(
              context.hiveConf, context.db, readEntitySet, writeEntitySet, importTasks, context.log,
              context.nestedContext);
      x.setEventType(context.dmd.getDumpType());

      // REPL LOAD is not partition level. It is always DB or table level. So, passing null for partition specs.
      // Also, REPL LOAD doesn't support external table and hence no location set as well.
      ImportSemanticAnalyzer.prepareImport(false, false, false, false,
          (context.precursor != null), null, context.tableName, context.dbName,
          null, context.location, x, updatedMetadata, context.getTxnMgr(), writeId);

      Task<? extends Serializable> openTxnTask = x.getOpenTxnTask();
      if (openTxnTask != null && !importTasks.isEmpty()) {
        for (Task<? extends Serializable> t : importTasks) {
          openTxnTask.addDependentTask(t);
        }
        importTasks.add(openTxnTask);
      }
      return importTasks;
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }
}
