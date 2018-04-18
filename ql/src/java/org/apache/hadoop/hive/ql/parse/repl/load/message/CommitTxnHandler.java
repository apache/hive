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
import org.apache.hadoop.hive.metastore.api.WriteEventInfo;
import org.apache.hadoop.hive.metastore.messaging.CommitTxnMessage;
import org.apache.hadoop.hive.ql.plan.ReplTxnWork;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * CommitTxnHandler
 * Target(Load) side handler for commit transaction event.
 */
public class CommitTxnHandler extends AbstractMessageHandler {
  @Override
  public List<Task<? extends Serializable>> handle(Context context)
          throws SemanticException {
    if (!AcidUtils.isAcidEnabled(context.hiveConf)) {
      context.log.error("Cannot load transaction events as acid is not enabled");
      throw new SemanticException("Cannot load transaction events as acid is not enabled");
    }

    CommitTxnMessage msg = deserializer.getCommitTxnMessage(context.dmd.getPayload());
    int numEntry = (msg.getTables() == null ? 0 : msg.getTables().size());
    List<Task<? extends Serializable>> tasks = new ArrayList<>();
    String dbName = (context.dbName == null || context.isDbNameEmpty() ? msg.getDB() : context.dbName);
    String tableNamePrev = null;
    String tblName = null;

    ReplTxnWork work = new ReplTxnWork(HiveUtils.getReplPolicy(context.dbName, context.tableName), context.dbName,
      context.tableName, msg.getTxnId(), ReplTxnWork.OperationType.REPL_COMMIT_TXN, context.eventOnlyReplicationSpec());

    if (numEntry > 0) {
      context.log.debug("Commit txn handler for txnid " + msg.getTxnId() + " databases : " + msg.getDatabases() +
              " tables : " + msg.getTables() + " partitions : " + msg.getPartitions() + " files : " +
              msg.getFilesList() + " write ids : " + msg.getWriteIds());
    }

    for (int idx = 0; idx < numEntry; idx++) {
      String actualTblName = msg.getTables().get(idx);
      //one import task per table
      if (tableNamePrev == null || !actualTblName.equals(tableNamePrev)) {
        // The data location is created by source, so the location should be formed based on the table name in msg.
        String location = context.location + Path.SEPARATOR +
                HiveUtils.getDumpPath(msg.getDatabases().get(idx), actualTblName);
        tblName = context.isTableNameEmpty() ? actualTblName : context.tableName;
        Context currentContext = new Context(dbName, tblName, location, context.precursor,
                context.dmd, context.hiveConf, context.db, context.nestedContext, context.log);

        // Piggybacking in Import logic for now
        TableHandler tableHandler = new TableHandler();
        tasks.addAll((tableHandler.handle(currentContext)));
        readEntitySet.addAll(tableHandler.readEntities());
        writeEntitySet.addAll(tableHandler.writeEntities());
        getUpdatedMetadata().copyUpdatedMetadata(tableHandler.getUpdatedMetadata());
        tableNamePrev = actualTblName;
      }

      try {
        WriteEventInfo writeEventInfo = new WriteEventInfo(msg.getWriteIds().get(idx),
                dbName, tblName, msg.getPartitions().get(idx));
        writeEventInfo.setFiles(msg.getFiles(idx));
        work.addWriteEventInfo(writeEventInfo);
      } catch (Exception e) {
        throw new SemanticException("Failed to extract write event info from commit txn message : " + e.getMessage());
      }
    }

    Task<ReplTxnWork> commitTxnTask = TaskFactory.get(work, context.hiveConf);
    updatedMetadata.set(context.dmd.getEventTo().toString(), context.dbName, context.tableName, null);
    context.log.debug("Added Commit txn task : {}", commitTxnTask.getId());
    tasks.add(commitTxnTask);
    return tasks;
  }
}

