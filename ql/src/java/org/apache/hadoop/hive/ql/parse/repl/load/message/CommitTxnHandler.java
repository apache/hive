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
import org.apache.hadoop.hive.ql.exec.repl.util.AddDependencyToLeaves;
import org.apache.hadoop.hive.ql.exec.util.DAGTraversal;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.ReplTxnWork;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * CommitTxnHandler
 * Target(Load) side handler for commit transaction event.
 */
public class CommitTxnHandler extends AbstractMessageHandler {
  @Override
  public List<Task<?>> handle(Context context)
          throws SemanticException {
    if (!AcidUtils.isAcidEnabled(context.hiveConf)) {
      context.log.error("Cannot load transaction events as acid is not enabled");
      throw new SemanticException("Cannot load transaction events as acid is not enabled");
    }

    CommitTxnMessage msg = deserializer.getCommitTxnMessage(context.dmd.getPayload());
    int numEntry = (msg.getTables() == null ? 0 : msg.getTables().size());
    List<Task<?>> tasks = new ArrayList<>();
    String dbName = context.dbName;
    String tableNamePrev = null;
    String tblName = null;

    ReplTxnWork work = new ReplTxnWork(HiveUtils.getReplPolicy(context.dbName), context.dbName,
                                       null, msg.getTxnId(), ReplTxnWork.OperationType.REPL_COMMIT_TXN,
                                        context.eventOnlyReplicationSpec(), context.getDumpDirectory(),
                                        context.getMetricCollector());

    if (numEntry > 0) {
      context.log.debug("Commit txn handler for txnid " + msg.getTxnId() + " databases : " + msg.getDatabases() +
              " tables : " + msg.getTables() + " partitions : " + msg.getPartitions() + " files : " +
              msg.getFilesList() + " write ids : " + msg.getWriteIds());
    }

    for (int idx = 0; idx < numEntry; idx++) {
      String actualTblName = msg.getTables().get(idx);
      String actualDBName = msg.getDatabases().get(idx);
      String completeName = Table.getCompleteName(actualDBName, actualTblName);

      // One import task per table. Events for same table are kept together in one dump directory during dump and are
      // grouped together in commit txn message.
      if (tableNamePrev == null || !(completeName.equals(tableNamePrev))) {
        // The data location is created by source, so the location should be formed based on the table name in msg.
        Path location = HiveUtils.getDumpPath(new Path(context.location), actualDBName, actualTblName);
        tblName = actualTblName;
        // for warehouse level dump, use db name from write event
        dbName = (context.isDbNameEmpty() ? actualDBName : context.dbName);
        Context currentContext = new Context(context, dbName,
                context.getDumpDirectory(), context.getMetricCollector());
        currentContext.setLocation(location.toUri().toString());

        // Piggybacking in Import logic for now
        TableHandler tableHandler = new TableHandler();
        tasks.addAll((tableHandler.handle(currentContext)));
        readEntitySet.addAll(tableHandler.readEntities());
        writeEntitySet.addAll(tableHandler.writeEntities());
        getUpdatedMetadata().copyUpdatedMetadata(tableHandler.getUpdatedMetadata());
        tableNamePrev = completeName;
      }

      try {
        WriteEventInfo writeEventInfo = new WriteEventInfo(msg.getWriteIds().get(idx),
                dbName, tblName, msg.getFiles(idx));
        if (msg.getPartitions().get(idx) != null && !msg.getPartitions().get(idx).isEmpty()) {
          writeEventInfo.setPartition(msg.getPartitions().get(idx));
        }
        work.addWriteEventInfo(writeEventInfo);
      } catch (Exception e) {
        throw new SemanticException("Failed to extract write event info from commit txn message : " + e.getMessage());
      }
    }

    Task<ReplTxnWork> commitTxnTask = TaskFactory.get(work, context.hiveConf);

    // For warehouse level dump, don't update the metadata of database as we don't know this txn is for which database.
    // Anyways, if this event gets executed again, it is taken care of.
    if (!context.isDbNameEmpty()) {
      updatedMetadata.set(context.dmd.getEventTo().toString(), context.dbName, null, null);
    }
    context.log.debug("Added Commit txn task : {}", commitTxnTask.getId());
    if (tasks.isEmpty()) {
      //will be used for setting the last repl id.
      return Collections.singletonList(commitTxnTask);
    }
    DAGTraversal.traverse(tasks, new AddDependencyToLeaves(commitTxnTask));
    return tasks;
  }
}

