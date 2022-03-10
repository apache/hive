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

import org.apache.hadoop.hive.metastore.messaging.AllocWriteIdMessage;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ReplTxnWork;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * AllocWriteIdHandler
 * Target(Load) side handler for alloc write id event.
 */
public class AllocWriteIdHandler extends AbstractMessageHandler {
  @Override
  public List<Task<?>> handle(Context context)
      throws SemanticException {
    if (!AcidUtils.isAcidEnabled(context.hiveConf)) {
      context.log.error("Cannot load alloc write id event as acid is not enabled");
      throw new SemanticException("Cannot load alloc write id event as acid is not enabled");
    }

    AllocWriteIdMessage msg =
        deserializer.getAllocWriteIdMessage(context.dmd.getPayload());

    String dbName = (context.dbName != null && !context.dbName.isEmpty() ? context.dbName : msg.getDB());

    // We need table name for alloc write id and that is received from source.
    String tableName = msg.getTableName();

    // Repl policy should be created based on the table name in context.
    ReplTxnWork work = new ReplTxnWork(HiveUtils.getReplPolicy(context.dbName), dbName, tableName,
        ReplTxnWork.OperationType.REPL_ALLOC_WRITE_ID, msg.getTxnToWriteIdList(), context.eventOnlyReplicationSpec(),
            context.getDumpDirectory(), context.getMetricCollector());

    Task<?> allocWriteIdTask = TaskFactory.get(work, context.hiveConf);
    context.log.info("Added alloc write id task : {}", allocWriteIdTask.getId());
    updatedMetadata.set(context.dmd.getEventTo().toString(), dbName, tableName, null);
    return Collections.singletonList(allocWriteIdTask);
  }
}

