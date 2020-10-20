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

import org.apache.hadoop.hive.metastore.messaging.OpenTxnMessage;
import org.apache.hadoop.hive.ql.plan.ReplTxnWork;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * OpenTxnHandler
 * Target(Load) side handler for open transaction event.
 */
public class OpenTxnHandler extends AbstractMessageHandler {
  @Override
  public List<Task<?>> handle(Context context)
      throws SemanticException {
    if (!AcidUtils.isAcidEnabled(context.hiveConf)) {
      context.log.error("Cannot load transaction events as acid is not enabled");
      throw new SemanticException("Cannot load transaction events as acid is not enabled");
    }
    OpenTxnMessage msg = deserializer.getOpenTxnMessage(context.dmd.getPayload());

    Task<ReplTxnWork> openTxnTask = TaskFactory.get(
        new ReplTxnWork(HiveUtils.getReplPolicy(context.dbName), context.dbName, null,
                msg.getTxnIds(), ReplTxnWork.OperationType.REPL_OPEN_TXN, context.eventOnlyReplicationSpec(),
                context.getDumpDirectory(), context.getMetricCollector()),
        context.hiveConf
    );

    // For warehouse level dump, don't update the metadata of database as we don't know this txn is for which database.
    // Anyways, if this event gets executed again, it is taken care of.
    if (!context.isDbNameEmpty()) {
      updatedMetadata.set(context.dmd.getEventTo().toString(), context.dbName, null, null);
    }
    context.log.debug("Added Open txn task : {}", openTxnTask.getId());
    return Collections.singletonList(openTxnTask);
  }
}
