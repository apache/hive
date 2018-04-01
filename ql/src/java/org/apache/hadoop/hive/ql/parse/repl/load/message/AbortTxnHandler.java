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

import org.apache.hadoop.hive.metastore.messaging.AbortTxnMessage;
import org.apache.hadoop.hive.ql.exec.ReplTxnWork;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * AbortTxnHandler
 * Target(Load) side handler for abort transaction event.
 */
public class AbortTxnHandler extends AbstractMessageHandler {
  @Override
  public List<Task<? extends Serializable>> handle(Context context)
      throws SemanticException {
    if (!AcidUtils.isAcidEnabled(context.hiveConf)) {
      context.log.error("Cannot load transaction events as acid is not enabled");
      throw new SemanticException("Cannot load transaction events as acid is not enabled");
    }

    AbortTxnMessage msg = deserializer.getAbortTxnMessage(context.dmd.getPayload());

    Task<ReplTxnWork> abortTxnTask = TaskFactory.get(
        new ReplTxnWork(context.dbName, context.tableName, msg.getTxnId(), ReplTxnWork.OperationType.REPL_ABORT_TXN),
        context.hiveConf
    );
    updatedMetadata.set(context.dmd.getEventTo().toString(), context.dbName, context.tableName, null);
    context.log.debug("Added Abort txn task : {}", abortTxnTask.getId());
    return Collections.singletonList(abortTxnTask);
  }
}
