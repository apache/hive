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
import org.apache.hadoop.hive.ql.exec.ReplTxnWork;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public class AllocWriteIdHandler extends AbstractMessageHandler {
  @Override
  public List<Task<? extends Serializable>> handle(Context context)
      throws SemanticException {
    if (!AcidUtils.isAcidEnabled(context.hiveConf)) {
      context.log.error("Cannot load alloc write id event as acid is not enabled");
      throw new SemanticException("Cannot load alloc write id event as acid is not enabled");
    }

    AllocWriteIdMessage msg =
        deserializer.getAllocWriteIdMessage(context.dmd.getPayload());

    //context table name is passed to ReplTxnWork , as the repl policy will be created based
    //on this table name. ReplPolicy is used to extract the target transaction id based on source
    //transaction id.
    ReplTxnWork work = new ReplTxnWork(context.dbName, context.tableName, msg.getTxnIds(),
            ReplTxnWork.OperationType.REPL_ALLOC_WRITE_ID);

    // The context table name can be null if repl load is done on a full db.
    // But we need table name for alloc write id and that is received from source.
    String tableName = (context.tableName != null && !context.tableName.isEmpty() ? context.tableName : msg
            .getTableName());
    work.setTableName(tableName);

    Task<? extends Serializable> allocWriteIdTask =
            TaskFactory.get(work, context.hiveConf);
    context.log.info(
        "Added alloc write id task : {}", allocWriteIdTask.getId());
    updatedMetadata.set(context.dmd.getEventTo().toString(), context.dbName, tableName, null);
    return Collections.singletonList(allocWriteIdTask);
  }
}

