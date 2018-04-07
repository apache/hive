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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * AllocWriteIdHandler
 * Target(Load) side handler for alloc write id event.
 */
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

    String dbName = (context.dbName != null && !context.dbName.isEmpty() ? context.dbName : msg.getDbName());

    // The context table name can be null if repl load is done on a full db.
    // But we need table name for alloc write id and that is received from source.
    String tableName = (context.tableName != null && !context.tableName.isEmpty() ? context.tableName : msg
            .getTableName());

    Table tbl;
    try {
      tbl = context.db.getTable(dbName, tableName);
    } catch (InvalidTableException e) {
      // return a empty list if table does not exist.
      return new ArrayList<>();
    } catch (HiveException e) {
      throw new SemanticException("Cannot load alloc write id event as get table failed with error: " + e.getMessage());
    }

    Map<String, String> params = tbl.getParameters();
    if (params != null && (params.containsKey(ReplicationSpec.KEY.CURR_STATE_ID.toString()))) {
      String replLastId = params.get(ReplicationSpec.KEY.CURR_STATE_ID.toString());
      if (Long.parseLong(replLastId) >= context.dmd.getEventTo()) {
        // if the event is already replayed, then no need to replay it again.
        return new ArrayList<>();
      }
    }

    //context table name is passed to ReplTxnWork , as the repl policy will be created based
    //on this table name. ReplPolicy is used to extract the target transaction id based on source
    //transaction id.
    ReplTxnWork work = new ReplTxnWork(dbName, context.tableName, ReplTxnWork.OperationType.REPL_ALLOC_WRITE_ID,
            msg.getTxnToWriteIdList());
    work.setTableName(tableName);

    Task<? extends Serializable> allocWriteIdTask = TaskFactory.get(work, context.hiveConf);
    context.log.info("Added alloc write id task : {}", allocWriteIdTask.getId());
    updatedMetadata.set(context.dmd.getEventTo().toString(), dbName, tableName, null);
    return Collections.singletonList(allocWriteIdTask);
  }
}

