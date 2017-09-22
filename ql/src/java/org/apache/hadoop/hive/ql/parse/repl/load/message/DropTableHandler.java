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

import org.apache.hadoop.hive.metastore.messaging.DropTableMessage;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.DropTableDesc;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public class DropTableHandler extends AbstractMessageHandler {
  @Override
  public List<Task<? extends Serializable>> handle(Context context)
      throws SemanticException {
    DropTableMessage msg = deserializer.getDropTableMessage(context.dmd.getPayload());
    String actualDbName = context.isDbNameEmpty() ? msg.getDB() : context.dbName;
    String actualTblName = context.isTableNameEmpty() ? msg.getTable() : context.tableName;
    DropTableDesc dropTableDesc = new DropTableDesc(
        actualDbName + "." + actualTblName,
        null, true, true, context.eventOnlyReplicationSpec(), false
    );
    Task<DDLWork> dropTableTask = TaskFactory.get(
        new DDLWork(readEntitySet, writeEntitySet, dropTableDesc),
        context.hiveConf
    );
    context.log.debug(
        "Added drop tbl task : {}:{}", dropTableTask.getId(), dropTableDesc.getTableName()
    );
    updatedMetadata.set(context.dmd.getEventTo().toString(), actualDbName, null, null);
    return Collections.singletonList(dropTableTask);
  }
}
