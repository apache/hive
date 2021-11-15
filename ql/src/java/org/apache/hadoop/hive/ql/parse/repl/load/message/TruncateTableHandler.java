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

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.messaging.AlterTableMessage;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.table.misc.truncate.TruncateTableDesc;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.List;

public class TruncateTableHandler extends AbstractMessageHandler {
  @Override
  public List<Task<?>> handle(Context context) throws SemanticException {
    AlterTableMessage msg = deserializer.getAlterTableMessage(context.dmd.getPayload());
    final TableName tName = TableName.fromString(msg.getTable(), null,
        context.isDbNameEmpty() ? msg.getDB() : context.dbName);

    TruncateTableDesc truncateTableDesc = new TruncateTableDesc(tName, null, context.eventOnlyReplicationSpec());
    truncateTableDesc.setWriteId(msg.getWriteId());
    Task<DDLWork> truncateTableTask = TaskFactory.get(new DDLWork(readEntitySet, writeEntitySet,
                truncateTableDesc, true, context.getDumpDirectory(),
                context.getMetricCollector()), context.hiveConf);

    context.log.debug("Added truncate tbl task : {}:{}:{}", truncateTableTask.getId(),
        truncateTableDesc.getTableName(), truncateTableDesc.getWriteId());
    updatedMetadata.set(context.dmd.getEventTo().toString(), tName.getDb(), tName.getTable(), null);

    try {
      return ReplUtils.addChildTask(truncateTableTask);
    } catch (Exception e) {
      throw new SemanticException(e.getMessage());
    }
  }
}
