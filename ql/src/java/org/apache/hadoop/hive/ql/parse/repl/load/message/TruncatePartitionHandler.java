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
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionMessage;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.table.misc.truncate.TruncateTableDesc;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TruncatePartitionHandler extends AbstractMessageHandler {
  @Override
  public List<Task<?>> handle(Context context) throws SemanticException {
    AlterPartitionMessage msg = deserializer.getAlterPartitionMessage(context.dmd.getPayload());
    final TableName tName = TableName.fromString(msg.getTable(), null,
        context.isDbNameEmpty() ? msg.getDB() : context.dbName);

    Map<String, String> partSpec = new LinkedHashMap<>();
    org.apache.hadoop.hive.metastore.api.Table tblObj;
    try {
      tblObj = msg.getTableObj();
      Iterator<String> afterIterator = msg.getPtnObjAfter().getValuesIterator();
      for (FieldSchema fs : tblObj.getPartitionKeys()) {
        partSpec.put(fs.getName(), afterIterator.next());
      }
    } catch (Exception e) {
      if (!(e instanceof SemanticException)) {
        throw new SemanticException("Error reading message members", e);
      } else {
        throw (SemanticException) e;
      }
    }

    TruncateTableDesc truncateTableDesc = new TruncateTableDesc(
            tName, partSpec,
            context.eventOnlyReplicationSpec());
    truncateTableDesc.setWriteId(msg.getWriteId());
    Task<DDLWork> truncatePtnTask = TaskFactory.get(
        new DDLWork(readEntitySet, writeEntitySet, truncateTableDesc, true,
                context.getDumpDirectory(), context.getMetricCollector()), context.hiveConf);
    context.log.debug("Added truncate ptn task : {}:{}:{}", truncatePtnTask.getId(),
        truncateTableDesc.getTableName(), truncateTableDesc.getWriteId());
    updatedMetadata.set(context.dmd.getEventTo().toString(), tName.getDb(), tName.getTable(), partSpec);

    try {
      return ReplUtils.addChildTask(truncatePtnTask);
    } catch (Exception e) {
      throw new SemanticException(e.getMessage());
    }
  }
}
