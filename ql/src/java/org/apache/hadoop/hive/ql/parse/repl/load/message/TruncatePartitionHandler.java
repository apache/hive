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

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionMessage;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.TruncateTableDesc;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TruncatePartitionHandler extends AbstractMessageHandler {
  @Override
  public List<Task<? extends Serializable>> handle(Context context) throws SemanticException {
    AlterPartitionMessage msg = deserializer.getAlterPartitionMessage(context.dmd.getPayload());
    String actualDbName = context.isDbNameEmpty() ? msg.getDB() : context.dbName;
    String actualTblName = context.isTableNameEmpty() ? msg.getTable() : context.tableName;

    Map<String, String> partSpec = new LinkedHashMap<>();
    try {
      org.apache.hadoop.hive.metastore.api.Table tblObj = msg.getTableObj();
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
            actualDbName + "." + actualTblName, partSpec,
            context.eventOnlyReplicationSpec());
    Task<DDLWork> truncatePtnTask = TaskFactory.get(
            new DDLWork(readEntitySet, writeEntitySet, truncateTableDesc),
            context.hiveConf);
    context.log.debug("Added truncate ptn task : {}:{}", truncatePtnTask.getId(),
        truncateTableDesc.getTableName());
    updatedMetadata.set(context.dmd.getEventTo().toString(), actualDbName, actualTblName, partSpec);
    return Collections.singletonList(truncatePtnTask);
  }
}
