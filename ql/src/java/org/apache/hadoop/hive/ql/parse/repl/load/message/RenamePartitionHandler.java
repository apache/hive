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
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionMessage;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.table.partition.rename.AlterTableRenamePartitionDesc;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class RenamePartitionHandler extends AbstractMessageHandler {
  @Override
  public List<Task<?>> handle(Context context)
      throws SemanticException {

    AlterPartitionMessage msg = deserializer.getAlterPartitionMessage(context.dmd.getPayload());
    String actualDbName = context.isDbNameEmpty() ? msg.getDB() : context.dbName;
    String actualTblName = msg.getTable();

    Map<String, String> newPartSpec = new LinkedHashMap<>();
    Map<String, String> oldPartSpec = new LinkedHashMap<>();
    TableName tableName = TableName.fromString(actualTblName, null, actualDbName);
    Table tableObj;
    ReplicationSpec replicationSpec = context.eventOnlyReplicationSpec();
    try {
      Iterator<String> beforeIterator = msg.getPtnObjBefore().getValuesIterator();
      Iterator<String> afterIterator = msg.getPtnObjAfter().getValuesIterator();
      tableObj = msg.getTableObj();
      for (FieldSchema fs : tableObj.getPartitionKeys()) {
        oldPartSpec.put(fs.getName(), beforeIterator.next());
        newPartSpec.put(fs.getName(), afterIterator.next());
      }

      AlterTableRenamePartitionDesc renamePtnDesc = new AlterTableRenamePartitionDesc(
              tableName, oldPartSpec, newPartSpec, replicationSpec, null);
      renamePtnDesc.setWriteId(msg.getWriteId());
      Task<DDLWork> renamePtnTask = TaskFactory.get(
          new DDLWork(readEntitySet, writeEntitySet, renamePtnDesc, true,
                  context.getDumpDirectory(), context.getMetricCollector()), context.hiveConf);
      context.log.debug("Added rename ptn task : {}:{}->{}",
                        renamePtnTask.getId(), oldPartSpec, newPartSpec);
      updatedMetadata.set(context.dmd.getEventTo().toString(), actualDbName, actualTblName, newPartSpec);
      return ReplUtils.addChildTask(renamePtnTask);
    } catch (Exception e) {
      throw (e instanceof SemanticException)
              ? (SemanticException) e
              : new SemanticException("Error reading message members", e);
    }
  }
}
