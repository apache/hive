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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.messaging.AddPrimaryKeyMessage;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;

public class AddPrimaryKeyHandler extends AbstractMessageHandler {
  @Override
  public List<Task<? extends Serializable>> handle(Context context)
      throws SemanticException {
    AddPrimaryKeyMessage msg = deserializer.getAddPrimaryKeyMessage(context.dmd.getPayload());

    List<SQLPrimaryKey> pks = null;
    try {
      pks = msg.getPrimaryKeys();
    } catch (Exception e) {
      if (!(e instanceof SemanticException)){
        throw new SemanticException("Error reading message members", e);
      } else {
        throw (SemanticException)e;
      }
    }

    List<Task<? extends Serializable>> tasks = new ArrayList<Task<? extends Serializable>>();
    if (pks.isEmpty()) {
      return tasks;
    }

    String actualDbName = context.isDbNameEmpty() ? pks.get(0).getTable_db() : context.dbName;
    String actualTblName = context.isTableNameEmpty() ? pks.get(0).getTable_name() : context.tableName;

    for (SQLPrimaryKey pk : pks) {
      pk.setTable_db(actualDbName);
      pk.setTable_name(actualTblName);
    }

    AlterTableDesc addConstraintsDesc = new AlterTableDesc(actualDbName + "." + actualTblName, pks, new ArrayList<SQLForeignKey>(),
        new ArrayList<SQLUniqueConstraint>(), context.eventOnlyReplicationSpec());
    Task<DDLWork> addConstraintsTask = TaskFactory.get(new DDLWork(readEntitySet, writeEntitySet, addConstraintsDesc), context.hiveConf);
    tasks.add(addConstraintsTask);
    context.log.debug("Added add constrains task : {}:{}", addConstraintsTask.getId(), actualTblName);
    updatedMetadata.set(context.dmd.getEventTo().toString(), actualDbName, actualTblName, null);
    return Collections.singletonList(addConstraintsTask);    
  }
}
