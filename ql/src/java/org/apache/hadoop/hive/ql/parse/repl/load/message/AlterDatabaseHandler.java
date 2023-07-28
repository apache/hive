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

import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.messaging.AlterDatabaseMessage;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AlterDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.apache.hadoop.hive.ql.plan.PrincipalDesc;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * AlterDatabaseHandler.
 * Handler at target warehouse for the EVENT_ALTER_DATABASE type of messages
 */
public class AlterDatabaseHandler extends AbstractMessageHandler {
  @Override
  public List<Task<? extends Serializable>> handle(Context context)
      throws SemanticException {

    if (!context.isTableNameEmpty()) {
      throw new SemanticException(
              "Alter Database are not supported for table-level replication");
    }

    AlterDatabaseMessage msg = deserializer.getAlterDatabaseMessage(context.dmd.getPayload());
    String actualDbName = context.isDbNameEmpty() ? msg.getDB() : context.dbName;

    try {
      Database oldDb = msg.getDbObjBefore();
      Database newDb = msg.getDbObjAfter();
      AlterDatabaseDesc alterDbDesc;

      if ((oldDb.getOwnerType() == newDb.getOwnerType())
            && oldDb.getOwnerName().equalsIgnoreCase(newDb.getOwnerName())) {
        // If owner information is unchanged, then DB properties would've changed
        Map<String, String> newDbProps = new HashMap<>();
        Map<String, String> dbProps = newDb.getParameters();

        for (Map.Entry<String, String> entry : dbProps.entrySet()) {
          String key = entry.getKey();
          // Ignore the keys which are local to source warehouse
          if (key.startsWith(Utils.BOOTSTRAP_DUMP_STATE_KEY_PREFIX)
                  || key.equals(ReplicationSpec.KEY.CURR_STATE_ID.toString())
                  || key.equals(ReplUtils.REPL_CHECKPOINT_KEY)
                  || key.equals(ReplChangeManager.SOURCE_OF_REPLICATION)) {
            continue;
          }
          newDbProps.put(key, entry.getValue());
        }
        alterDbDesc = new AlterDatabaseDesc(actualDbName,
                newDbProps, context.eventOnlyReplicationSpec());
      } else {
        alterDbDesc = new AlterDatabaseDesc(actualDbName,
                new PrincipalDesc(newDb.getOwnerName(), newDb.getOwnerType()),
                context.eventOnlyReplicationSpec());
      }

      Task<DDLWork> alterDbTask = TaskFactory.get(
          new DDLWork(readEntitySet, writeEntitySet, alterDbDesc), context.hiveConf);
      context.log.debug("Added alter database task : {}:{}",
              alterDbTask.getId(), actualDbName);

      // Only database object is updated
      updatedMetadata.set(context.dmd.getEventTo().toString(), actualDbName,
              null, null);
      return Collections.singletonList(alterDbTask);
    } catch (Exception e) {
      throw (e instanceof SemanticException)
          ? (SemanticException) e
          : new SemanticException("Error reading message members", e);
    }
  }
}
