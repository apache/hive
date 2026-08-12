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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.api.repl.exim;

import org.apache.hive.hcatalog.api.HCatNotificationEvent;
import org.apache.hive.hcatalog.api.repl.Command;
import org.apache.hive.hcatalog.api.repl.ReplicationTask;
import org.apache.hive.hcatalog.api.repl.ReplicationUtils;
import org.apache.hive.hcatalog.api.repl.commands.ExportCommand;
import org.apache.hive.hcatalog.api.repl.commands.ImportCommand;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.messaging.InsertMessage;

import java.util.Collections;
import java.util.Map;

public class InsertReplicationTask extends ReplicationTask {
  private final InsertMessage insertMessage;

  public InsertReplicationTask(HCatNotificationEvent event) {
    super(event);
    validateEventType(event, HCatConstants.HCAT_INSERT_EVENT);
    insertMessage = messageFactory.getDeserializer().getInsertMessage(event.getMessage());
  }


  public boolean needsStagingDirs(){
    // we need staging directories as long as a single partition needed addition
    return true;
  }

  @Override
  public Iterable<? extends Command> getSrcWhCommands() {
    verifyActionable();

    final String dbName = insertMessage.getDB();
    final String tableName = insertMessage.getTable();
    final Map<String,String> ptnDesc = insertMessage.getPartitionKeyValues();
    // Note : ptnDesc can be null or empty for non-ptn table

    return Collections.singletonList(new ExportCommand(
        dbName,
        tableName,
        ptnDesc,
        srcStagingDirProvider.getStagingDirectory(
            ReplicationUtils.getUniqueKey(
                getEvent().getEventId(),
                dbName,
                tableName,
                ptnDesc)
        ),
        false,
        event.getEventId()
    ));

  }

  public Iterable<? extends Command> getDstWhCommands() {
    verifyActionable();

    final String dbName = insertMessage.getDB();
    final String tableName = insertMessage.getTable();
    final Map<String,String> ptnDesc = insertMessage.getPartitionKeyValues();
    // Note : ptnDesc can be null or empty for non-ptn table

    return Collections.singletonList(new ImportCommand(
        ReplicationUtils.mapIfMapAvailable(dbName, dbNameMapping),
        ReplicationUtils.mapIfMapAvailable(tableName, tableNameMapping),
        ptnDesc,
        dstStagingDirProvider.getStagingDirectory(
            ReplicationUtils.getUniqueKey(
                getEvent().getEventId(),
                dbName, // Note - important to retain the same key as the export
                tableName,
                ptnDesc)
        ),
        false,
        event.getEventId()
    ));

  }

}
