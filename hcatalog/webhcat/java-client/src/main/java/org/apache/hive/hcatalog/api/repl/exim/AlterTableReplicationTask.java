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
import org.apache.hive.hcatalog.messaging.AlterTableMessage;

import java.util.Collections;

public class AlterTableReplicationTask extends ReplicationTask {
  private final AlterTableMessage alterTableMessage;

  public AlterTableReplicationTask(HCatNotificationEvent event) {
    super(event);
    validateEventType(event, HCatConstants.HCAT_ALTER_TABLE_EVENT);
    alterTableMessage = messageFactory.getDeserializer().getAlterTableMessage(event.getMessage());
  }

  public boolean needsStagingDirs(){
    return true;
  }

  public Iterable<? extends Command> getSrcWhCommands() {
    verifyActionable();
    final String dbName = alterTableMessage.getDB();
    final String tableName = alterTableMessage.getTable();
    return Collections.singletonList(new ExportCommand(
        dbName,
        tableName,
        null,
        srcStagingDirProvider.getStagingDirectory(
            ReplicationUtils.getUniqueKey(
                getEvent().getEventId(),
                dbName,
                tableName,
                null)
        ),
        true,
        event.getEventId()
    ));
  }

  public Iterable<? extends Command> getDstWhCommands() {
    verifyActionable();
    final String dbName = alterTableMessage.getDB();
    final String tableName = alterTableMessage.getTable();
    return Collections.singletonList(new ImportCommand(
        ReplicationUtils.mapIfMapAvailable(dbName, dbNameMapping),
        ReplicationUtils.mapIfMapAvailable(tableName, tableNameMapping),
        null,
        dstStagingDirProvider.getStagingDirectory(
            ReplicationUtils.getUniqueKey(
                getEvent().getEventId(),
                dbName, // Note - important to retain the same key as the export
                tableName,
                null)
        ),
        true,
        event.getEventId()
    ));
  }
}
