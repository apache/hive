/**
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
import org.apache.hive.hcatalog.api.repl.commands.DropTableCommand;
import org.apache.hive.hcatalog.api.repl.commands.NoopCommand;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.messaging.DropTableMessage;

import java.util.Collections;

public class DropTableReplicationTask extends ReplicationTask {
  private DropTableMessage dropTableMessage = null;

  public DropTableReplicationTask(HCatNotificationEvent event) {
    super(event);
    validateEventType(event, HCatConstants.HCAT_DROP_TABLE_EVENT);
    dropTableMessage = messageFactory.getDeserializer().getDropTableMessage(event.getMessage());
  }

  public boolean needsStagingDirs(){
    return false;
  }

  public Iterable<? extends Command> getSrcWhCommands() {
    verifyActionable();
    return Collections.singletonList(new NoopCommand(event.getEventId()));
  }

  public Iterable<? extends Command> getDstWhCommands() {
    verifyActionable();
    final String dstDbName = ReplicationUtils.mapIfMapAvailable(dropTableMessage.getDB(), dbNameMapping);
    final String dstTableName = ReplicationUtils.mapIfMapAvailable(dropTableMessage.getTable(), tableNameMapping);
    return Collections.singletonList(new DropTableCommand(dstDbName, dstTableName, true, event.getEventId()));
  }
}
