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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.hive.hcatalog.api.HCatNotificationEvent;
import org.apache.hive.hcatalog.api.repl.Command;
import org.apache.hive.hcatalog.api.repl.ReplicationTask;
import org.apache.hive.hcatalog.api.repl.ReplicationUtils;
import org.apache.hive.hcatalog.api.repl.commands.ExportCommand;
import org.apache.hive.hcatalog.api.repl.commands.ImportCommand;
import org.apache.hive.hcatalog.api.repl.commands.NoopCommand;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.messaging.AddPartitionMessage;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;

public class AddPartitionReplicationTask extends ReplicationTask {

  AddPartitionMessage addPartitionMessage = null;

  public AddPartitionReplicationTask(HCatNotificationEvent event) {
    super(event);
    validateEventType(event,HCatConstants.HCAT_ADD_PARTITION_EVENT);
    addPartitionMessage = messageFactory.getDeserializer().getAddPartitionMessage(event.getMessage());
  }

  public boolean needsStagingDirs(){
    // we need staging directories as long as a single partition needed addition
    return (!addPartitionMessage.getPartitions().isEmpty());
  }


  public Iterable<? extends Command> getSrcWhCommands() {
    verifyActionable();
    if (addPartitionMessage.getPartitions().isEmpty()){
      return Collections.singletonList(new NoopCommand(event.getEventId()));
    }

    return Iterables.transform(addPartitionMessage.getPartitions(), new Function<Map<String,String>, Command>(){
      @Override
      public Command apply(@Nullable Map<String, String> ptnDesc) {
        return new ExportCommand(
          addPartitionMessage.getDB(),
          addPartitionMessage.getTable(),
          ptnDesc,
          srcStagingDirProvider.getStagingDirectory(
            ReplicationUtils.getUniqueKey(
                getEvent().getEventId(),
                addPartitionMessage.getDB(),
                addPartitionMessage.getTable(),
                ptnDesc)
          ),
          false,
          event.getEventId()
        );
      }
    });

  }

  public Iterable<? extends Command> getDstWhCommands() {
    verifyActionable();
    if (addPartitionMessage.getPartitions().isEmpty()){
      return Collections.singletonList(new NoopCommand(event.getEventId()));
    }

    final String dstDbName = ReplicationUtils.mapIfMapAvailable(addPartitionMessage.getDB(), dbNameMapping);
    final String dstTableName = ReplicationUtils.mapIfMapAvailable(addPartitionMessage.getTable(), tableNameMapping);

    return Iterables.transform(addPartitionMessage.getPartitions(), new Function<Map<String, String>, Command>() {
      @Override
      public Command apply(@Nullable Map<String, String> ptnDesc) {
        return new ImportCommand(
            dstDbName,
            dstTableName,
            ptnDesc,
            dstStagingDirProvider.getStagingDirectory(
                ReplicationUtils.getUniqueKey(
                    getEvent().getEventId(),
                    addPartitionMessage.getDB(), // Note - important to retain the same key as the export
                    addPartitionMessage.getTable(),
                    ptnDesc)
            ),
            false,
            event.getEventId()
        );
      }
    });
  }
}
