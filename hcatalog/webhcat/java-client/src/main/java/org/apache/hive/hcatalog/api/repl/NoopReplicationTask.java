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

package org.apache.hive.hcatalog.api.repl;

import org.apache.hive.hcatalog.api.HCatNotificationEvent;
import org.apache.hive.hcatalog.api.repl.commands.NoopCommand;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is there to help testing, and to help initial development
 * and will be the default Replication Task for under-development replication
 * tasks to override.
 *
 * This is not intended to be a permanent class, and will likely move to the test
 * package after initial implementation.
 */

public class NoopReplicationTask extends ReplicationTask {

  List<Command> noopReturn = null;

  public NoopReplicationTask(HCatNotificationEvent event) {
    super(event);
    noopReturn = new ArrayList<Command>();
    noopReturn.add(new NoopCommand(event.getEventId()));
  }

  @Override
  public boolean needsStagingDirs() {
    return false;
  }

  @Override
  public boolean isActionable(){
    return true;
  }

  /**
   * Returns a list of commands to send to a hive driver on the source warehouse
   * @return a list of commands to send to a hive driver on the source warehouse
   */
  @Override
  public Iterable<? extends Command> getSrcWhCommands() {
    verifyActionable();
    return noopReturn;
  }

  /**
   * Returns a list of commands to send to a hive driver on the dest warehouse
   * @return a list of commands to send to a hive driver on the dest warehouse
   */
  @Override
  public Iterable<? extends Command> getDstWhCommands() {
    verifyActionable();
    return noopReturn;
  }

}

