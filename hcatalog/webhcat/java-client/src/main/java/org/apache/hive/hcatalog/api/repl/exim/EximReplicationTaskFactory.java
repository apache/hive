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

import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatNotificationEvent;
import org.apache.hive.hcatalog.api.repl.NoopReplicationTask;
import org.apache.hive.hcatalog.api.repl.ReplicationTask;
import org.apache.hive.hcatalog.common.HCatConstants;

/**
 * EXIMReplicationTaskFactory is an export-import based ReplicationTask.Factory.
 *
 * It's primary mode of enabling replication is by translating each event it gets
 * from the notification subsystem into hive commands that essentially export data
 * to be copied over and imported on the other end.
 *
 * The Commands that Tasks return here are expected to be hive commands.
 */
public class EximReplicationTaskFactory implements ReplicationTask.Factory {
  public ReplicationTask create(HCatClient client, HCatNotificationEvent event){
    // TODO : Java 1.7+ support using String with switches, but IDEs don't all seem to know that.
    // If casing is fine for now. But we should eventually remove this. Also, I didn't want to
    // create another enum just for this.
    if (event.getEventType().equals(HCatConstants.HCAT_CREATE_DATABASE_EVENT)) {
      return new CreateDatabaseReplicationTask(event);
    } else if (event.getEventType().equals(HCatConstants.HCAT_DROP_DATABASE_EVENT)) {
      return new DropDatabaseReplicationTask(event);
    } else if (event.getEventType().equals(HCatConstants.HCAT_CREATE_TABLE_EVENT)) {
      return new CreateTableReplicationTask(event);
    } else if (event.getEventType().equals(HCatConstants.HCAT_DROP_TABLE_EVENT)) {
      return new DropTableReplicationTask(event);
    } else if (event.getEventType().equals(HCatConstants.HCAT_ADD_PARTITION_EVENT)) {
      return new AddPartitionReplicationTask(event);
    } else if (event.getEventType().equals(HCatConstants.HCAT_DROP_PARTITION_EVENT)) {
      return new DropPartitionReplicationTask(event);
    } else if (event.getEventType().equals(HCatConstants.HCAT_ALTER_TABLE_EVENT)) {
      return new AlterTableReplicationTask(event);
    } else if (event.getEventType().equals(HCatConstants.HCAT_ALTER_PARTITION_EVENT)) {
      return new AlterPartitionReplicationTask(event);
    } else if (event.getEventType().equals(HCatConstants.HCAT_INSERT_EVENT)) {
      return new InsertReplicationTask(event);
    } else {
      throw new IllegalStateException("Unrecognized Event type, no replication task available");
    }
  }
}
