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
import org.apache.hive.hcatalog.api.repl.NoopReplicationTask;
import org.apache.hive.hcatalog.common.HCatConstants;

public class CreateDatabaseReplicationTask extends NoopReplicationTask {

  // "CREATE DATABASE" is specifically not replicated across, per design, since if a user
  // drops a database and recreates another with the same one, we want to distinguish
  // between the two. We will replicate the drop across, but after that, the goal is
  // that if a new db is created, a new replication definition should be created in
  // the replication implementer above this. Thus, we extend NoopReplicationTask and
  // the only additional thing we do is validate event type.

  public CreateDatabaseReplicationTask(HCatNotificationEvent event) {
    super(event);
    validateEventType(event, HCatConstants.HCAT_CREATE_DATABASE_EVENT);
  }
}
