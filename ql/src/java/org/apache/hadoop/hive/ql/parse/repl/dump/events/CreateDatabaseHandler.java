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
package org.apache.hadoop.hive.ql.parse.repl.dump.events;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.CreateDatabaseMessage;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;

class CreateDatabaseHandler extends AbstractEventHandler<CreateDatabaseMessage> {
  CreateDatabaseHandler(NotificationEvent event) {
    super(event);
  }

  @Override
  CreateDatabaseMessage eventMessage(String stringRepresentation) {
    return deserializer.getCreateDatabaseMessage(stringRepresentation);
  }

  @Override
  public void handle(Context withinContext) throws Exception {
    LOG.info("Processing#{} CREATE_DATABASE message : {}", fromEventId(), eventMessageAsJSON);
    Path metaDataPath = new Path(withinContext.eventRoot, EximUtil.METADATA_NAME);
    FileSystem fileSystem = metaDataPath.getFileSystem(withinContext.hiveConf);
    EximUtil.createDbExportDump(fileSystem, metaDataPath, eventMessage.getDatabaseObject(),
        withinContext.replicationSpec, withinContext.hiveConf);
    withinContext.createDmd(this).write();
  }

  @Override
  public DumpType dumpType() {
    return DumpType.EVENT_CREATE_DATABASE;
  }
}
