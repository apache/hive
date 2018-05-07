/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.hadoop.hive.metastore.messaging.CreateFunctionMessage;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;
import org.apache.hadoop.hive.ql.parse.repl.dump.io.FunctionSerializer;
import org.apache.hadoop.hive.ql.parse.repl.dump.io.JsonWriter;

class CreateFunctionHandler extends AbstractEventHandler {
  CreateFunctionHandler(NotificationEvent event) {
    super(event);
  }

  @Override
  public void handle(Context withinContext) throws Exception {
    CreateFunctionMessage createFunctionMessage =
        deserializer.getCreateFunctionMessage(event.getMessage());
    LOG.info("Processing#{} CREATE_MESSAGE message : {}", fromEventId(), event.getMessage());
    Path metadataPath = new Path(withinContext.eventRoot, EximUtil.METADATA_NAME);
    FileSystem fileSystem = metadataPath.getFileSystem(withinContext.hiveConf);

    try (JsonWriter jsonWriter = new JsonWriter(fileSystem, metadataPath)) {
      new FunctionSerializer(createFunctionMessage.getFunctionObj(), withinContext.hiveConf)
          .writeTo(jsonWriter, withinContext.replicationSpec);
    }
    withinContext.createDmd(this).write();
  }

  @Override
  public DumpType dumpType() {
    return DumpType.EVENT_CREATE_FUNCTION;
  }
}
