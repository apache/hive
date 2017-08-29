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
import org.apache.hadoop.hive.metastore.messaging.CreateTableMessage;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.EximUtil;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.hive.ql.parse.repl.DumpType;

class CreateTableHandler extends AbstractEventHandler {

  CreateTableHandler(NotificationEvent event) {
    super(event);
  }

  @Override
  public void handle(Context withinContext) throws Exception {
    CreateTableMessage ctm = deserializer.getCreateTableMessage(event.getMessage());
    LOG.info("Processing#{} CREATE_TABLE message : {}", fromEventId(), event.getMessage());
    org.apache.hadoop.hive.metastore.api.Table tobj = ctm.getTableObj();

    if (tobj == null) {
      LOG.debug("Event#{} was a CREATE_TABLE_EVENT with no table listed");
      return;
    }

    Table qlMdTable = new Table(tobj);
    if (qlMdTable.isView()) {
      withinContext.replicationSpec.setIsMetadataOnly(true);
    }

    Path metaDataPath = new Path(withinContext.eventRoot, EximUtil.METADATA_NAME);
    EximUtil.createExportDump(
        metaDataPath.getFileSystem(withinContext.hiveConf),
        metaDataPath,
        qlMdTable,
        null,
        withinContext.replicationSpec);

    Path dataPath = new Path(withinContext.eventRoot, "data");
    Iterable<String> files = ctm.getFiles();
    if (files != null) {
      // encoded filename/checksum of files, write into _files
      try (BufferedWriter fileListWriter = writer(withinContext, dataPath)) {
        for (String file : files) {
          fileListWriter.write(file + "\n");
        }
      }
    }
    withinContext.createDmd(this).write();
  }

  private BufferedWriter writer(Context withinContext, Path dataPath) throws IOException {
    FileSystem fs = dataPath.getFileSystem(withinContext.hiveConf);
    Path filesPath = new Path(dataPath, EximUtil.FILES_NAME);
    return new BufferedWriter(new OutputStreamWriter(fs.create(filesPath)));
  }

  @Override
  public DumpType dumpType() {
    return DumpType.EVENT_CREATE_TABLE;
  }
}
