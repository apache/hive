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
package org.apache.hadoop.hive.ql.parse.repl.events;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.InsertMessage;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.thrift.TException;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.ql.parse.ReplicationSemanticAnalyzer.DUMPTYPE;
import static org.apache.hadoop.hive.ql.parse.ReplicationSemanticAnalyzer.DumpMetaData;

public class InsertHandler extends AbstractHandler {

  InsertHandler(NotificationEvent event) {
    super(event);
  }

  @Override
  public void handle(Context withinContext) throws Exception {
    InsertMessage insertMsg = deserializer.getInsertMessage(event.getMessage());
    org.apache.hadoop.hive.ql.metadata.Table qlMdTable = tableObject(withinContext, insertMsg);
    Map<String, String> partSpec = insertMsg.getPartitionKeyValues();
    List<Partition> qlPtns = null;
    if (qlMdTable.isPartitioned() && !partSpec.isEmpty()) {
      qlPtns = Collections.singletonList(withinContext.db.getPartition(qlMdTable, partSpec, false));
    }
    Path metaDataPath = new Path(withinContext.eventRoot, EximUtil.METADATA_NAME);
    // Mark the replication type as insert into to avoid overwrite while import
    withinContext.replicationSpec.setIsInsert(true);
    EximUtil.createExportDump(metaDataPath.getFileSystem(withinContext.hiveConf), metaDataPath,
        qlMdTable, qlPtns,
        withinContext.replicationSpec);
    Iterable<String> files = insertMsg.getFiles();

    if (files != null) {
      // encoded filename/checksum of files, write into _files
      try (BufferedWriter fileListWriter = writer(withinContext)) {
        for (String file : files) {
          fileListWriter.write(file + "\n");
        }
      }
    }

    LOG.info("Processing#{} INSERT message : {}", fromEventId(), event.getMessage());
    DumpMetaData dmd = withinContext.createDmd(this);
    dmd.setPayload(event.getMessage());
    dmd.write();
  }

  private org.apache.hadoop.hive.ql.metadata.Table tableObject(
      Context withinContext, InsertMessage insertMsg) throws TException {
    return new org.apache.hadoop.hive.ql.metadata.Table(
        withinContext.db.getMSC().getTable(
            insertMsg.getDB(), insertMsg.getTable()
        )
    );
  }

  private BufferedWriter writer(Context withinContext) throws IOException {
    Path dataPath = new Path(withinContext.eventRoot, EximUtil.DATA_PATH_NAME);
    Path filesPath = new Path(dataPath, EximUtil.FILES_NAME);
    FileSystem fs = dataPath.getFileSystem(withinContext.hiveConf);
    return new BufferedWriter(new OutputStreamWriter(fs.create(filesPath)));
  }

  @Override
  public DUMPTYPE dumpType() {
    return DUMPTYPE.EVENT_INSERT;
  }
}
