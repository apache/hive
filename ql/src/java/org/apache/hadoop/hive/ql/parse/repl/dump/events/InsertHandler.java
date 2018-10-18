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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.InsertMessage;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collections;
import java.util.List;


class InsertHandler extends AbstractEventHandler {

  InsertHandler(NotificationEvent event) {
    super(event);
  }

  @Override
  public void handle(Context withinContext) throws Exception {
    if (withinContext.hiveConf.getBoolVar(HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY)) {
      return;
    }
    InsertMessage insertMsg = deserializer.getInsertMessage(event.getMessage());
    org.apache.hadoop.hive.ql.metadata.Table qlMdTable = tableObject(insertMsg);

    if (!Utils.shouldReplicate(withinContext.replicationSpec, qlMdTable, withinContext.hiveConf)) {
      return;
    }

    // In case of ACID tables, insert event should not have fired.
    assert(!AcidUtils.isTransactionalTable(qlMdTable));

    List<Partition> qlPtns = null;
    if (qlMdTable.isPartitioned() && (null != insertMsg.getPtnObj())) {
      qlPtns = Collections.singletonList(partitionObject(qlMdTable, insertMsg));
    }
    Path metaDataPath = new Path(withinContext.eventRoot, EximUtil.METADATA_NAME);

    // Mark the replace type based on INSERT-INTO or INSERT_OVERWRITE operation
    withinContext.replicationSpec.setIsReplace(insertMsg.isReplace());
    EximUtil.createExportDump(metaDataPath.getFileSystem(withinContext.hiveConf), metaDataPath,
        qlMdTable, qlPtns,
        withinContext.replicationSpec,
        withinContext.hiveConf);
    Iterable<String> files = insertMsg.getFiles();

    if (files != null) {
      Path dataPath;
      if ((null == qlPtns) || qlPtns.isEmpty()) {
        dataPath = new Path(withinContext.eventRoot, EximUtil.DATA_PATH_NAME);
      } else {
        /*
         * Insert into/overwrite operation shall operate on one or more partitions or even partitions from multiple
         * tables. But, Insert event is generated for each partition to which the data is inserted. So, qlPtns list
         * will have only one entry.
         */
        assert(1 == qlPtns.size());
        dataPath = new Path(withinContext.eventRoot, qlPtns.get(0).getName());
      }

      // encoded filename/checksum of files, write into _files
      try (BufferedWriter fileListWriter = writer(withinContext, dataPath)) {
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

  private org.apache.hadoop.hive.ql.metadata.Table tableObject(InsertMessage insertMsg) throws Exception {
    return new org.apache.hadoop.hive.ql.metadata.Table(insertMsg.getTableObj());
  }

  private org.apache.hadoop.hive.ql.metadata.Partition partitionObject(
          org.apache.hadoop.hive.ql.metadata.Table qlMdTable, InsertMessage insertMsg) throws Exception {
    return new org.apache.hadoop.hive.ql.metadata.Partition(qlMdTable, insertMsg.getPtnObj());
  }

  private BufferedWriter writer(Context withinContext, Path dataPath) throws IOException {
    Path filesPath = new Path(dataPath, EximUtil.FILES_NAME);
    FileSystem fs = dataPath.getFileSystem(withinContext.hiveConf);
    return new BufferedWriter(new OutputStreamWriter(fs.create(filesPath)));
  }

  @Override
  public DumpType dumpType() {
    return DumpType.EVENT_INSERT;
  }
}
