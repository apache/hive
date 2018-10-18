
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

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.WriteEventInfo;
import org.apache.hadoop.hive.metastore.messaging.CommitTxnMessage;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;
import org.apache.hadoop.fs.FileSystem;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

class CommitTxnHandler extends AbstractEventHandler {

  CommitTxnHandler(NotificationEvent event) {
    super(event);
  }

  private BufferedWriter writer(Context withinContext, Path dataPath) throws IOException {
    Path filesPath = new Path(dataPath, EximUtil.FILES_NAME);
    FileSystem fs = dataPath.getFileSystem(withinContext.hiveConf);
    return new BufferedWriter(new OutputStreamWriter(fs.create(filesPath)));
  }

  private void writeDumpFiles(Context withinContext, Iterable<String> files, Path dataPath) throws IOException {
    // encoded filename/checksum of files, write into _files
    try (BufferedWriter fileListWriter = writer(withinContext, dataPath)) {
      for (String file : files) {
        fileListWriter.write(file + "\n");
      }
    }
  }

  private void createDumpFile(Context withinContext, org.apache.hadoop.hive.ql.metadata.Table qlMdTable,
                  List<Partition> qlPtns, List<List<String>> fileListArray) throws IOException, SemanticException {
    if (fileListArray == null || fileListArray.isEmpty()) {
      return;
    }

    Path metaDataPath = new Path(withinContext.eventRoot, EximUtil.METADATA_NAME);
    // In case of ACID operations, same directory may have many other sub directory for different write id stmt id
    // combination. So we can not set isreplace to true.
    withinContext.replicationSpec.setIsReplace(false);
    EximUtil.createExportDump(metaDataPath.getFileSystem(withinContext.hiveConf), metaDataPath,
            qlMdTable, qlPtns,
            withinContext.replicationSpec,
            withinContext.hiveConf);

    if ((null == qlPtns) || qlPtns.isEmpty()) {
      Path dataPath = new Path(withinContext.eventRoot, EximUtil.DATA_PATH_NAME);
      writeDumpFiles(withinContext, fileListArray.get(0), dataPath);
    } else {
      for (int idx = 0; idx < qlPtns.size(); idx++) {
        Path dataPath = new Path(withinContext.eventRoot, qlPtns.get(idx).getName());
        writeDumpFiles(withinContext, fileListArray.get(idx), dataPath);
      }
    }
  }

  private void createDumpFileForTable(Context withinContext, org.apache.hadoop.hive.ql.metadata.Table qlMdTable,
                    List<Partition> qlPtns, List<List<String>> fileListArray) throws IOException, SemanticException {
    Path newPath = HiveUtils.getDumpPath(withinContext.eventRoot, qlMdTable.getDbName(), qlMdTable.getTableName());
    Context context = new Context(withinContext);
    context.setEventRoot(newPath);
    createDumpFile(context, qlMdTable, qlPtns, fileListArray);
  }

  @Override
  public void handle(Context withinContext) throws Exception {
    LOG.info("Processing#{} COMMIT_TXN message : {}", fromEventId(), event.getMessage());
    String payload = event.getMessage();

    if (!withinContext.hiveConf.getBoolVar(HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY)) {
      CommitTxnMessage commitTxnMessage = deserializer.getCommitTxnMessage(event.getMessage());

      String contextDbName =  withinContext.dbName == null ? null :
              StringUtils.normalizeIdentifier(withinContext.dbName);
      String contextTableName =  withinContext.tableName == null ? null :
              StringUtils.normalizeIdentifier(withinContext.tableName);
      List<WriteEventInfo> writeEventInfoList = HiveMetaStore.HMSHandler.getMSForConf(withinContext.hiveConf).
              getAllWriteEventInfo(commitTxnMessage.getTxnId(), contextDbName, contextTableName);
      int numEntry = (writeEventInfoList != null ? writeEventInfoList.size() : 0);
      if (numEntry != 0) {
        commitTxnMessage.addWriteEventInfo(writeEventInfoList);
        payload = commitTxnMessage.toString();
        LOG.debug("payload for commit txn event : " + payload);
      }

      org.apache.hadoop.hive.ql.metadata.Table qlMdTablePrev = null;
      org.apache.hadoop.hive.ql.metadata.Table qlMdTable = null;
      List<Partition> qlPtns = new ArrayList<>();
      List<List<String>> filesTobeAdded = new ArrayList<>();

      // The below loop creates dump directory for each table. It reads through the list of write notification events,
      // groups the entries per table and creates the lists of files to be replicated. The event directory in the dump
      // path will have subdirectory for each table. This folder will have metadata for the table and the list of files
      // to be replicated. The entries are added in the table with txn id, db name,table name, partition name
      // combination as primary key, so the entries with same table will come together. Only basic table metadata is
      // used during import, so we need not dump the latest table metadata.
      for (int idx = 0; idx < numEntry; idx++) {
        qlMdTable = new org.apache.hadoop.hive.ql.metadata.Table(commitTxnMessage.getTableObj(idx));
        if (qlMdTablePrev == null) {
          qlMdTablePrev = qlMdTable;
        }

        // one dump directory per table
        if (!qlMdTablePrev.getCompleteName().equals(qlMdTable.getCompleteName())) {
          createDumpFileForTable(withinContext, qlMdTablePrev, qlPtns, filesTobeAdded);
          qlPtns = new ArrayList<>();
          filesTobeAdded = new ArrayList<>();
          qlMdTablePrev = qlMdTable;
        }

        if (qlMdTable.isPartitioned() && (null != commitTxnMessage.getPartitionObj(idx))) {
          qlPtns.add(new org.apache.hadoop.hive.ql.metadata.Partition(qlMdTable,
                  commitTxnMessage.getPartitionObj(idx)));
        }

        filesTobeAdded.add(Lists.newArrayList(
                ReplChangeManager.getListFromSeparatedString(commitTxnMessage.getFiles(idx))));
      }

      //Dump last table in the list
      if (qlMdTablePrev != null) {
        createDumpFileForTable(withinContext, qlMdTablePrev, qlPtns, filesTobeAdded);
      }
    }

    DumpMetaData dmd = withinContext.createDmd(this);
    dmd.setPayload(payload);
    dmd.write();
  }

  @Override
  public DumpType dumpType() {
    return DumpType.EVENT_COMMIT_TXN;
  }
}
