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

import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.api.GetAllWriteEventInfoRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.WriteEventInfo;
import org.apache.hadoop.hive.metastore.messaging.CommitTxnMessage;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.metadata.HiveFatalException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;

import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class CommitTxnHandler extends AbstractEventHandler<CommitTxnMessage> {

  CommitTxnHandler(NotificationEvent event) {
    super(event);
  }

  @Override
  CommitTxnMessage eventMessage(String stringRepresentation) {
    return deserializer.getCommitTxnMessage(stringRepresentation);
  }

  private void writeDumpFiles(Table qlMdTable, Partition ptn, Iterable<String> files, Context withinContext,
                              Path dataPath)
          throws IOException, LoginException, MetaException, HiveFatalException, SemanticException {
    boolean copyAtLoad = withinContext.hiveConf.getBoolVar(HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET);
    if (copyAtLoad) {
      // encoded filename/checksum of files, write into _files
      writeEncodedDumpFiles(withinContext, files, dataPath);
    } else {
      for (String file : files) {
        writeFileEntry(qlMdTable, ptn, file, withinContext);
      }
    }
  }

  private void createDumpFile(Context withinContext, org.apache.hadoop.hive.ql.metadata.Table qlMdTable,
                  List<Partition> qlPtns, List<List<String>> fileListArray)
          throws IOException, SemanticException, LoginException, MetaException, HiveFatalException {
    if (fileListArray == null || fileListArray.isEmpty()) {
      return;
    }

    Path metaDataPath = new Path(withinContext.eventRoot, EximUtil.METADATA_NAME);
    // In case of ACID operations, same directory may have many other subdirectory for different write id stmt id
    // combination. So we can not set isreplace to true.
    withinContext.replicationSpec.setIsReplace(false);
    EximUtil.createExportDump(metaDataPath.getFileSystem(withinContext.hiveConf), metaDataPath,
            qlMdTable, qlPtns,
            withinContext.replicationSpec,
            withinContext.hiveConf);

    if ((null == qlPtns) || qlPtns.isEmpty()) {
      Path dataPath = new Path(withinContext.eventRoot, EximUtil.DATA_PATH_NAME);
      writeDumpFiles(qlMdTable, null, fileListArray.get(0), withinContext, dataPath);
    } else {
      for (int idx = 0; idx < qlPtns.size(); idx++) {
        Path dataPath = new Path(withinContext.eventRoot, EximUtil.DATA_PATH_NAME + File.separator
                + qlPtns.get(idx).getName());
        writeDumpFiles(qlMdTable, qlPtns.get(idx), fileListArray.get(idx), withinContext, dataPath);
      }
    }
  }

  private void createDumpFileForTable(Context withinContext, org.apache.hadoop.hive.ql.metadata.Table qlMdTable,
                    List<Partition> qlPtns, List<List<String>> fileListArray)
          throws IOException, SemanticException, LoginException, MetaException, HiveFatalException {
    Path newPath = HiveUtils.getDumpPath(withinContext.eventRoot, qlMdTable.getDbName(), qlMdTable.getTableName());
    Context context = new Context(withinContext);
    context.setEventRoot(newPath);
    createDumpFile(context, qlMdTable, qlPtns, fileListArray);
  }

  private List<WriteEventInfo> getAllWriteEventInfo(Context withinContext) throws Exception {
    String contextDbName = StringUtils.normalizeIdentifier(withinContext.replScope.getDbName());
    GetAllWriteEventInfoRequest request = new GetAllWriteEventInfoRequest(eventMessage.getTxnId());
    request.setDbName(contextDbName);
    List<WriteEventInfo> writeEventInfoList = withinContext.db.getMSC().getAllWriteEventInfo(request);
    return ((writeEventInfoList == null)
            ? null
            : new ArrayList<>(Collections2.filter(writeEventInfoList,
              writeEventInfo -> {
                assert(writeEventInfo != null);
                // If replication policy is replaced with new included/excluded tables list, then events
                // corresponding to tables which are included in both old and new policies should be dumped.
                // If table is included in new policy but not in old policy, then it should be skipped.
                // Those tables would be bootstrapped along with the current incremental
                // replication dump. If the table is in the list of tables to be bootstrapped, then
                // it should be skipped.
                return (ReplUtils.tableIncludedInReplScope(withinContext.replScope, writeEventInfo.getTable())
                        && ReplUtils.tableIncludedInReplScope(withinContext.oldReplScope, writeEventInfo.getTable())
                        && !withinContext.getTablesForBootstrap().contains(writeEventInfo.getTable().toLowerCase()));
              })));
  }

  @Override
  public void handle(Context withinContext) throws Exception {
    if (!ReplUtils.includeAcidTableInDump(withinContext.hiveConf)) {
      return;
    }
    LOG.info("Processing#{} COMMIT_TXN message : {}", fromEventId(), eventMessageAsJSON);
    String payload = eventMessageAsJSON;

    if (!withinContext.hiveConf.getBoolVar(HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY)) {

      boolean replicatingAcidEvents = true;
      if (withinContext.hiveConf.getBoolVar(HiveConf.ConfVars.REPL_BOOTSTRAP_ACID_TABLES)) {
        // We do not dump ACID table related events when taking a bootstrap dump of ACID tables as
        // part of an incremental dump. So we shouldn't be dumping any changes to ACID table as
        // part of the commit. At the same time we need to dump the commit transaction event so
        // that replication can end a transaction opened when replaying open transaction event.
        LOG.debug("writeEventsInfoList will be removed from commit message because we are " +
                "bootstrapping acid tables.");
        replicatingAcidEvents = false;
      } else if (!ReplUtils.includeAcidTableInDump(withinContext.hiveConf)) {
        // Similar to the above condition, only for testing purposes, if the config doesn't allow
        // ACID tables to be replicated, we don't dump any changes to the ACID tables as part of
        // commit.
        LOG.debug("writeEventsInfoList will be removed from commit message because we are " +
                "not dumping acid tables.");
        replicatingAcidEvents = false;
      }

      List<WriteEventInfo> writeEventInfoList = null;
      if (replicatingAcidEvents) {
        writeEventInfoList = getAllWriteEventInfo(withinContext);

        if (ReplUtils.filterTransactionOperations(withinContext.hiveConf)
           && (writeEventInfoList == null || writeEventInfoList.size() == 0)) {
          // If optimizing transactions, no need to dump this one
          // if there were no write events.
          return;
        }
      }

      int numEntry = (writeEventInfoList != null ? writeEventInfoList.size() : 0);
      if (numEntry != 0) {
        eventMessage.addWriteEventInfo(writeEventInfoList);
        payload = jsonMessageEncoder.getSerializer().serialize(eventMessage);
        LOG.debug("payload for commit txn event : " + eventMessageAsJSON);
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
        qlMdTable = new org.apache.hadoop.hive.ql.metadata.Table(eventMessage.getTableObj(idx));
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

        if (qlMdTable.isPartitioned() && (null != eventMessage.getPartitionObj(idx))) {
          qlPtns.add(new org.apache.hadoop.hive.ql.metadata.Partition(qlMdTable,
              eventMessage.getPartitionObj(idx)));
        }

        filesTobeAdded.add(Lists.newArrayList(
            ReplChangeManager.getListFromSeparatedString(eventMessage.getFiles(idx))));
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
