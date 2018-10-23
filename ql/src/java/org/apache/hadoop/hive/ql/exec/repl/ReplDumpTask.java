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
package org.apache.hadoop.hive.ql.exec.repl;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.messaging.event.filters.AndFilter;
import org.apache.hadoop.hive.metastore.messaging.event.filters.DatabaseAndTableFilter;
import org.apache.hadoop.hive.metastore.messaging.event.filters.EventBoundaryFilter;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.events.EventUtils;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.TableSpec;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.ReplicationSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;
import org.apache.hadoop.hive.ql.parse.repl.ReplLogger;
import org.apache.hadoop.hive.ql.parse.repl.dump.HiveWrapper;
import org.apache.hadoop.hive.ql.parse.repl.dump.TableExport;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.apache.hadoop.hive.ql.parse.repl.dump.events.EventHandler;
import org.apache.hadoop.hive.ql.parse.repl.dump.events.EventHandlerFactory;
import org.apache.hadoop.hive.ql.parse.repl.dump.io.ConstraintsSerializer;
import org.apache.hadoop.hive.ql.parse.repl.dump.io.FunctionSerializer;
import org.apache.hadoop.hive.ql.parse.repl.dump.io.JsonWriter;
import org.apache.hadoop.hive.ql.parse.repl.dump.log.BootstrapDumpLogger;
import org.apache.hadoop.hive.ql.parse.repl.dump.log.IncrementalDumpLogger;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;
import org.apache.hadoop.hive.ql.plan.ExportWork.MmContext;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class ReplDumpTask extends Task<ReplDumpWork> implements Serializable {
  private static final String dumpSchema = "dump_dir,last_repl_id#string,string";
  private static final String FUNCTIONS_ROOT_DIR_NAME = "_functions";
  private static final String CONSTRAINTS_ROOT_DIR_NAME = "_constraints";
  private static final String FUNCTION_METADATA_FILE_NAME = "_metadata";
  public enum ConstraintFileType {COMMON("common", "c_"), FOREIGNKEY("fk", "f_");
    private final String name;
    private final String prefix;
    private ConstraintFileType(String name, String prefix) {
      this.name = name;
      this.prefix = prefix;
    }
    public String getName() {
      return this.name;
    }

    public String getPrefix() {
      return prefix;
    }
  }

  private static long sleepTime = 60000;
  private Logger LOG = LoggerFactory.getLogger(ReplDumpTask.class);
  private ReplLogger replLogger;

  @Override
  public String getName() {
    return "REPL_DUMP";
  }

  @Override
  protected int execute(DriverContext driverContext) {
    try {
      Hive hiveDb = getHive();
      Path dumpRoot = new Path(conf.getVar(HiveConf.ConfVars.REPLDIR), getNextDumpDir());
      DumpMetaData dmd = new DumpMetaData(dumpRoot, conf);
      Path cmRoot = new Path(conf.getVar(HiveConf.ConfVars.REPLCMDIR));
      Long lastReplId;
      if (work.isBootStrapDump()) {
        lastReplId = bootStrapDump(dumpRoot, dmd, cmRoot, hiveDb);
      } else {
        lastReplId = incrementalDump(dumpRoot, dmd, cmRoot, hiveDb);
      }
      prepareReturnValues(Arrays.asList(dumpRoot.toUri().toString(), String.valueOf(lastReplId)), dumpSchema);
    } catch (RuntimeException e) {
      LOG.error("failed", e);
      setException(e);
      return ErrorMsg.getErrorMsg(e.getMessage()).getErrorCode();
    } catch (Exception e) {
      LOG.error("failed", e);
      setException(e);
      return ErrorMsg.getErrorMsg(e.getMessage()).getErrorCode();
    }
    return 0;
  }

  private void prepareReturnValues(List<String> values, String schema) throws SemanticException {
    LOG.debug("prepareReturnValues : " + schema);
    for (String s : values) {
      LOG.debug("    > " + s);
    }
    Utils.writeOutput(values, new Path(work.resultTempPath), conf);
  }

  private Long incrementalDump(Path dumpRoot, DumpMetaData dmd, Path cmRoot, Hive hiveDb) throws Exception {
    Long lastReplId;// get list of events matching dbPattern & tblPattern
    // go through each event, and dump out each event to a event-level dump dir inside dumproot

    // TODO : instead of simply restricting by message format, we should eventually
    // move to a jdbc-driver-stype registering of message format, and picking message
    // factory per event to decode. For now, however, since all messages have the
    // same factory, restricting by message format is effectively a guard against
    // older leftover data that would cause us problems.

    work.overrideEventTo(hiveDb);

    IMetaStoreClient.NotificationFilter evFilter = new AndFilter(
        new DatabaseAndTableFilter(work.dbNameOrPattern, work.tableNameOrPattern),
        new EventBoundaryFilter(work.eventFrom, work.eventTo));

    EventUtils.MSClientNotificationFetcher evFetcher
        = new EventUtils.MSClientNotificationFetcher(hiveDb);

    EventUtils.NotificationEventIterator evIter = new EventUtils.NotificationEventIterator(
        evFetcher, work.eventFrom, work.maxEventLimit(), evFilter);

    lastReplId = work.eventTo;

    // Right now the only pattern allowed to be specified is *, which matches all the database
    // names. So passing dbname as is works since getDbNotificationEventsCount can exclude filter
    // on database name when it's *. In future, if we support more elaborate patterns, we will
    // have to pass DatabaseAndTableFilter created above to getDbNotificationEventsCount() to get
    // correct event count.
    String dbName = (null != work.dbNameOrPattern && !work.dbNameOrPattern.isEmpty())
        ? work.dbNameOrPattern
        : "?";
    replLogger = new IncrementalDumpLogger(dbName, dumpRoot.toString(),
            evFetcher.getDbNotificationEventsCount(work.eventFrom, dbName, work.eventTo,
                    work.maxEventLimit()));
    replLogger.startLog();
    while (evIter.hasNext()) {
      NotificationEvent ev = evIter.next();
      lastReplId = ev.getEventId();
      Path evRoot = new Path(dumpRoot, String.valueOf(lastReplId));
      dumpEvent(ev, evRoot, cmRoot, hiveDb);
    }

    replLogger.endLog(lastReplId.toString());

    LOG.info("Done dumping events, preparing to return {},{}", dumpRoot.toUri(), lastReplId);
    Utils.writeOutput(
        Arrays.asList(
            "incremental",
            String.valueOf(work.eventFrom),
            String.valueOf(lastReplId)
        ),
        dmd.getDumpFilePath(), conf);
    dmd.setDump(DumpType.INCREMENTAL, work.eventFrom, lastReplId, cmRoot);
    dmd.write();
    return lastReplId;
  }

  private void dumpEvent(NotificationEvent ev, Path evRoot, Path cmRoot, Hive db) throws Exception {
    EventHandler.Context context = new EventHandler.Context(
        evRoot,
        cmRoot,
        db,
        conf,
        getNewEventOnlyReplicationSpec(ev.getEventId()),
        work.dbNameOrPattern,
        work.tableNameOrPattern
    );
    EventHandler eventHandler = EventHandlerFactory.handlerFor(ev);
    eventHandler.handle(context);
    replLogger.eventLog(String.valueOf(ev.getEventId()), eventHandler.dumpType().toString());
  }

  private ReplicationSpec getNewEventOnlyReplicationSpec(Long eventId) {
    ReplicationSpec rspec =
        getNewReplicationSpec(eventId.toString(), eventId.toString(), conf.getBoolean(
            HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY.varname, false));
    rspec.setReplSpecType(ReplicationSpec.Type.INCREMENTAL_DUMP);
    return rspec;
  }

  Long bootStrapDump(Path dumpRoot, DumpMetaData dmd, Path cmRoot, Hive hiveDb) throws Exception {
    // bootstrap case
    // Last repl id would've been captured during compile phase in queryState configs before opening txn.
    // This is needed as we dump data on ACID/MM tables based on read snapshot or else we may lose data from
    // concurrent txns when bootstrap dump in progress. If it is not available, then get it from metastore.
    Long bootDumpBeginReplId = queryState.getConf().getLong(ReplicationSemanticAnalyzer.LAST_REPL_ID_KEY, -1L);
    assert (bootDumpBeginReplId >= 0L);

    String validTxnList = getValidTxnListForReplDump(hiveDb);
    for (String dbName : Utils.matchesDb(hiveDb, work.dbNameOrPattern)) {
      LOG.debug("ReplicationSemanticAnalyzer: analyzeReplDump dumping db: " + dbName);
      replLogger = new BootstrapDumpLogger(dbName, dumpRoot.toString(),
              Utils.getAllTables(hiveDb, dbName).size(),
              hiveDb.getAllFunctions().size());
      replLogger.startLog();
      Path dbRoot = dumpDbMetadata(dbName, dumpRoot, bootDumpBeginReplId, hiveDb);
      dumpFunctionMetadata(dbName, dumpRoot, hiveDb);

      String uniqueKey = Utils.setDbBootstrapDumpState(hiveDb, dbName);
      Exception caught = null;
      try {
        for (String tblName : Utils.matchesTbl(hiveDb, dbName, work.tableNameOrPattern)) {
          LOG.debug(
              "analyzeReplDump dumping table: " + tblName + " to db root " + dbRoot.toUri());
          dumpTable(dbName, tblName, validTxnList, dbRoot, bootDumpBeginReplId, hiveDb);
          dumpConstraintMetadata(dbName, tblName, dbRoot, hiveDb);
        }
      } catch (Exception e) {
        caught = e;
      } finally {
        try {
          Utils.resetDbBootstrapDumpState(hiveDb, dbName, uniqueKey);
        } catch (Exception e) {
          if (caught == null) {
            throw e;
          } else {
            LOG.error("failed to reset the db state for " + uniqueKey
                + " on failure of repl dump", e);
            throw caught;
          }
        }
        if(caught != null) {
          throw caught;
        }
      }
      replLogger.endLog(bootDumpBeginReplId.toString());
    }
    Long bootDumpEndReplId = currentNotificationId(hiveDb);
    LOG.info("Preparing to return {},{}->{}",
        dumpRoot.toUri(), bootDumpBeginReplId, bootDumpEndReplId);
    dmd.setDump(DumpType.BOOTSTRAP, bootDumpBeginReplId, bootDumpEndReplId, cmRoot);
    dmd.write();

    // Set the correct last repl id to return to the user
    // Currently returned bootDumpBeginReplId as we don't consolidate the events after bootstrap
    return bootDumpBeginReplId;
  }

  long currentNotificationId(Hive hiveDb) throws TException {
    return hiveDb.getMSC().getCurrentNotificationEventId().getEventId();
  }

  Path dumpDbMetadata(String dbName, Path dumpRoot, long lastReplId, Hive hiveDb) throws Exception {
    Path dbRoot = new Path(dumpRoot, dbName);
    // TODO : instantiating FS objects are generally costly. Refactor
    FileSystem fs = dbRoot.getFileSystem(conf);
    Path dumpPath = new Path(dbRoot, EximUtil.METADATA_NAME);
    HiveWrapper.Tuple<Database> database = new HiveWrapper(hiveDb, dbName, lastReplId).database();
    EximUtil.createDbExportDump(fs, dumpPath, database.object, database.replicationSpec);
    return dbRoot;
  }

  void dumpTable(String dbName, String tblName, String validTxnList, Path dbRoot, long lastReplId, Hive hiveDb) throws Exception {
    try {
      HiveWrapper.Tuple<Table> tuple = new HiveWrapper(hiveDb, dbName).table(tblName);
      TableSpec tableSpec = new TableSpec(tuple.object);
      TableExport.Paths exportPaths =
          new TableExport.Paths(work.astRepresentationForErrorMsg, dbRoot, tblName, conf, true);
      String distCpDoAsUser = conf.getVar(HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER);
      tuple.replicationSpec.setIsReplace(true);  // by default for all other objects this is false
      if (AcidUtils.isTransactionalTable(tableSpec.tableHandle)) {
        tuple.replicationSpec.setValidWriteIdList(getValidWriteIdList(dbName, tblName, validTxnList));

        // For transactional table, data would be valid snapshot for current txn and doesn't include data
        // added/modified by concurrent txns which are later than current txn. So, need to set last repl Id of this table
        // as bootstrap dump's last repl Id.
        tuple.replicationSpec.setCurrentReplicationState(String.valueOf(lastReplId));
      }
      MmContext mmCtx = MmContext.createIfNeeded(tableSpec.tableHandle);
      new TableExport(
          exportPaths, tableSpec, tuple.replicationSpec, hiveDb, distCpDoAsUser, conf, mmCtx).write();


      replLogger.tableLog(tblName, tableSpec.tableHandle.getTableType());
    } catch (InvalidTableException te) {
      // Bootstrap dump shouldn't fail if the table is dropped/renamed while dumping it.
      // Just log a debug message and skip it.
      LOG.debug(te.getMessage());
    }
  }

  private String getValidWriteIdList(String dbName, String tblName, String validTxnString) throws LockException {
    if ((validTxnString == null) || validTxnString.isEmpty()) {
      return null;
    }
    String fullTableName = AcidUtils.getFullTableName(dbName, tblName);
    ValidWriteIdList validWriteIds = getTxnMgr()
            .getValidWriteIds(Collections.singletonList(fullTableName), validTxnString)
            .getTableValidWriteIdList(fullTableName);
    return ((validWriteIds != null) ? validWriteIds.toString() : null);
  }

  private List<Long> getOpenTxns(ValidTxnList validTxnList) {
    long[] invalidTxns = validTxnList.getInvalidTransactions();
    List<Long> openTxns = new ArrayList<>();
    for (long invalidTxn : invalidTxns) {
      if (!validTxnList.isTxnAborted(invalidTxn)) {
        openTxns.add(invalidTxn);
      }
    }
    return openTxns;
  }

  String getValidTxnListForReplDump(Hive hiveDb) throws HiveException {
    // Key design point for REPL DUMP is to not have any txns older than current txn in which dump runs.
    // This is needed to ensure that Repl dump doesn't copy any data files written by any open txns
    // mainly for streaming ingest case where one delta file shall have data from committed/aborted/open txns.
    // It may also have data inconsistency if the on-going txns doesn't have corresponding open/write
    // events captured which means, catch-up incremental phase won't be able to replicate those txns.
    // So, the logic is to wait for configured amount of time to see if all open txns < current txn is
    // getting aborted/committed. If not, then we forcefully abort those txns just like AcidHouseKeeperService.
    ValidTxnList validTxnList = getTxnMgr().getValidTxns();
    long timeoutInMs = HiveConf.getTimeVar(conf,
            HiveConf.ConfVars.REPL_BOOTSTRAP_DUMP_OPEN_TXN_TIMEOUT, TimeUnit.MILLISECONDS);
    long waitUntilTime = System.currentTimeMillis() + timeoutInMs;
    while (System.currentTimeMillis() < waitUntilTime) {
      // If there are no txns which are open for the given ValidTxnList snapshot, then just return it.
      if (getOpenTxns(validTxnList).isEmpty()) {
        return validTxnList.toString();
      }

      // Wait for 1 minute and check again.
      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
        LOG.info("REPL DUMP thread sleep interrupted", e);
      }
      validTxnList = getTxnMgr().getValidTxns();
    }

    // After the timeout just force abort the open txns
    List<Long> openTxns = getOpenTxns(validTxnList);
    if (!openTxns.isEmpty()) {
      hiveDb.abortTransactions(openTxns);
      validTxnList = getTxnMgr().getValidTxns();
      if (validTxnList.getMinOpenTxn() != null) {
        openTxns = getOpenTxns(validTxnList);
        LOG.warn("REPL DUMP unable to force abort all the open txns: {} after timeout due to unknown reasons. " +
                "However, this is rare case that shouldn't happen.", openTxns);
        throw new IllegalStateException("REPL DUMP triggered abort txns failed for unknown reasons.");
      }
    }
    return validTxnList.toString();
  }

  private ReplicationSpec getNewReplicationSpec(String evState, String objState,
      boolean isMetadataOnly) {
    return new ReplicationSpec(true, isMetadataOnly, evState, objState, false, true, true);
  }

  private String getNextDumpDir() {
    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST)) {
      // make it easy to write .q unit tests, instead of unique id generation.
      // however, this does mean that in writing tests, we have to be aware that
      // repl dump will clash with prior dumps, and thus have to clean up properly.
      if (ReplDumpWork.testInjectDumpDir == null) {
        return "next";
      } else {
        return ReplDumpWork.testInjectDumpDir;
      }
    } else {
      return UUID.randomUUID().toString();
      // TODO: time good enough for now - we'll likely improve this.
      // We may also work in something the equivalent of pid, thrid and move to nanos to ensure
      // uniqueness.
    }
  }

  void dumpFunctionMetadata(String dbName, Path dumpRoot, Hive hiveDb) throws Exception {
    Path functionsRoot = new Path(new Path(dumpRoot, dbName), FUNCTIONS_ROOT_DIR_NAME);
    List<String> functionNames = hiveDb.getFunctions(dbName, "*");
    for (String functionName : functionNames) {
      HiveWrapper.Tuple<Function> tuple = functionTuple(functionName, dbName, hiveDb);
      if (tuple == null) {
        continue;
      }
      Path functionRoot = new Path(functionsRoot, functionName);
      Path functionMetadataFile = new Path(functionRoot, FUNCTION_METADATA_FILE_NAME);
      try (JsonWriter jsonWriter =
          new JsonWriter(functionMetadataFile.getFileSystem(conf), functionMetadataFile)) {
        FunctionSerializer serializer = new FunctionSerializer(tuple.object, conf);
        serializer.writeTo(jsonWriter, tuple.replicationSpec);
      }
      replLogger.functionLog(functionName);
    }
  }

  void dumpConstraintMetadata(String dbName, String tblName, Path dbRoot, Hive hiveDb) throws Exception {
    try {
      Path constraintsRoot = new Path(dbRoot, CONSTRAINTS_ROOT_DIR_NAME);
      Path commonConstraintsFile = new Path(constraintsRoot, ConstraintFileType.COMMON.getPrefix() + tblName);
      Path fkConstraintsFile = new Path(constraintsRoot, ConstraintFileType.FOREIGNKEY.getPrefix() + tblName);
      List<SQLPrimaryKey> pks = hiveDb.getPrimaryKeyList(dbName, tblName);
      List<SQLForeignKey> fks = hiveDb.getForeignKeyList(dbName, tblName);
      List<SQLUniqueConstraint> uks = hiveDb.getUniqueConstraintList(dbName, tblName);
      List<SQLNotNullConstraint> nns = hiveDb.getNotNullConstraintList(dbName, tblName);
      if ((pks != null && !pks.isEmpty()) || (uks != null && !uks.isEmpty())
          || (nns != null && !nns.isEmpty())) {
        try (JsonWriter jsonWriter =
            new JsonWriter(commonConstraintsFile.getFileSystem(conf), commonConstraintsFile)) {
          ConstraintsSerializer serializer = new ConstraintsSerializer(pks, null, uks, nns, conf);
          serializer.writeTo(jsonWriter, null);
        }
      }
      if (fks != null && !fks.isEmpty()) {
        try (JsonWriter jsonWriter =
            new JsonWriter(fkConstraintsFile.getFileSystem(conf), fkConstraintsFile)) {
          ConstraintsSerializer serializer = new ConstraintsSerializer(null, fks, null, null, conf);
          serializer.writeTo(jsonWriter, null);
        }
      }
    } catch (NoSuchObjectException e) {
      // Bootstrap constraint dump shouldn't fail if the table is dropped/renamed while dumping it.
      // Just log a debug message and skip it.
      LOG.debug(e.getMessage());
    }
  }

  private HiveWrapper.Tuple<Function> functionTuple(String functionName, String dbName, Hive hiveDb) {
    try {
      HiveWrapper.Tuple<Function> tuple = new HiveWrapper(hiveDb, dbName).function(functionName);
      if (tuple.object.getResourceUris().isEmpty()) {
        LOG.warn("Not replicating function: " + functionName + " as it seems to have been created "
                + "without USING clause");
        return null;
      }
      return tuple;
    } catch (HiveException e) {
      //This can happen as we are querying the getFunctions before we are getting the actual function
      //in between there can be a drop function by a user in which case our call will fail.
      LOG.info("Function " + functionName
          + " could not be found, we are ignoring it as it can be a valid state ", e);
      return null;
    }
  }

  @Override
  public StageType getType() {
    return StageType.REPL_DUMP;
  }

  @Override
  public boolean canExecuteInParallel() {
    return false;
  }
}
