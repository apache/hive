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

import com.google.common.primitives.Ints;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.hive.metastore.messaging.EventUtils;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.metastore.messaging.event.filters.AndFilter;
import org.apache.hadoop.hive.metastore.messaging.event.filters.DatabaseAndTableFilter;
import org.apache.hadoop.hive.metastore.messaging.event.filters.EventBoundaryFilter;
import org.apache.hadoop.hive.metastore.messaging.event.filters.MessageFormatFilter;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.TableSpec;
import org.apache.hadoop.hive.ql.parse.EximUtil;
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
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

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

  private Logger LOG = LoggerFactory.getLogger(ReplDumpTask.class);
  private ReplLogger replLogger;

  @Override
  public String getName() {
    return "REPL_DUMP";
  }

  @Override
  protected int execute(DriverContext driverContext) {
    try {
      Path dumpRoot = new Path(conf.getVar(HiveConf.ConfVars.REPLDIR), getNextDumpDir());
      DumpMetaData dmd = new DumpMetaData(dumpRoot, conf);
      Path cmRoot = new Path(conf.getVar(HiveConf.ConfVars.REPLCMDIR));
      Long lastReplId;
      if (work.isBootStrapDump()) {
        lastReplId = bootStrapDump(dumpRoot, dmd, cmRoot);
      } else {
        lastReplId = incrementalDump(dumpRoot, dmd, cmRoot);
      }
      prepareReturnValues(Arrays.asList(dumpRoot.toUri().toString(), String.valueOf(lastReplId)), dumpSchema);
    } catch (Exception e) {
      LOG.error("failed", e);
      setException(e);
      return 1;
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

  private Long incrementalDump(Path dumpRoot, DumpMetaData dmd, Path cmRoot) throws Exception {
    Long lastReplId;// get list of events matching dbPattern & tblPattern
    // go through each event, and dump out each event to a event-level dump dir inside dumproot

    // TODO : instead of simply restricting by message format, we should eventually
    // move to a jdbc-driver-stype registering of message format, and picking message
    // factory per event to decode. For now, however, since all messages have the
    // same factory, restricting by message format is effectively a guard against
    // older leftover data that would cause us problems.

    work.overrideEventTo(getHive());

    IMetaStoreClient.NotificationFilter evFilter = new AndFilter(
        new DatabaseAndTableFilter(work.dbNameOrPattern, work.tableNameOrPattern),
        new EventBoundaryFilter(work.eventFrom, work.eventTo),
        new MessageFormatFilter(MessageFactory.getInstance().getMessageFormat()));

    EventUtils.MSClientNotificationFetcher evFetcher
        = new EventUtils.MSClientNotificationFetcher(getHive().getMSC());

    EventUtils.NotificationEventIterator evIter = new EventUtils.NotificationEventIterator(
        evFetcher, work.eventFrom, work.maxEventLimit(), evFilter);

    lastReplId = work.eventTo;
    String dbName = (null != work.dbNameOrPattern && !work.dbNameOrPattern.isEmpty())
        ? work.dbNameOrPattern
        : "?";
    replLogger = new IncrementalDumpLogger(dbName, dumpRoot.toString(),
            evFetcher.getDbNotificationEventsCount(work.eventFrom, dbName));
    replLogger.startLog();
    while (evIter.hasNext()) {
      NotificationEvent ev = evIter.next();
      lastReplId = ev.getEventId();
      Path evRoot = new Path(dumpRoot, String.valueOf(lastReplId));
      dumpEvent(ev, evRoot, cmRoot);
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

  private void dumpEvent(NotificationEvent ev, Path evRoot, Path cmRoot) throws Exception {
    EventHandler.Context context = new EventHandler.Context(
        evRoot,
        cmRoot,
        getHive(),
        conf,
        getNewEventOnlyReplicationSpec(ev.getEventId())
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

  private Long bootStrapDump(Path dumpRoot, DumpMetaData dmd, Path cmRoot) throws Exception {
    // bootstrap case
    Hive hiveDb = getHive();
    Long bootDumpBeginReplId = hiveDb.getMSC().getCurrentNotificationEventId().getEventId();
    for (String dbName : Utils.matchesDb(hiveDb, work.dbNameOrPattern)) {
      LOG.debug("ReplicationSemanticAnalyzer: analyzeReplDump dumping db: " + dbName);
      replLogger = new BootstrapDumpLogger(dbName, dumpRoot.toString(),
              Utils.getAllTables(getHive(), dbName).size(),
              getHive().getAllFunctions().size());
      replLogger.startLog();
      Path dbRoot = dumpDbMetadata(dbName, dumpRoot);
      dumpFunctionMetadata(dbName, dumpRoot);

      String uniqueKey = Utils.setDbBootstrapDumpState(hiveDb, dbName);
      for (String tblName : Utils.matchesTbl(hiveDb, dbName, work.tableNameOrPattern)) {
        LOG.debug(
            "analyzeReplDump dumping table: " + tblName + " to db root " + dbRoot.toUri());
        dumpTable(dbName, tblName, dbRoot);
        dumpConstraintMetadata(dbName, tblName, dbRoot);
      }
      Utils.resetDbBootstrapDumpState(hiveDb, dbName, uniqueKey);
      replLogger.endLog(bootDumpBeginReplId.toString());
    }
    Long bootDumpEndReplId = hiveDb.getMSC().getCurrentNotificationEventId().getEventId();
    LOG.info("Bootstrap object dump phase took from {} to {}", bootDumpBeginReplId,
        bootDumpEndReplId);

    // Now that bootstrap has dumped all objects related, we have to account for the changes
    // that occurred while bootstrap was happening - i.e. we have to look through all events
    // during the bootstrap period and consolidate them with our dump.

    IMetaStoreClient.NotificationFilter evFilter =
        new DatabaseAndTableFilter(work.dbNameOrPattern, work.tableNameOrPattern);
    EventUtils.MSClientNotificationFetcher evFetcher =
        new EventUtils.MSClientNotificationFetcher(hiveDb.getMSC());
    EventUtils.NotificationEventIterator evIter = new EventUtils.NotificationEventIterator(
        evFetcher, bootDumpBeginReplId,
        Ints.checkedCast(bootDumpEndReplId - bootDumpBeginReplId) + 1,
        evFilter);

    // Now we consolidate all the events that happenned during the objdump into the objdump
    while (evIter.hasNext()) {
      NotificationEvent ev = evIter.next();
      Path eventRoot = new Path(dumpRoot, String.valueOf(ev.getEventId()));
      // FIXME : implement consolidateEvent(..) similar to dumpEvent(ev,evRoot)
    }
    LOG.info(
        "Consolidation done, preparing to return {},{}->{}",
        dumpRoot.toUri(), bootDumpBeginReplId, bootDumpEndReplId);
    dmd.setDump(DumpType.BOOTSTRAP, bootDumpBeginReplId, bootDumpEndReplId, cmRoot);
    dmd.write();

    // Set the correct last repl id to return to the user
    // Currently returned bootDumpBeginReplId as we don't consolidate the events after bootstrap
    return bootDumpBeginReplId;
  }

  private Path dumpDbMetadata(String dbName, Path dumpRoot) throws Exception {
    Path dbRoot = new Path(dumpRoot, dbName);
    // TODO : instantiating FS objects are generally costly. Refactor
    FileSystem fs = dbRoot.getFileSystem(conf);
    Path dumpPath = new Path(dbRoot, EximUtil.METADATA_NAME);
    HiveWrapper.Tuple<Database> database = new HiveWrapper(getHive(), dbName).database();
    EximUtil.createDbExportDump(fs, dumpPath, database.object, database.replicationSpec);
    return dbRoot;
  }

  private void dumpTable(String dbName, String tblName, Path dbRoot) throws Exception {
    try {
      Hive db = getHive();
      HiveWrapper.Tuple<Table> tuple = new HiveWrapper(db, dbName).table(tblName);
      TableSpec tableSpec = new TableSpec(tuple.object);
      TableExport.Paths exportPaths =
          new TableExport.Paths(work.astRepresentationForErrorMsg, dbRoot, tblName, conf, true);
      String distCpDoAsUser = conf.getVar(HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER);
      tuple.replicationSpec.setIsReplace(true);  // by default for all other objects this is false
      new TableExport(exportPaths, tableSpec, tuple.replicationSpec, db, distCpDoAsUser, conf).write();

      replLogger.tableLog(tblName, tableSpec.tableHandle.getTableType());
    } catch (InvalidTableException te) {
      // Bootstrap dump shouldn't fail if the table is dropped/renamed while dumping it.
      // Just log a debug message and skip it.
      LOG.debug(te.getMessage());
    }
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

  private void dumpFunctionMetadata(String dbName, Path dumpRoot) throws Exception {
    Path functionsRoot = new Path(new Path(dumpRoot, dbName), FUNCTIONS_ROOT_DIR_NAME);
    List<String> functionNames = getHive().getFunctions(dbName, "*");
    for (String functionName : functionNames) {
      HiveWrapper.Tuple<Function> tuple = functionTuple(functionName, dbName);
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

  private void dumpConstraintMetadata(String dbName, String tblName, Path dbRoot) throws Exception {
    try {
      Path constraintsRoot = new Path(dbRoot, CONSTRAINTS_ROOT_DIR_NAME);
      Path commonConstraintsFile = new Path(constraintsRoot, ConstraintFileType.COMMON.getPrefix() + tblName);
      Path fkConstraintsFile = new Path(constraintsRoot, ConstraintFileType.FOREIGNKEY.getPrefix() + tblName);
      Hive db = getHive();
      List<SQLPrimaryKey> pks = db.getPrimaryKeyList(dbName, tblName);
      List<SQLForeignKey> fks = db.getForeignKeyList(dbName, tblName);
      List<SQLUniqueConstraint> uks = db.getUniqueConstraintList(dbName, tblName);
      List<SQLNotNullConstraint> nns = db.getNotNullConstraintList(dbName, tblName);
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

  private HiveWrapper.Tuple<Function> functionTuple(String functionName, String dbName) {
    try {
      HiveWrapper.Tuple<Function> tuple = new HiveWrapper(getHive(), dbName).function(functionName);
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
