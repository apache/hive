/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.repl;

import com.google.common.primitives.Ints;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
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
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.TableSpec;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;
import org.apache.hadoop.hive.ql.parse.repl.dump.HiveWrapper;
import org.apache.hadoop.hive.ql.parse.repl.dump.TableExport;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.apache.hadoop.hive.ql.parse.repl.dump.events.EventHandler;
import org.apache.hadoop.hive.ql.parse.repl.dump.events.EventHandlerFactory;
import org.apache.hadoop.hive.ql.parse.repl.dump.io.FunctionSerializer;
import org.apache.hadoop.hive.ql.parse.repl.dump.io.JsonWriter;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class ReplDumpTask extends Task<ReplDumpWork> implements Serializable {
  private static final String dumpSchema = "dump_dir,last_repl_id#string,string";
  private static final String FUNCTIONS_ROOT_DIR_NAME = "_functions";
  private static final String FUNCTION_METADATA_DIR_NAME = "_metadata";

  private Logger LOG = LoggerFactory.getLogger(ReplDumpTask.class);
  private Logger REPL_STATE_LOG = LoggerFactory.getLogger("ReplState");

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
    REPL_STATE_LOG
        .info("Repl Dump: Started Repl Dump for DB: {}, Dump Type: INCREMENTAL", dbName);
    while (evIter.hasNext()) {
      NotificationEvent ev = evIter.next();
      lastReplId = ev.getEventId();
      Path evRoot = new Path(dumpRoot, String.valueOf(lastReplId));
      dumpEvent(ev, evRoot, cmRoot);
    }

    REPL_STATE_LOG.info("Repl Dump: Completed Repl Dump for DB: {}", dbName);

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
    EventHandlerFactory.handlerFor(ev).handle(context);
    REPL_STATE_LOG.info(
        "Repl Dump: Dumped event with ID: {}, Type: {} and dumped metadata and data to path {}",
        String.valueOf(ev.getEventId()), ev.getEventType(), evRoot.toUri().toString());
  }

  private ReplicationSpec getNewEventOnlyReplicationSpec(Long eventId) throws SemanticException {
    return getNewReplicationSpec(eventId.toString(), eventId.toString());
  }

  private Long bootStrapDump(Path dumpRoot, DumpMetaData dmd, Path cmRoot) throws Exception {
    // bootstrap case
    Long bootDumpBeginReplId = getHive().getMSC().getCurrentNotificationEventId().getEventId();
    for (String dbName : Utils.matchesDb(getHive(), work.dbNameOrPattern)) {
      REPL_STATE_LOG
          .info("Repl Dump: Started analyzing Repl Dump for DB: {}, Dump Type: BOOTSTRAP",
              dbName);
      LOG.debug("ReplicationSemanticAnalyzer: analyzeReplDump dumping db: " + dbName);

      Path dbRoot = dumpDbMetadata(dbName, dumpRoot);
      dumpFunctionMetadata(dbName, dumpRoot);
      for (String tblName : Utils.matchesTbl(getHive(), dbName, work.tableNameOrPattern)) {
        LOG.debug(
            "analyzeReplDump dumping table: " + tblName + " to db root " + dbRoot.toUri());
        dumpTable(dbName, tblName, dbRoot);
      }
    }
    Long bootDumpEndReplId = getHive().getMSC().getCurrentNotificationEventId().getEventId();
    LOG.info("Bootstrap object dump phase took from {} to {}", bootDumpBeginReplId,
        bootDumpEndReplId);

    // Now that bootstrap has dumped all objects related, we have to account for the changes
    // that occurred while bootstrap was happening - i.e. we have to look through all events
    // during the bootstrap period and consolidate them with our dump.

    IMetaStoreClient.NotificationFilter evFilter =
        new DatabaseAndTableFilter(work.dbNameOrPattern, work.tableNameOrPattern);
    EventUtils.MSClientNotificationFetcher evFetcher =
        new EventUtils.MSClientNotificationFetcher(getHive().getMSC());
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
    return bootDumpEndReplId;
  }

  private Path dumpDbMetadata(String dbName, Path dumpRoot) throws Exception {
    Path dbRoot = new Path(dumpRoot, dbName);
    // TODO : instantiating FS objects are generally costly. Refactor
    FileSystem fs = dbRoot.getFileSystem(conf);
    Path dumpPath = new Path(dbRoot, EximUtil.METADATA_NAME);
    HiveWrapper.Tuple<Database> database = new HiveWrapper(getHive(), dbName).database();
    EximUtil.createDbExportDump(fs, dumpPath, database.object, database.replicationSpec);
    REPL_STATE_LOG.info("Repl Dump: Dumped DB metadata");
    return dbRoot;
  }

  private void dumpTable(String dbName, String tblName, Path dbRoot) throws Exception {
    try {
      Hive db = getHive();
      TableSpec ts = new TableSpec(db, conf, dbName + "." + tblName, null);
      TableExport.Paths exportPaths =
          new TableExport.Paths(work.astRepresentationForErrorMsg, dbRoot, tblName, conf);
      new TableExport(exportPaths, ts, getNewReplicationSpec(), db, conf, LOG).run();
      REPL_STATE_LOG.info(
          "Repl Dump: Analyzed dump for table/view: {}.{} and dumping metadata and data to path {}",
          dbName, tblName, exportPaths.exportRootDir.toString());
    } catch (InvalidTableException te) {
      // Bootstrap dump shouldn't fail if the table is dropped/renamed while dumping it.
      // Just log a debug message and skip it.
      LOG.debug(te.getMessage());
    }
  }

  private ReplicationSpec getNewReplicationSpec() throws TException {
    ReplicationSpec rspec = getNewReplicationSpec("replv2", "will-be-set");
    rspec.setCurrentReplicationState(String.valueOf(getHive().getMSC()
        .getCurrentNotificationEventId().getEventId()));
    return rspec;
  }

  private ReplicationSpec getNewReplicationSpec(String evState, String objState) {
    return new ReplicationSpec(true, false, evState, objState, false, true, true);
  }

  private String getNextDumpDir() {
    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST)) {
      // make it easy to write .q unit tests, instead of unique id generation.
      // however, this does mean that in writing tests, we have to be aware that
      // repl dump will clash with prior dumps, and thus have to clean up properly.
      if (work.testInjectDumpDir == null) {
        return "next";
      } else {
        return work.testInjectDumpDir;
      }
    } else {
      return String.valueOf(System.currentTimeMillis());
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
      Path functionMetadataRoot = new Path(functionRoot, FUNCTION_METADATA_DIR_NAME);
      try (JsonWriter jsonWriter =
          new JsonWriter(functionMetadataRoot.getFileSystem(conf), functionMetadataRoot)) {
        FunctionSerializer serializer = new FunctionSerializer(tuple.object, conf);
        serializer.writeTo(jsonWriter, tuple.replicationSpec);
      }
      REPL_STATE_LOG.info("Repl Dump: Dumped metadata for function: {}", functionName);
    }
  }

  private HiveWrapper.Tuple<Function> functionTuple(String functionName, String dbName) {
    try {
      HiveWrapper.Tuple<Function> tuple = new HiveWrapper(getHive(), dbName).function(functionName);
      if (tuple.object.getResourceUris().isEmpty()) {
        REPL_STATE_LOG.warn(
            "Not replicating function: " + functionName + " as it seems to have been created "
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
    return StageType.REPLDUMP;
  }
}
