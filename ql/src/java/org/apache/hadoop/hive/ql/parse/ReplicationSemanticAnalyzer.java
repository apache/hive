/**
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
package org.apache.hadoop.hive.ql.parse;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.primitives.Ints;
import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.EventUtils;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.metastore.messaging.event.filters.AndFilter;
import org.apache.hadoop.hive.metastore.messaging.event.filters.DatabaseAndTableFilter;
import org.apache.hadoop.hive.metastore.messaging.event.filters.EventBoundaryFilter;
import org.apache.hadoop.hive.metastore.messaging.event.filters.MessageFormatFilter;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;
import org.apache.hadoop.hive.ql.parse.repl.dump.HiveWrapper;
import org.apache.hadoop.hive.ql.parse.repl.dump.events.EventHandler;
import org.apache.hadoop.hive.ql.parse.repl.dump.events.EventHandlerFactory;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;
import org.apache.hadoop.hive.ql.parse.repl.dump.io.FunctionSerializer;
import org.apache.hadoop.hive.ql.parse.repl.dump.io.JsonWriter;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.apache.hadoop.hive.ql.parse.repl.load.MetaData;
import org.apache.hadoop.hive.ql.parse.repl.load.message.MessageHandler;
import org.apache.hadoop.hive.ql.parse.repl.load.message.MessageHandlerFactory;
import org.apache.hadoop.hive.ql.plan.AlterDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc;
import org.apache.hadoop.hive.ql.plan.CreateDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.CreateFunctionDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.DependencyCollectionWork;
import org.apache.hadoop.hive.ql.plan.FunctionWork;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_FROM;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_LIMIT;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_REPL_DUMP;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_REPL_LOAD;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_REPL_STATUS;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_TO;

public class ReplicationSemanticAnalyzer extends BaseSemanticAnalyzer {
  // Database name or pattern
  private String dbNameOrPattern;
  // Table name or pattern
  private String tblNameOrPattern;
  private Long eventFrom;
  private Long eventTo;
  private Integer maxEventLimit;
  // Base path for REPL LOAD
  private String path;

  private static String testInjectDumpDir = null; // unit tests can overwrite this to affect default dump behaviour
  private static final String dumpSchema = "dump_dir,last_repl_id#string,string";

  private static final String FUNCTIONS_ROOT_DIR_NAME = "_functions";
  private static final String FUNCTION_METADATA_DIR_NAME = "_metadata";
  private final static Logger REPL_STATE_LOG = LoggerFactory.getLogger("ReplState");

  ReplicationSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    LOG.debug("ReplicationSemanticAanalyzer: analyzeInternal");
    LOG.debug(ast.getName() + ":" + ast.getToken().getText() + "=" + ast.getText());
    switch (ast.getToken().getType()) {
      case TOK_REPL_DUMP: {
        LOG.debug("ReplicationSemanticAnalyzer: analyzeInternal: dump");
        initReplDump(ast);
        analyzeReplDump(ast);
        break;
      }
      case TOK_REPL_LOAD: {
        LOG.debug("ReplicationSemanticAnalyzer: analyzeInternal: load");
        initReplLoad(ast);
        analyzeReplLoad(ast);
        break;
      }
      case TOK_REPL_STATUS: {
        LOG.debug("ReplicationSemanticAnalyzer: analyzeInternal: status");
        initReplStatus(ast);
        analyzeReplStatus(ast);
        break;
      }
      default: {
        throw new SemanticException("Unexpected root token");
      }
    }
  }

  private void initReplDump(ASTNode ast) {
    int numChildren = ast.getChildCount();
    dbNameOrPattern = PlanUtils.stripQuotes(ast.getChild(0).getText());
    // skip the first node, which is always required
    int currNode = 1;
    while (currNode < numChildren) {
      if (ast.getChild(currNode).getType() != TOK_FROM) {
        // optional tblName was specified.
        tblNameOrPattern = PlanUtils.stripQuotes(ast.getChild(currNode).getText());
      } else {
        // TOK_FROM subtree
        Tree fromNode = ast.getChild(currNode);
        eventFrom = Long.parseLong(PlanUtils.stripQuotes(fromNode.getChild(0).getText()));
        // skip the first, which is always required
        int numChild = 1;
        while (numChild < fromNode.getChildCount()) {
          if (fromNode.getChild(numChild).getType() == TOK_TO) {
            eventTo =
                Long.parseLong(PlanUtils.stripQuotes(fromNode.getChild(numChild + 1).getText()));
            // skip the next child, since we already took care of it
            numChild++;
          } else if (fromNode.getChild(numChild).getType() == TOK_LIMIT) {
            maxEventLimit =
                Integer.parseInt(PlanUtils.stripQuotes(fromNode.getChild(numChild + 1).getText()));
            // skip the next child, since we already took care of it
            numChild++;
          }
          // move to the next child in FROM tree
          numChild++;
        }
        // FROM node is always the last
        break;
      }
      // move to the next root node
      currNode++;
    }
  }

  // REPL DUMP
  private void analyzeReplDump(ASTNode ast) throws SemanticException {
    LOG.debug("ReplicationSemanticAnalyzer.analyzeReplDump: " + String.valueOf(dbNameOrPattern)
        + "." + String.valueOf(tblNameOrPattern) + " from " + String.valueOf(eventFrom) + " to "
        + String.valueOf(eventTo) + " maxEventLimit " + String.valueOf(maxEventLimit));
    String replRoot = conf.getVar(HiveConf.ConfVars.REPLDIR);
    Path dumpRoot = new Path(replRoot, getNextDumpDir());
    DumpMetaData dmd = new DumpMetaData(dumpRoot, conf);
    Path cmRoot = new Path(conf.getVar(HiveConf.ConfVars.REPLCMDIR));
    Long lastReplId;
    try {
      if (eventFrom == null){
        // bootstrap case
        Long bootDumpBeginReplId = db.getMSC().getCurrentNotificationEventId().getEventId();
        for (String dbName : matchesDb(dbNameOrPattern)) {
          REPL_STATE_LOG.info("Repl Dump: Started analyzing Repl Dump for DB: {}, Dump Type: BOOTSTRAP", dbName);
          LOG.debug("ReplicationSemanticAnalyzer: analyzeReplDump dumping db: " + dbName);
          Path dbRoot = dumpDbMetadata(dbName, dumpRoot);
          dumpFunctionMetadata(dbName, dumpRoot);
          for (String tblName : matchesTbl(dbName, tblNameOrPattern)) {
            LOG.debug("ReplicationSemanticAnalyzer: analyzeReplDump dumping table: " + tblName
                + " to db root " + dbRoot.toUri());
            dumpTbl(ast, dbName, tblName, dbRoot);
          }
          REPL_STATE_LOG.info("Repl Dump: Completed analyzing Repl Dump for DB: {} and created {} COPY tasks to dump " +
                              "metadata and data",
                              dbName, rootTasks.size());
        }
        Long bootDumpEndReplId = db.getMSC().getCurrentNotificationEventId().getEventId();
        LOG.info("Bootstrap object dump phase took from {} to {}", bootDumpBeginReplId, bootDumpEndReplId);

        // Now that bootstrap has dumped all objects related, we have to account for the changes
        // that occurred while bootstrap was happening - i.e. we have to look through all events
        // during the bootstrap period and consolidate them with our dump.

        IMetaStoreClient.NotificationFilter evFilter =
            new DatabaseAndTableFilter(dbNameOrPattern, tblNameOrPattern);
        EventUtils.MSClientNotificationFetcher evFetcher =
            new EventUtils.MSClientNotificationFetcher(db.getMSC());
        EventUtils.NotificationEventIterator evIter = new EventUtils.NotificationEventIterator(
            evFetcher, bootDumpBeginReplId,
            Ints.checkedCast(bootDumpEndReplId - bootDumpBeginReplId) + 1,
            evFilter );

        // Now we consolidate all the events that happenned during the objdump into the objdump
        while (evIter.hasNext()){
          NotificationEvent ev = evIter.next();
          Path evRoot = new Path(dumpRoot, String.valueOf(ev.getEventId()));
          // FIXME : implement consolidateEvent(..) similar to dumpEvent(ev,evRoot)
        }
        LOG.info(
            "Consolidation done, preparing to return {},{}->{}",
            dumpRoot.toUri(), bootDumpBeginReplId, bootDumpEndReplId);
        dmd.setDump(DumpType.BOOTSTRAP, bootDumpBeginReplId, bootDumpEndReplId, cmRoot);
        dmd.write();

        // Set the correct last repl id to return to the user
        lastReplId = bootDumpEndReplId;
      } else {
        // get list of events matching dbPattern & tblPattern
        // go through each event, and dump out each event to a event-level dump dir inside dumproot
        if (eventTo == null){
          eventTo = db.getMSC().getCurrentNotificationEventId().getEventId();
          LOG.debug("eventTo not specified, using current event id : {}", eventTo);
        } else if (eventTo < eventFrom) {
          throw new Exception("Invalid event ID input received in TO clause");
        }

        Integer maxRange = Ints.checkedCast(eventTo - eventFrom + 1);
        if ((maxEventLimit == null) || (maxEventLimit > maxRange)){
          maxEventLimit = maxRange;
        }

        // TODO : instead of simply restricting by message format, we should eventually
        // move to a jdbc-driver-stype registering of message format, and picking message
        // factory per event to decode. For now, however, since all messages have the
        // same factory, restricting by message format is effectively a guard against
        // older leftover data that would cause us problems.

        IMetaStoreClient.NotificationFilter evFilter = new AndFilter(
            new DatabaseAndTableFilter(dbNameOrPattern, tblNameOrPattern),
            new EventBoundaryFilter(eventFrom, eventTo),
            new MessageFormatFilter(MessageFactory.getInstance().getMessageFormat()));

        EventUtils.MSClientNotificationFetcher evFetcher
            = new EventUtils.MSClientNotificationFetcher(db.getMSC());

        EventUtils.NotificationEventIterator evIter = new EventUtils.NotificationEventIterator(
            evFetcher, eventFrom, maxEventLimit, evFilter);

        lastReplId = eventTo;
        REPL_STATE_LOG.info("Repl Dump: Started Repl Dump for DB: {}, Dump Type: INCREMENTAL",
                            (null != dbNameOrPattern && !dbNameOrPattern.isEmpty()) ? dbNameOrPattern : "?");
        while (evIter.hasNext()){
          NotificationEvent ev = evIter.next();
          lastReplId = ev.getEventId();
          Path evRoot = new Path(dumpRoot, String.valueOf(lastReplId));
          dumpEvent(ev, evRoot, cmRoot);
        }

        REPL_STATE_LOG.info("Repl Dump: Completed Repl Dump for DB: {}",
                            (null != dbNameOrPattern && !dbNameOrPattern.isEmpty()) ? dbNameOrPattern : "?");

        LOG.info("Done dumping events, preparing to return {},{}", dumpRoot.toUri(), lastReplId);
        Utils.writeOutput(
            Arrays.asList(
                "incremental",
                String.valueOf(eventFrom),
                String.valueOf(lastReplId)
            ),
            dmd.getDumpFilePath(), conf);
        dmd.setDump(DumpType.INCREMENTAL, eventFrom, lastReplId, cmRoot);
        dmd.write();
      }
      prepareReturnValues(Arrays.asList(dumpRoot.toUri().toString(), String.valueOf(lastReplId)), dumpSchema);
      setFetchTask(createFetchTask(dumpSchema));
    } catch (Exception e) {
      // TODO : simple wrap & rethrow for now, clean up with error codes
      LOG.warn("Error during analyzeReplDump", e);
      throw new SemanticException(e);
    }
  }

  private void dumpEvent(NotificationEvent ev, Path evRoot, Path cmRoot) throws Exception {
    EventHandler.Context context = new EventHandler.Context(
        evRoot,
        cmRoot,
        db,
        conf,
        getNewEventOnlyReplicationSpec(ev.getEventId())
    );
    EventHandlerFactory.handlerFor(ev).handle(context);
    REPL_STATE_LOG.info("Repl Dump: Dumped event with ID: {}, Type: {} and dumped metadata and data to path {}",
                        String.valueOf(ev.getEventId()), ev.getEventType(), evRoot.toUri().toString());
  }

  public static void injectNextDumpDirForTest(String dumpdir){
    testInjectDumpDir = dumpdir;
  }

  private String getNextDumpDir() {
    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST)) {
      // make it easy to write .q unit tests, instead of unique id generation.
      // however, this does mean that in writing tests, we have to be aware that
      // repl dump will clash with prior dumps, and thus have to clean up properly.
      if (testInjectDumpDir == null){
        return "next";
      } else {
        return testInjectDumpDir;
      }
    } else {
      return String.valueOf(System.currentTimeMillis());
      // TODO: time good enough for now - we'll likely improve this.
      // We may also work in something the equivalent of pid, thrid and move to nanos to ensure
      // uniqueness.
    }
  }

  /**
   *
   * @param dbName
   * @param dumpRoot
   * @return db dumped path
   * @throws SemanticException
   */
  private Path dumpDbMetadata(String dbName, Path dumpRoot) throws SemanticException {
    Path dbRoot = new Path(dumpRoot, dbName);
    try {
      // TODO : instantiating FS objects are generally costly. Refactor
      FileSystem fs = dbRoot.getFileSystem(conf);
      Path dumpPath = new Path(dbRoot, EximUtil.METADATA_NAME);
      HiveWrapper.Tuple<Database> database = new HiveWrapper(db, dbName).database();
      EximUtil.createDbExportDump(fs, dumpPath, database.object, database.replicationSpec);
      REPL_STATE_LOG.info("Repl Dump: Dumped DB metadata");
    } catch (Exception e) {
      // TODO : simple wrap & rethrow for now, clean up with error codes
      throw new SemanticException(e);
    }
    return dbRoot;
  }

  private void dumpFunctionMetadata(String dbName, Path dumpRoot) throws SemanticException {
    Path functionsRoot = new Path(new Path(dumpRoot, dbName), FUNCTIONS_ROOT_DIR_NAME);
    try {
      // TODO : This should ideally return the Function Objects and not Strings(function names) that should be done by the caller, Look at this separately.
      List<String> functionNames = db.getFunctions(dbName, "*");
      for (String functionName : functionNames) {
        HiveWrapper.Tuple<Function> tuple;
        try {
          tuple = new HiveWrapper(db, dbName).function(functionName);
        } catch (HiveException e) {
          //This can happen as we are querying the getFunctions before we are getting the actual function
          //in between there can be a drop function by a user in which case our call will fail.
          LOG.info("Function " + functionName + " could not be found, we are ignoring it as it can be a valid state ", e);
          continue;
        }
        if (tuple.object.getResourceUris().isEmpty()) {
          REPL_STATE_LOG.warn(
              "Not replicating function: " + functionName + " as it seems to have been created "
                  + "without USING clause");
          continue;
        }

        Path functionMetadataRoot =
            new Path(new Path(functionsRoot, functionName), FUNCTION_METADATA_DIR_NAME);
        try (JsonWriter jsonWriter = new JsonWriter(functionMetadataRoot.getFileSystem(conf),
            functionMetadataRoot)) {
          new FunctionSerializer(tuple.object).writeTo(jsonWriter, tuple.replicationSpec);
        }
        REPL_STATE_LOG.info("Repl Dump: Dumped metadata for function: {}", functionName);
      }
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  /**
   *
   * @param ast
   * @param dbName
   * @param tblName
   * @param dbRoot
   * @return tbl dumped path
   * @throws SemanticException
   */
  private Path dumpTbl(ASTNode ast, String dbName, String tblName, Path dbRoot) throws SemanticException {
    Path tableRoot = new Path(dbRoot, tblName);
    try {
      URI toURI = EximUtil.getValidatedURI(conf, tableRoot.toUri().toString());
      TableSpec ts = new TableSpec(db, conf, dbName + "." + tblName, null);

      ExportSemanticAnalyzer.prepareExport(ast, toURI, ts, getNewReplicationSpec(), db, conf, ctx,
          rootTasks, inputs, outputs, LOG);
      REPL_STATE_LOG.info("Repl Dump: Analyzed dump for table/view: {}.{} and created copy tasks to dump metadata " +
                          "and data to path {}", dbName, tblName, toURI.toString());
    } catch (HiveException e) {
      // TODO : simple wrap & rethrow for now, clean up with error codes
      throw new SemanticException(e);
    }
    return tableRoot;
  }

  // REPL LOAD
  private void initReplLoad(ASTNode ast) {
    int numChildren = ast.getChildCount();
    path = PlanUtils.stripQuotes(ast.getChild(0).getText());
    if (numChildren > 1) {
      dbNameOrPattern = PlanUtils.stripQuotes(ast.getChild(1).getText());
    }
    if (numChildren > 2) {
      tblNameOrPattern = PlanUtils.stripQuotes(ast.getChild(2).getText());
    }
  }

  /*
   * Example dump dirs we need to be able to handle :
   *
   * for: hive.repl.rootdir = staging/
   * Then, repl dumps will be created in staging/<dumpdir>
   *
   * single-db-dump: staging/blah12345 will contain a db dir for the db specified
   *  blah12345/
   *   default/
   *    _metadata
   *    tbl1/
   *      _metadata
   *      dt=20160907/
   *        _files
   *    tbl2/
   *    tbl3/
   *    unptn_tbl/
   *      _metadata
   *      _files
   *
   * multi-db-dump: staging/bar12347 will contain dirs for each db covered
   * staging/
   *  bar12347/
   *   default/
   *     ...
   *   sales/
   *     ...
   *
   * single table-dump: staging/baz123 will contain a table object dump inside
   * staging/
   *  baz123/
   *    _metadata
   *    dt=20150931/
   *      _files
   *
   * incremental dump : staging/blue123 will contain dirs for each event inside.
   * staging/
   *  blue123/
   *    34/
   *    35/
   *    36/
   */
  private void analyzeReplLoad(ASTNode ast) throws SemanticException {
    LOG.debug("ReplSemanticAnalyzer.analyzeReplLoad: " + String.valueOf(dbNameOrPattern) + "."
        + String.valueOf(tblNameOrPattern) + " from " + String.valueOf(path));

    // for analyze repl load, we walk through the dir structure available in the path,
    // looking at each db, and then each table, and then setting up the appropriate
    // import job in its place.

    try {

      Path loadPath = new Path(path);
      final FileSystem fs = loadPath.getFileSystem(conf);

      if (!fs.exists(loadPath)) {
        // supposed dump path does not exist.
        throw new FileNotFoundException(loadPath.toUri().toString());
      }

      // Now, the dumped path can be one of three things:
      // a) It can be a db dump, in which case we expect a set of dirs, each with a
      // db name, and with a _metadata file in each, and table dirs inside that.
      // b) It can be a table dump dir, in which case we expect a _metadata dump of
      // a table in question in the dir, and individual ptn dir hierarchy.
      // c) A dump can be an incremental dump, which means we have several subdirs
      // each of which have the evid as the dir name, and each of which correspond
      // to a event-level dump. Currently, only CREATE_TABLE and ADD_PARTITION are
      // handled, so all of these dumps will be at a table/ptn level.

      // For incremental repl, we will have individual events which can
      // be other things like roles and fns as well.
      // At this point, all dump dirs should contain a _dumpmetadata file that
      // tells us what is inside that dumpdir.

      DumpMetaData dmd = new DumpMetaData(loadPath, conf);

      boolean evDump = false;
      if (dmd.isIncrementalDump()){
        LOG.debug("{} contains an incremental dump", loadPath);
        evDump = true;
      } else {
        LOG.debug("{} contains an bootstrap dump", loadPath);
      }

      if ((!evDump) && (tblNameOrPattern != null) && !(tblNameOrPattern.isEmpty())) {
        // not an event dump, and table name pattern specified, this has to be a tbl-level dump
        rootTasks.addAll(analyzeTableLoad(dbNameOrPattern, tblNameOrPattern, path, null, null, null));
        return;
      }

      FileStatus[] srcs = LoadSemanticAnalyzer.matchFilesOrDir(fs, loadPath);
      if (srcs == null || (srcs.length == 0)) {
        LOG.warn("Nothing to load at {}", loadPath.toUri().toString());
        return;
      }

      FileStatus[] dirsInLoadPath = fs.listStatus(loadPath, EximUtil.getDirectoryFilter(fs));

      if ((dirsInLoadPath == null) || (dirsInLoadPath.length == 0)) {
        throw new IllegalArgumentException("No data to load in path " + loadPath.toUri().toString());
      }

      if (!evDump){
        // not an event dump, not a table dump - thus, a db dump
        if ((dbNameOrPattern != null) && (dirsInLoadPath.length > 1)) {
          LOG.debug("Found multiple dirs when we expected 1:");
          for (FileStatus d : dirsInLoadPath) {
            LOG.debug("> " + d.getPath().toUri().toString());
          }
          throw new IllegalArgumentException(
              "Multiple dirs in "
                  + loadPath.toUri().toString()
                  + " does not correspond to REPL LOAD expecting to load to a singular destination point.");
        }

        for (FileStatus dir : dirsInLoadPath) {
          analyzeDatabaseLoad(dbNameOrPattern, fs, dir);
        }
      } else {
        // event dump, each subdir is an individual event dump.
        Arrays.sort(dirsInLoadPath); // we need to guarantee that the directory listing we got is in order of evid.

        Task<? extends Serializable> evTaskRoot = TaskFactory.get(new DependencyCollectionWork(), conf);
        Task<? extends Serializable> taskChainTail = evTaskRoot;

        int evstage = 0;
        int evIter = 0;
        Long lastEvid = null;
        Map<String,Long> dbsUpdated = new ReplicationSpec.ReplStateMap<String,Long>();
        Map<String,Long> tablesUpdated = new ReplicationSpec.ReplStateMap<String,Long>();

        REPL_STATE_LOG.info("Repl Load: Started analyzing Repl load for DB: {} from path {}, Dump Type: INCREMENTAL",
                (null != dbNameOrPattern && !dbNameOrPattern.isEmpty()) ? dbNameOrPattern : "?",
                loadPath.toUri().toString());
        for (FileStatus dir : dirsInLoadPath){
          LOG.debug("Loading event from {} to {}.{}", dir.getPath().toUri(), dbNameOrPattern, tblNameOrPattern);
          // event loads will behave similar to table loads, with one crucial difference
          // precursor order is strict, and each event must be processed after the previous one.
          // The way we handle this strict order is as follows:
          // First, we start with a taskChainTail which is a dummy noop task (a DependecyCollectionTask)
          // at the head of our event chain. For each event we process, we tell analyzeTableLoad to
          // create tasks that use the taskChainTail as a dependency. Then, we collect all those tasks
          // and introduce a new barrier task(also a DependencyCollectionTask) which depends on all
          // these tasks. Then, this barrier task becomes our new taskChainTail. Thus, we get a set of
          // tasks as follows:
          //
          //                 --->ev1.task1--                          --->ev2.task1--
          //                /               \                        /               \
          //  evTaskRoot-->*---->ev1.task2---*--> ev1.barrierTask-->*---->ev2.task2---*->evTaskChainTail
          //                \               /
          //                 --->ev1.task3--
          //
          // Once this entire chain is generated, we add evTaskRoot to rootTasks, so as to execute the
          // entire chain

          String locn = dir.getPath().toUri().toString();
          DumpMetaData eventDmd = new DumpMetaData(new Path(locn), conf);
          List<Task<? extends Serializable>> evTasks = analyzeEventLoad(
              dbNameOrPattern, tblNameOrPattern, locn, taskChainTail,
              dbsUpdated, tablesUpdated, eventDmd);
          evIter++;
          REPL_STATE_LOG.info("Repl Load: Analyzed load for event {}/{} " +
                              "with ID: {}, Type: {}, Path: {}",
                              evIter, dirsInLoadPath.length,
                              dir.getPath().getName(), eventDmd.getDumpType().toString(), locn);

          LOG.debug("evstage#{} got {} tasks", evstage, evTasks!=null ? evTasks.size() : 0);
          if ((evTasks != null) && (!evTasks.isEmpty())){
            Task<? extends Serializable> barrierTask = TaskFactory.get(new DependencyCollectionWork(), conf);
            for (Task<? extends Serializable> t : evTasks){
              t.addDependentTask(barrierTask);
              LOG.debug("Added {}:{} as a precursor of barrier task {}:{}",
                  t.getClass(), t.getId(), barrierTask.getClass(), barrierTask.getId());
            }
            LOG.debug("Updated taskChainTail from {}{} to {}{}",
                taskChainTail.getClass(), taskChainTail.getId(), barrierTask.getClass(), barrierTask.getId());
            taskChainTail = barrierTask;
            evstage++;
            lastEvid = dmd.getEventTo();
          }
        }

        // Now, we need to update repl.last.id for the various parent objects that were updated.
        // This update logic will work differently based on what "level" REPL LOAD was run on.
        //  a) If this was a REPL LOAD at a table level, i.e. both dbNameOrPattern and
        //     tblNameOrPattern were specified, then the table is the only thing we should
        //     update the repl.last.id for.
        //  b) If this was a db-level REPL LOAD, then we should update the db, as well as any
        //     tables affected by partition level operations. (any table level ops will
        //     automatically be updated as the table gets updated. Note - renames will need
        //     careful handling.
        //  c) If this was a wh-level REPL LOAD, then we should update every db for which there
        //     were events occurring, as well as tables for which there were ptn-level ops
        //     happened. Again, renames must be taken care of.
        //
        // So, what we're going to do is have each event load update dbsUpdated and tablesUpdated
        // accordingly, but ignore updates to tablesUpdated & dbsUpdated in the case of a
        // table-level REPL LOAD, using only the table itself. In the case of a db-level REPL
        // LOAD, we ignore dbsUpdated, but inject our own, and do not ignore tblsUpdated.
        // And for wh-level, we do no special processing, and use all of dbsUpdated and
        // tblsUpdated as-is.

        // Additional Note - although this var says "dbNameOrPattern", on REPL LOAD side,
        // we do not support a pattern It can be null or empty, in which case
        // we re-use the existing name from the dump, or it can be specified,
        // in which case we honour it. However, having this be a pattern is an error.
        // Ditto for tblNameOrPattern.


        if (evstage > 0){
          if ((tblNameOrPattern != null) && (!tblNameOrPattern.isEmpty())){
            // if tblNameOrPattern is specified, then dbNameOrPattern will be too, and
            // thus, this is a table-level REPL LOAD - only table needs updating.
            // If any of the individual events logged any other dbs as having changed,
            // null them out.
            dbsUpdated.clear();
            tablesUpdated.clear();
            tablesUpdated.put(dbNameOrPattern + "." + tblNameOrPattern, lastEvid);
          } else  if ((dbNameOrPattern != null) && (!dbNameOrPattern.isEmpty())){
            // if dbNameOrPattern is specified and tblNameOrPattern isn't, this is a
            // db-level update, and thus, the database needs updating. In addition.
            dbsUpdated.clear();
            dbsUpdated.put(dbNameOrPattern, lastEvid);
          }
        }

        for (String tableName : tablesUpdated.keySet()){
          // weird - AlterTableDesc requires a HashMap to update props instead of a Map.
          HashMap<String,String> mapProp = new HashMap<String,String>();
          mapProp.put(
              ReplicationSpec.KEY.CURR_STATE_ID.toString(),
              tablesUpdated.get(tableName).toString());
          AlterTableDesc alterTblDesc =  new AlterTableDesc(
              AlterTableDesc.AlterTableTypes.ADDPROPS, null, false);
          alterTblDesc.setProps(mapProp);
          alterTblDesc.setOldName(tableName);
          Task<? extends Serializable> updateReplIdTask = TaskFactory.get(
              new DDLWork(inputs, outputs, alterTblDesc), conf);
          taskChainTail.addDependentTask(updateReplIdTask);
          taskChainTail = updateReplIdTask;
        }
        for (String dbName : dbsUpdated.keySet()){
          Map<String,String> mapProp = new HashMap<String,String>();
          mapProp.put(
              ReplicationSpec.KEY.CURR_STATE_ID.toString(),
              dbsUpdated.get(dbName).toString());
          AlterDatabaseDesc alterDbDesc = new AlterDatabaseDesc(dbName, mapProp);
          Task<? extends Serializable> updateReplIdTask = TaskFactory.get(
              new DDLWork(inputs, outputs, alterDbDesc), conf);
          taskChainTail.addDependentTask(updateReplIdTask);
          taskChainTail = updateReplIdTask;
        }
        rootTasks.add(evTaskRoot);
        REPL_STATE_LOG.info("Repl Load: Completed analyzing Repl load for DB: {} from path {} and created import " +
                            "(DDL/COPY/MOVE) tasks",
                            (null != dbNameOrPattern && !dbNameOrPattern.isEmpty()) ? dbNameOrPattern : "?",
                            loadPath.toUri().toString());
      }

    } catch (Exception e) {
      // TODO : simple wrap & rethrow for now, clean up with error codes
      throw new SemanticException(e);
    }

  }

  private List<Task<? extends Serializable>> analyzeEventLoad(
      String dbName, String tblName, String locn, Task<? extends Serializable> precursor,
      Map<String, Long> dbsUpdated, Map<String, Long> tablesUpdated, DumpMetaData dmd)
      throws SemanticException {
    MessageHandler.Context context =
        new MessageHandler.Context(dbName, tblName, locn, precursor, dmd, conf, db, ctx, LOG);
    MessageHandler messageHandler = MessageHandlerFactory.handlerFor(dmd.getDumpType());
    List<Task<? extends Serializable>> tasks = messageHandler.handle(context);

    if (precursor != null) {
      for (Task<? extends Serializable> t : tasks) {
        precursor.addDependentTask(t);
        LOG.debug("Added {}:{} as a precursor of {}:{}",
            precursor.getClass(), precursor.getId(), t.getClass(), t.getId());
      }
    }
    dbsUpdated.putAll(messageHandler.databasesUpdated());
    tablesUpdated.putAll(messageHandler.tablesUpdated());
    inputs.addAll(messageHandler.readEntities());
    outputs.addAll(messageHandler.writeEntities());
    return tasks;
  }

  private boolean existEmptyDb(String dbName) throws InvalidOperationException, HiveException {
    Hive hiveDb = Hive.get();
    Database db = hiveDb.getDatabase(dbName);
    if (null != db) {
      List<String> allTables = hiveDb.getAllTables(dbName);
      List<String> allFunctions = hiveDb.getFunctions(dbName, "*");
      if (!allTables.isEmpty()) {
        throw new InvalidOperationException(
                "Database " + db.getName() + " is not empty. One or more tables exist.");
      }
      if (!allFunctions.isEmpty()) {
        throw new InvalidOperationException(
                "Database " + db.getName() + " is not empty. One or more functions exist.");
      }

      return true;
    }

    return false;
  }

  private void analyzeDatabaseLoad(String dbName, FileSystem fs, FileStatus dir)
      throws SemanticException {
    try {
      // Path being passed to us is a db dump location. We go ahead and load as needed.
      // dbName might be null or empty, in which case we keep the original db name for the new
      // database creation

      // Two steps here - first, we read the _metadata file here, and create a CreateDatabaseDesc
      // associated with that
      // Then, we iterate over all subdirs, and create table imports for each.

      MetaData rv = new MetaData();
      try {
        rv = EximUtil.readMetaData(fs, new Path(dir.getPath(), EximUtil.METADATA_NAME));
      } catch (IOException e) {
        throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(), e);
      }

      Database dbObj = rv.getDatabase();

      if (dbObj == null) {
        throw new IllegalArgumentException(
            "_metadata file read did not contain a db object - invalid dump.");
      }

      if ((dbName == null) || (dbName.isEmpty())) {
        // We use dbName specified as long as it is not null/empty. If so, then we use the original
        // name
        // recorded in the thrift object.
        dbName = dbObj.getName();
      }

      REPL_STATE_LOG.info("Repl Load: Started analyzing Repl Load for DB: {} from Dump Dir: {}, Dump Type: BOOTSTRAP",
                          dbName, dir.getPath().toUri().toString());

      Task<? extends Serializable> dbRootTask = null;
      if (existEmptyDb(dbName)) {
        AlterDatabaseDesc alterDbDesc = new AlterDatabaseDesc(dbName, dbObj.getParameters());
        dbRootTask = TaskFactory.get(new DDLWork(inputs, outputs, alterDbDesc), conf);
      } else {
        CreateDatabaseDesc createDbDesc = new CreateDatabaseDesc();
        createDbDesc.setName(dbName);
        createDbDesc.setComment(dbObj.getDescription());
        createDbDesc.setDatabaseProperties(dbObj.getParameters());
        // note that we do not set location - for repl load, we want that auto-created.

        createDbDesc.setIfNotExists(false);
        // If it exists, we want this to be an error condition. Repl Load is not intended to replace a
        // db.
        // TODO: we might revisit this in create-drop-recreate cases, needs some thinking on.
        dbRootTask = TaskFactory.get(new DDLWork(inputs, outputs, createDbDesc), conf);
      }

      rootTasks.add(dbRootTask);
      FileStatus[] dirsInDbPath = fs.listStatus(dir.getPath(), EximUtil.getDirectoryFilter(fs));

      for (FileStatus tableDir : Collections2.filter(Arrays.asList(dirsInDbPath), new TableDirPredicate())) {
        analyzeTableLoad(
            dbName, null, tableDir.getPath().toUri().toString(), dbRootTask, null, null);
        REPL_STATE_LOG.info("Repl Load: Analyzed table/view/partition load from path {}",
                            tableDir.getPath().toUri().toString());
      }

      //Function load
      Path functionMetaDataRoot = new Path(dir.getPath(), FUNCTIONS_ROOT_DIR_NAME);
      if (fs.exists(functionMetaDataRoot)) {
        List<FileStatus> functionDirectories =
            Arrays.asList(fs.listStatus(functionMetaDataRoot, EximUtil.getDirectoryFilter(fs)));
        for (FileStatus functionDir : functionDirectories) {
          analyzeFunctionLoad(dbName, functionDir, dbRootTask);
          REPL_STATE_LOG.info("Repl Load: Analyzed function load from path {}",
                              functionDir.getPath().toUri().toString());
        }
      }

      REPL_STATE_LOG.info("Repl Load: Completed analyzing Repl Load for DB: {} and created import (DDL/COPY/MOVE) tasks",
              dbName);
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  private static class TableDirPredicate implements Predicate<FileStatus> {
    @Override
    public boolean apply(FileStatus fileStatus) {
      return !fileStatus.getPath().getName().contains(FUNCTIONS_ROOT_DIR_NAME);
    }
  }

  private void analyzeFunctionLoad(String dbName, FileStatus functionDir,
      Task<? extends Serializable> createDbTask) throws IOException, SemanticException {
    URI fromURI = EximUtil
        .getValidatedURI(conf, stripQuotes(functionDir.getPath().toUri().toString()));
    Path fromPath = new Path(fromURI.getScheme(), fromURI.getAuthority(), fromURI.getPath());

    FileSystem fs = FileSystem.get(fromURI, conf);
    inputs.add(toReadEntity(fromPath, conf));

    try {
      MetaData metaData = EximUtil.readMetaData(fs, new Path(fromPath, EximUtil.METADATA_NAME));
      ReplicationSpec replicationSpec = metaData.getReplicationSpec();
      if (replicationSpec.isNoop()) {
        // nothing to do here, silently return.
        return;
      }
      CreateFunctionDesc desc = new CreateFunctionDesc(
          dbName + "." + metaData.function.getFunctionName(),
          false,
          metaData.function.getClassName(),
          metaData.function.getResourceUris()
      );

      Task<FunctionWork> currentTask = TaskFactory.get(new FunctionWork(desc), conf);
      if (createDbTask != null) {
        createDbTask.addDependentTask(currentTask);
        LOG.debug("Added {}:{} as a precursor of {}:{}",
            createDbTask.getClass(), createDbTask.getId(), currentTask.getClass(),
            currentTask.getId());
      }
    } catch (IOException e) {
      throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(), e);
    }
  }

  private List<Task<? extends Serializable>> analyzeTableLoad(
      String dbName, String tblName, String locn,
      Task<? extends Serializable> precursor,
      Map<String,Long> dbsUpdated, Map<String,Long> tablesUpdated) throws SemanticException {
    // Path being passed to us is a table dump location. We go ahead and load it in as needed.
    // If tblName is null, then we default to the table name specified in _metadata, which is good.
    // or are both specified, in which case, that's what we are intended to create the new table as.
    if (dbName == null || dbName.isEmpty()) {
      throw new SemanticException("Database name cannot be null for a table load");
    }
    try {
      // no location set on repl loads
      boolean isLocationSet = false;
      // all repl imports are non-external
      boolean isExternalSet = false;
      // bootstrap loads are not partition level
      boolean isPartSpecSet = false;
      // repl loads are not partition level
      LinkedHashMap<String, String> parsedPartSpec = null;
      // no location for repl imports
      String parsedLocation = null;
      List<Task<? extends Serializable>> importTasks = new ArrayList<Task<? extends Serializable>>();

      EximUtil.SemanticAnalyzerWrapperContext x =
          new EximUtil.SemanticAnalyzerWrapperContext(conf, db, inputs, outputs, importTasks, LOG,
              ctx);
      ImportSemanticAnalyzer.prepareImport(isLocationSet, isExternalSet, isPartSpecSet,
          (precursor != null), parsedLocation, tblName, dbName, parsedPartSpec, locn, x,
          dbsUpdated, tablesUpdated);

      if (precursor != null) {
        for (Task<? extends Serializable> t : importTasks) {
          precursor.addDependentTask(t);
          LOG.debug("Added {}:{} as a precursor of {}:{}",
              precursor.getClass(), precursor.getId(), t.getClass(), t.getId());
        }
      }

      return importTasks;
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  // REPL STATUS
  private void initReplStatus(ASTNode ast) {
    int numChildren = ast.getChildCount();
    dbNameOrPattern = PlanUtils.stripQuotes(ast.getChild(0).getText());
    if (numChildren > 1) {
      tblNameOrPattern = PlanUtils.stripQuotes(ast.getChild(1).getText());
    }
  }

  private void analyzeReplStatus(ASTNode ast) throws SemanticException {
    LOG.debug("ReplicationSemanticAnalyzer.analyzeReplStatus: " + String.valueOf(dbNameOrPattern)
        + "." + String.valueOf(tblNameOrPattern));

    String replLastId = null;

    try {
      if (tblNameOrPattern != null) {
        // Checking for status of table
        Table tbl = db.getTable(dbNameOrPattern, tblNameOrPattern);
        if (tbl != null) {
          inputs.add(new ReadEntity(tbl));
          Map<String, String> params = tbl.getParameters();
          if (params != null && (params.containsKey(ReplicationSpec.KEY.CURR_STATE_ID.toString()))) {
            replLastId = params.get(ReplicationSpec.KEY.CURR_STATE_ID.toString());
          }
        }
      } else {
        // Checking for status of a db
        Database database = db.getDatabase(dbNameOrPattern);
        if (database != null) {
          inputs.add(new ReadEntity(database));
          Map<String, String> params = database.getParameters();
          if (params != null && (params.containsKey(ReplicationSpec.KEY.CURR_STATE_ID.toString()))) {
            replLastId = params.get(ReplicationSpec.KEY.CURR_STATE_ID.toString());
          }
        }
      }
    } catch (HiveException e) {
      throw new SemanticException(e); // TODO : simple wrap & rethrow for now, clean up with error
                                      // codes
    }

    prepareReturnValues(Collections.singletonList(replLastId), "last_repl_id#string");
    setFetchTask(createFetchTask("last_repl_id#string"));
    LOG.debug("ReplicationSemanticAnalyzer.analyzeReplStatus: writing repl.last.id={} out to {}",
        String.valueOf(replLastId), ctx.getResFile(), conf);
  }

  private void prepareReturnValues(List<String> values, String schema) throws SemanticException {
    LOG.debug("prepareReturnValues : " + schema);
    for (String s : values) {
      LOG.debug("    > " + s);
    }
    ctx.setResFile(ctx.getLocalTmpPath());
    Utils.writeOutput(values, ctx.getResFile(), conf);
  }

  private ReplicationSpec getNewReplicationSpec() throws SemanticException {
    try {
      ReplicationSpec rspec = getNewReplicationSpec("replv2", "will-be-set");
      rspec.setCurrentReplicationState(String.valueOf(db.getMSC()
          .getCurrentNotificationEventId().getEventId()));
      return rspec;
    } catch (Exception e) {
      throw new SemanticException(e); // TODO : simple wrap & rethrow for now, clean up with error codes
    }
  }

  // Use for specifying object state as well as event state
  private ReplicationSpec getNewReplicationSpec(String evState, String objState) throws SemanticException {
    return new ReplicationSpec(true, false, evState, objState, false, true, true);
  }

  // Use for replication states focused on event only, where the obj state will be the event state
  private ReplicationSpec getNewEventOnlyReplicationSpec(Long eventId) throws SemanticException {
    return getNewReplicationSpec(eventId.toString(), eventId.toString());
  }

  private Iterable<? extends String> matchesTbl(String dbName, String tblPattern)
      throws HiveException {
    if (tblPattern == null) {
      return removeValuesTemporaryTables(db.getAllTables(dbName));
    } else {
      return db.getTablesByPattern(dbName, tblPattern);
    }
  }

  private final static String TMP_TABLE_PREFIX =
      SemanticAnalyzer.VALUES_TMP_TABLE_NAME_PREFIX.toLowerCase();

  static Iterable<String> removeValuesTemporaryTables(List<String> tableNames) {
    return Collections2.filter(tableNames,
        tableName -> {
          assert tableName != null;
          return !tableName.toLowerCase().startsWith(TMP_TABLE_PREFIX);
        });
  }

  private Iterable<? extends String> matchesDb(String dbPattern) throws HiveException {
    if (dbPattern == null) {
      return db.getAllDatabases();
    } else {
      return db.getDatabasesByPattern(dbPattern);
    }
  }
}
