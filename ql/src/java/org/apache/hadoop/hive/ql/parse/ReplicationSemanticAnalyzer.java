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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.AddPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterTableMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateTableMessage;
import org.apache.hadoop.hive.metastore.messaging.DropPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.DropTableMessage;
import org.apache.hadoop.hive.metastore.messaging.EventUtils;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.ReplCopyTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc;
import org.apache.hadoop.hive.ql.plan.CreateDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.DependencyCollectionWork;
import org.apache.hadoop.hive.ql.plan.DropTableDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.RenamePartitionDesc;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.IOUtils;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.ql.parse.HiveParser.*;

public class ReplicationSemanticAnalyzer extends BaseSemanticAnalyzer {
  // Database name or pattern
  private String dbNameOrPattern;
  // Table name or pattern
  private String tblNameOrPattern;
  private Long eventFrom;
  private Long eventTo;
  private Integer batchSize;
  // Base path for REPL LOAD
  private String path;

  private static String testInjectDumpDir = null; // unit tests can overwrite this to affect default dump behaviour
  private static final String dumpSchema = "dump_dir,last_repl_id#string,string";

  public static final String DUMPMETADATA = "_dumpmetadata";

  public enum DUMPTYPE {
    BOOTSTRAP("BOOTSTRAP"),
    INCREMENTAL("INCREMENTAL"),
    EVENT_CREATE_TABLE("EVENT_CREATE_TABLE"),
    EVENT_ADD_PARTITION("EVENT_ADD_PARTITION"),
    EVENT_DROP_TABLE("EVENT_DROP_TABLE"),
    EVENT_DROP_PARTITION("EVENT_DROP_PARTITION"),
    EVENT_ALTER_TABLE("EVENT_ALTER_TABLE"),
    EVENT_RENAME_TABLE("EVENT_RENAME_TABLE"),
    EVENT_ALTER_PARTITION("EVENT_ALTER_PARTITION"),
    EVENT_RENAME_PARTITION("EVENT_RENAME_PARTITION"),
    EVENT_UNKNOWN("EVENT_UNKNOWN");

    String type = null;
    DUMPTYPE(String type) {
      this.type = type;
    }

    @Override
    public String toString(){
      return type;
    }

  };

  public class DumpMetaData {
    // wrapper class for reading and writing metadata about a dump
    // responsible for _dumpmetadata files

    private DUMPTYPE dumpType;
    private Long eventFrom = null;
    private Long eventTo = null;
    private String payload = null;
    private boolean initialized = false;

    private final Path dumpRoot;
    private final Path dumpFile;

    public DumpMetaData(Path dumpRoot) {
      this.dumpRoot = dumpRoot;
      dumpFile = new Path(dumpRoot, DUMPMETADATA);
    }

    public DumpMetaData(Path dumpRoot, DUMPTYPE lvl, Long eventFrom, Long eventTo){
      this(dumpRoot);
      setDump(lvl, eventFrom, eventTo);
    }

    public void setDump(DUMPTYPE lvl, Long eventFrom, Long eventTo){
      this.dumpType = lvl;
      this.eventFrom = eventFrom;
      this.eventTo = eventTo;
      this.initialized = true;
    }

    public void loadDumpFromFile() throws SemanticException {
      try {
        // read from dumpfile and instantiate self
        FileSystem fs = dumpFile.getFileSystem(conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(dumpFile)));
        String line = null;
        if ( (line = br.readLine()) != null){
          String[] lineContents = line.split("\t", 4);
          setDump(DUMPTYPE.valueOf(lineContents[0]), Long.valueOf(lineContents[1]), Long.valueOf(lineContents[2]));
          setPayload(lineContents[3].equals(Utilities.nullStringOutput) ? null : lineContents[3]);
        } else {
          throw new IOException("Unable to read valid values from dumpFile:"+dumpFile.toUri().toString());
        }
      } catch (IOException ioe){
        throw new SemanticException(ioe);
      }
    }

    public DUMPTYPE getDumpType() throws SemanticException {
      initializeIfNot();
      return this.dumpType;
    }

    public String getPayload() throws SemanticException {
      initializeIfNot();
      return this.payload;
    }

    public void setPayload(String payload) {
      this.payload = payload;
    }

    public Long getEventFrom() throws SemanticException {
      initializeIfNot();
      return eventFrom;
    }

    public Long getEventTo() throws SemanticException {
      initializeIfNot();
      return eventTo;
    }

    public Path getDumpFilePath() {
      return dumpFile;
    }

    public boolean isIncrementalDump() throws SemanticException {
      initializeIfNot();
      return (this.dumpType == DUMPTYPE.INCREMENTAL);
    }

    private void initializeIfNot() throws SemanticException {
      if (!initialized){
        loadDumpFromFile();
      }
    }

    public void write() throws SemanticException {
      writeOutput(Arrays.asList(dumpType.toString(), eventFrom.toString(), eventTo.toString(), payload), dumpFile);
    }

  }

  public ReplicationSemanticAnalyzer(QueryState queryState) throws SemanticException {
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
          } else if (fromNode.getChild(numChild).getType() == TOK_BATCH) {
            batchSize =
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
        + String.valueOf(eventTo) + " batchsize " + String.valueOf(batchSize));
    String replRoot = conf.getVar(HiveConf.ConfVars.REPLDIR);
    Path dumpRoot = new Path(replRoot, getNextDumpDir());
    DumpMetaData dmd = new DumpMetaData(dumpRoot);
    Long lastReplId;
    try {
      if (eventFrom == null){
        // bootstrap case
        Long bootDumpBeginReplId = db.getMSC().getCurrentNotificationEventId().getEventId();
        for (String dbName : matchesDb(dbNameOrPattern)) {
          LOG.debug("ReplicationSemanticAnalyzer: analyzeReplDump dumping db: " + dbName);
          Path dbRoot = dumpDbMetadata(dbName, dumpRoot);
          for (String tblName : matchesTbl(dbName, tblNameOrPattern)) {
            LOG.debug("ReplicationSemanticAnalyzer: analyzeReplDump dumping table: " + tblName
                + " to db root " + dbRoot.toUri());
            dumpTbl(ast, dbName, tblName, dbRoot);
          }
        }
        Long bootDumpEndReplId = db.getMSC().getCurrentNotificationEventId().getEventId();
        LOG.info("Bootstrap object dump phase took from {} to {}", bootDumpBeginReplId, bootDumpEndReplId);

        // Now that bootstrap has dumped all objects related, we have to account for the changes
        // that occurred while bootstrap was happening - i.e. we have to look through all events
        // during the bootstrap period and consolidate them with our dump.

        IMetaStoreClient.NotificationFilter evFilter =
            EventUtils.getDbTblNotificationFilter(dbNameOrPattern, tblNameOrPattern);
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
        LOG.info("Consolidation done, preparing to return {},{}->{}", dumpRoot.toUri(), bootDumpBeginReplId, bootDumpEndReplId);
        dmd.setDump(DUMPTYPE.BOOTSTRAP, bootDumpBeginReplId, bootDumpEndReplId);
        dmd.write();

        // Set the correct last repl id to return to the user
        lastReplId = bootDumpEndReplId;
      } else {
        // get list of events matching dbPattern & tblPattern
        // go through each event, and dump out each event to a event-level dump dir inside dumproot
        if (eventTo == null){
          eventTo = db.getMSC().getCurrentNotificationEventId().getEventId();
          LOG.debug("eventTo not specified, using current event id : {}", eventTo);
        }

        Integer maxRange = Ints.checkedCast(eventTo - eventFrom + 1);
        if (batchSize == null){
          batchSize = maxRange;
        } else {
          if (batchSize > maxRange){
            batchSize = maxRange;
          }
        }

        IMetaStoreClient.NotificationFilter evFilter = EventUtils.andFilter(
            EventUtils.getDbTblNotificationFilter(dbNameOrPattern, tblNameOrPattern),
            EventUtils.getEventBoundaryFilter(eventFrom, eventTo));

        EventUtils.MSClientNotificationFetcher evFetcher
            = new EventUtils.MSClientNotificationFetcher(db.getMSC());

        EventUtils.NotificationEventIterator evIter = new EventUtils.NotificationEventIterator(
            evFetcher, eventFrom, batchSize, evFilter);

        while (evIter.hasNext()){
          NotificationEvent ev = evIter.next();
          Path evRoot = new Path(dumpRoot, String.valueOf(ev.getEventId()));
          dumpEvent(ev, evRoot);
        }

        LOG.info("Done dumping events, preparing to return {},{}", dumpRoot.toUri(), eventTo);
        writeOutput(Arrays.asList("incremental", String.valueOf(eventFrom), String.valueOf(eventTo)), dmd.getDumpFilePath());
        dmd.setDump(DUMPTYPE.INCREMENTAL, eventFrom, eventTo);
        dmd.write();
        // Set the correct last repl id to return to the user
        lastReplId = eventTo;
      }
      prepareReturnValues(Arrays.asList(dumpRoot.toUri().toString(), String.valueOf(lastReplId)), dumpSchema);
      setFetchTask(createFetchTask(dumpSchema));
    } catch (Exception e) {
      // TODO : simple wrap & rethrow for now, clean up with error codes
      LOG.warn("Error during analyzeReplDump", e);
      throw new SemanticException(e);
    }
  }

  private void dumpEvent(NotificationEvent ev, Path evRoot) throws Exception {
    long evid = ev.getEventId();
    String evidStr = String.valueOf(evid);
    ReplicationSpec replicationSpec = getNewEventOnlyReplicationSpec(evidStr);
    MessageDeserializer md = MessageFactory.getInstance().getDeserializer();
    switch (ev.getEventType()){
      case MessageFactory.CREATE_TABLE_EVENT : {
        CreateTableMessage ctm = md.getCreateTableMessage(ev.getMessage());
        LOG.info("Processing#{} CREATE_TABLE message : {}", ev.getEventId(), ev.getMessage());
        org.apache.hadoop.hive.metastore.api.Table tobj = ctm.getTableObj();

        if (tobj == null){
          LOG.debug("Event#{} was a CREATE_TABLE_EVENT with no table listed");
          break;
        }

        Table qlMdTable = new Table(tobj);

        Path metaDataPath = new Path(evRoot, EximUtil.METADATA_NAME);
        EximUtil.createExportDump(
            metaDataPath.getFileSystem(conf),
            metaDataPath,
            qlMdTable,
            null,
            replicationSpec);

        // FIXME : dump _files should happen at dbnotif time, doing it here is incorrect
        // we will, however, do so here, now, for dev/debug's sake.
        Path dataPath = new Path(evRoot, "data");
        rootTasks.add(ReplCopyTask.getDumpCopyTask(replicationSpec, qlMdTable.getPath(), dataPath , conf));
        (new DumpMetaData(evRoot, DUMPTYPE.EVENT_CREATE_TABLE, evid, evid)).write();
        break;
      }
      case MessageFactory.ADD_PARTITION_EVENT : {
        AddPartitionMessage apm = md.getAddPartitionMessage(ev.getMessage());
        LOG.info("Processing#{} ADD_PARTITION message : {}", ev.getEventId(), ev.getMessage());
        // FIXME : Current MessageFactory api is lacking,
        // and impl is in JSONMessageFactory instead. This needs to be
        // refactored correctly so we don't depend on a specific impl.
        Iterable<org.apache.hadoop.hive.metastore.api.Partition> ptns = apm.getPartitionObjs();
        if ((ptns == null) || (!ptns.iterator().hasNext())) {
          LOG.debug("Event#{} was an ADD_PTN_EVENT with no partitions");
          break;
        }
        org.apache.hadoop.hive.metastore.api.Table tobj = apm.getTableObj();
        if (tobj == null){
          LOG.debug("Event#{} was a ADD_PTN_EVENT with no table listed");
          break;
        }

        final Table qlMdTable = new Table(tobj);
        Iterable<Partition> qlPtns = Iterables.transform(
            ptns,
            new Function<org.apache.hadoop.hive.metastore.api.Partition, Partition>() {
              @Nullable
              @Override
              public Partition apply(@Nullable org.apache.hadoop.hive.metastore.api.Partition input) {
                if (input == null) {
                  return null;
                }
                try {
                  return new Partition(qlMdTable, input);
                } catch (HiveException e) {
                  throw new IllegalArgumentException(e);
                }
              }
            }
        );

        Path metaDataPath = new Path(evRoot, EximUtil.METADATA_NAME);
        EximUtil.createExportDump(
            metaDataPath.getFileSystem(conf),
            metaDataPath,
            qlMdTable,
            qlPtns,
            replicationSpec);

        // FIXME : dump _files should ideally happen at dbnotif time, doing it here introduces
        // rubberbanding. But, till we have support for that, this is our closest equivalent
        for (Partition qlPtn : qlPtns){
          Path ptnDataPath = new Path(evRoot, qlPtn.getName());
          rootTasks.add(ReplCopyTask.getDumpCopyTask(
              replicationSpec, qlPtn.getPartitionPath(), ptnDataPath, conf));
        }

        (new DumpMetaData(evRoot, DUMPTYPE.EVENT_ADD_PARTITION, evid, evid)).write();
        break;
      }
      case MessageFactory.DROP_TABLE_EVENT : {
        LOG.info("Processing#{} DROP_TABLE message : {}", ev.getEventId(), ev.getMessage());
        DumpMetaData dmd = new DumpMetaData(evRoot, DUMPTYPE.EVENT_DROP_TABLE, evid, evid);
        dmd.setPayload(ev.getMessage());
        dmd.write();
        break;
      }
      case MessageFactory.DROP_PARTITION_EVENT : {
        LOG.info("Processing#{} DROP_PARTITION message : {}", ev.getEventId(), ev.getMessage());
        DumpMetaData dmd = new DumpMetaData(evRoot, DUMPTYPE.EVENT_DROP_PARTITION, evid, evid);
        dmd.setPayload(ev.getMessage());
        dmd.write();
        break;
      }
      case MessageFactory.ALTER_TABLE_EVENT : {
        LOG.info("Processing#{} ALTER_TABLE message : {}", ev.getEventId(), ev.getMessage());
        AlterTableMessage atm = md.getAlterTableMessage(ev.getMessage());
        org.apache.hadoop.hive.metastore.api.Table tobjBefore = atm.getTableObjBefore();
        org.apache.hadoop.hive.metastore.api.Table tobjAfter = atm.getTableObjAfter();

        if (tobjBefore.getDbName().equals(tobjAfter.getDbName()) &&
            tobjBefore.getTableName().equals(tobjAfter.getTableName())){
          // regular alter scenario
          replicationSpec.setIsMetadataOnly(true);
          Table qlMdTableAfter = new Table(tobjAfter);
          Path metaDataPath = new Path(evRoot, EximUtil.METADATA_NAME);
          EximUtil.createExportDump(
              metaDataPath.getFileSystem(conf),
              metaDataPath,
              qlMdTableAfter,
              null,
              replicationSpec);

          DumpMetaData dmd = new DumpMetaData(evRoot, DUMPTYPE.EVENT_ALTER_TABLE, evid, evid);
          dmd.setPayload(ev.getMessage());
          dmd.write();
        } else {
          // rename scenario
          DumpMetaData dmd = new DumpMetaData(evRoot, DUMPTYPE.EVENT_RENAME_TABLE, evid, evid);
          dmd.setPayload(ev.getMessage());
          dmd.write();
        }

        break;
      }
      case MessageFactory.ALTER_PARTITION_EVENT : {
        LOG.info("Processing#{} ALTER_PARTITION message : {}", ev.getEventId(), ev.getMessage());
        AlterPartitionMessage apm = md.getAlterPartitionMessage(ev.getMessage());
        org.apache.hadoop.hive.metastore.api.Table tblObj = apm.getTableObj();
        org.apache.hadoop.hive.metastore.api.Partition pobjBefore = apm.getPtnObjBefore();
        org.apache.hadoop.hive.metastore.api.Partition pobjAfter = apm.getPtnObjAfter();

        boolean renameScenario = false;
        Iterator<String> beforeValIter = pobjBefore.getValuesIterator();
        Iterator<String> afterValIter = pobjAfter.getValuesIterator();
        for ( ; beforeValIter.hasNext() ; ){
          if (!beforeValIter.next().equals(afterValIter.next())){
            renameScenario = true;
            break;
          }
        }

        if (!renameScenario){
          // regular partition alter
          replicationSpec.setIsMetadataOnly(true);
          Table qlMdTable = new Table(tblObj);
          List<Partition> qlPtns = new ArrayList<Partition>();
          qlPtns.add(new Partition(qlMdTable, pobjAfter));
          Path metaDataPath = new Path(evRoot, EximUtil.METADATA_NAME);
          EximUtil.createExportDump(
              metaDataPath.getFileSystem(conf),
              metaDataPath,
              qlMdTable,
              qlPtns,
              replicationSpec);
          DumpMetaData dmd = new DumpMetaData(evRoot, DUMPTYPE.EVENT_ALTER_PARTITION, evid, evid);
          dmd.setPayload(ev.getMessage());
          dmd.write();
          break;
        } else {
          // rename scenario
          DumpMetaData dmd = new DumpMetaData(evRoot, DUMPTYPE.EVENT_RENAME_PARTITION, evid, evid);
          dmd.setPayload(ev.getMessage());
          dmd.write();
          break;
        }

      }
      // TODO : handle other event types
      default:
        LOG.info("Dummy processing#{} message : {}", ev.getEventId(), ev.getMessage());
        DumpMetaData dmd = new DumpMetaData(evRoot, DUMPTYPE.EVENT_UNKNOWN, evid, evid);
        dmd.setPayload(ev.getMessage());
        dmd.write();
        break;
    }

  }

  public static void injectNextDumpDirForTest(String dumpdir){
    testInjectDumpDir = dumpdir;
  }

  String getNextDumpDir() {
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
      Database dbObj = db.getDatabase(dbName);
      EximUtil.createDbExportDump(fs, dumpPath, dbObj, getNewReplicationSpec());
    } catch (Exception e) {
      // TODO : simple wrap & rethrow for now, clean up with error codes
      throw new SemanticException(e);
    }
    return dbRoot;
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

      DumpMetaData dmd = new DumpMetaData(loadPath);

      boolean evDump = false;
      if (dmd.isIncrementalDump()){
        LOG.debug("{} contains an incremental dump", loadPath);
        evDump = true;
      } else {
        LOG.debug("{} contains an bootstrap dump", loadPath);
      }

      if ((!evDump) && (tblNameOrPattern != null) && !(tblNameOrPattern.isEmpty())) {
        // not an event dump, and table name pattern specified, this has to be a tbl-level dump
        rootTasks.addAll(analyzeTableLoad(dbNameOrPattern, tblNameOrPattern, path, null));
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
        Task<? extends Serializable> evTaskRoot = TaskFactory.get(new DependencyCollectionWork(), conf);
        Task<? extends Serializable> taskChainTail = evTaskRoot;
        int evstage = 0;
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

          List<Task<? extends Serializable>> evTasks = analyzeEventLoad(
              dbNameOrPattern, tblNameOrPattern, dir.getPath().toUri().toString(), taskChainTail);
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
          }
        }
        // TODO : Over here, we need to track a Map<dbName:String,evLast:Long> for every db updated
        // and update repl.last.id for each, if this is a wh-level load, and if it is a db-level load,
        // then a single repl.last.id update, and if this is a tbl-lvl load which does not alter the
        // table itself, we'll need to update repl.last.id for that as well.
        LOG.debug("added evTaskRoot {}:{}", evTaskRoot.getClass(), evTaskRoot.getId());
        rootTasks.add(evTaskRoot);
      }

    } catch (Exception e) {
      // TODO : simple wrap & rethrow for now, clean up with error codes
      throw new SemanticException(e);
    }

  }

  private List<Task<? extends Serializable>> analyzeEventLoad(
      String dbName, String tblName, String locn,
      Task<? extends Serializable> precursor) throws SemanticException {
    // Currently handles only create-tbl & insert-ptn, since only those are dumped
    // As we add more event types, this will expand.
    DumpMetaData dmd = new DumpMetaData(new Path(locn));
    MessageDeserializer md = MessageFactory.getInstance().getDeserializer();
    switch (dmd.getDumpType()) {
      case EVENT_CREATE_TABLE: {
        return analyzeTableLoad(dbName, tblName, locn, precursor);
      }
      case EVENT_ADD_PARTITION: {
        return analyzeTableLoad(dbName, tblName, locn, precursor);
      }
      case EVENT_DROP_TABLE: {
        DropTableMessage dropTableMessage = md.getDropTableMessage(dmd.getPayload());
        DropTableDesc dropTableDesc = new DropTableDesc(
            dbName + "." + (tblName == null ? dropTableMessage.getTable() : tblName),
            null, true, true, getNewEventOnlyReplicationSpec(String.valueOf(dmd.getEventFrom())));
        Task<DDLWork> dropTableTask = TaskFactory.get(new DDLWork(inputs, outputs, dropTableDesc), conf);
        if (precursor != null){
          precursor.addDependentTask(dropTableTask);
        }
        List<Task<? extends Serializable>> tasks = new ArrayList<Task<? extends Serializable>>();
        tasks.add(dropTableTask);
        LOG.debug("Added drop tbl task : {}:{}", dropTableTask.getId(), dropTableDesc.getTableName());
        return tasks;
      }
      case EVENT_DROP_PARTITION: {
        DropPartitionMessage dropPartitionMessage = md.getDropPartitionMessage(dmd.getPayload());
        Map<Integer, List<ExprNodeGenericFuncDesc>> partSpecs = genPartSpecs(dropPartitionMessage.getPartitions());
        if (partSpecs.size() > 0){
          DropTableDesc dropPtnDesc = new DropTableDesc(
              dbName + "." + (tblName == null ? dropPartitionMessage.getTable() : tblName), partSpecs,
              null, true, getNewEventOnlyReplicationSpec(String.valueOf(dmd.getEventFrom())));
          Task<DDLWork> dropPtnTask = TaskFactory.get(new DDLWork(inputs, outputs, dropPtnDesc), conf);
          if (precursor != null){
            precursor.addDependentTask(dropPtnTask);
          }
          List<Task<? extends Serializable>> tasks = new ArrayList<Task<? extends Serializable>>();
          tasks.add(dropPtnTask);
          LOG.debug("Added drop ptn task : {}:{},{}",
              dropPtnTask.getId(), dropPtnDesc.getTableName(), dropPartitionMessage.getPartitions());
          return tasks;
        } else {
          throw new SemanticException("DROP PARTITION EVENT does not return any part descs for event message :"+dmd.getPayload());
        }
      }
      case EVENT_ALTER_TABLE: {
        return analyzeTableLoad(dbName, tblName, locn, precursor);
      }
      case EVENT_RENAME_TABLE: {
        AlterTableMessage renameTableMessage = md.getAlterTableMessage(dmd.getPayload());
        if ((tblName != null) && (!tblName.isEmpty())){
          throw new SemanticException("RENAMES of tables are not supported for table-level replication");
        }
        try {
          String oldDbName = renameTableMessage.getTableObjBefore().getDbName();
          String newDbName = renameTableMessage.getTableObjAfter().getDbName();

          if ((dbName != null) && (!dbName.isEmpty())){
            // If we're loading into a db, instead of into the warehouse, then the oldDbName and
            // newDbName must be the same
            if (!oldDbName.equalsIgnoreCase(newDbName)){
              throw new SemanticException("Cannot replicate an event renaming a table across"
                  + " databases into a db level load " + oldDbName +"->" + newDbName);
            } else {
              // both were the same, and can be replaced by the new db we're loading into.
              oldDbName = dbName;
              newDbName = dbName;
            }
          }

          String oldName = oldDbName + "." + renameTableMessage.getTableObjBefore().getTableName();
          String newName = newDbName + "." + renameTableMessage.getTableObjAfter().getTableName();
          AlterTableDesc renameTableDesc = new AlterTableDesc(oldName, newName, false);
          Task<DDLWork> renameTableTask = TaskFactory.get(new DDLWork(inputs, outputs, renameTableDesc), conf);
          if (precursor != null){
            precursor.addDependentTask(renameTableTask);
          }
          List<Task<? extends Serializable>> tasks = new ArrayList<Task<? extends Serializable>>();
          tasks.add(renameTableTask);
          LOG.debug("Added rename table task : {}:{}->{}", renameTableTask.getId(), oldName, newName);
          return tasks;
        } catch (Exception e) {
          if (!(e instanceof SemanticException)){
            throw new SemanticException("Error reading message members", e);
          } else {
            throw (SemanticException)e;
          }
        }
      }
      case EVENT_ALTER_PARTITION: {
        return analyzeTableLoad(dbName, tblName, locn, precursor);
      }
      case EVENT_RENAME_PARTITION: {
        AlterPartitionMessage renamePtnMessage = md.getAlterPartitionMessage(dmd.getPayload());

        Map<String, String> newPartSpec = new LinkedHashMap<String,String>();
        Map<String, String> oldPartSpec = new LinkedHashMap<String,String>();
        String tableName = dbName + "." +
            ((tblName == null || tblName.isEmpty()) ? renamePtnMessage.getTable() : tblName);
        try {
          org.apache.hadoop.hive.metastore.api.Table tblObj = renamePtnMessage.getTableObj();
          org.apache.hadoop.hive.metastore.api.Partition pobjBefore = renamePtnMessage.getPtnObjBefore();
          org.apache.hadoop.hive.metastore.api.Partition pobjAfter = renamePtnMessage.getPtnObjAfter();
          Iterator<String> beforeValIter = pobjBefore.getValuesIterator();
          Iterator<String> afterValIter = pobjAfter.getValuesIterator();
          for (FieldSchema fs : tblObj.getPartitionKeys()){
            oldPartSpec.put(fs.getName(), beforeValIter.next());
            newPartSpec.put(fs.getName(), afterValIter.next());
          }
        } catch (Exception e) {
          if (!(e instanceof SemanticException)){
            throw new SemanticException("Error reading message members", e);
          } else {
            throw (SemanticException)e;
          }
        }

        RenamePartitionDesc renamePtnDesc = new RenamePartitionDesc(tableName, oldPartSpec, newPartSpec);
        Task<DDLWork> renamePtnTask = TaskFactory.get(new DDLWork(inputs, outputs, renamePtnDesc), conf);
        if (precursor != null){
          precursor.addDependentTask(renamePtnTask);
        }
        List<Task<? extends Serializable>> tasks = new ArrayList<Task<? extends Serializable>>();
        tasks.add(renamePtnTask);
        LOG.debug("Added rename ptn task : {}:{}->{}", renamePtnTask.getId(), oldPartSpec, newPartSpec);
        return tasks;
      }
      case EVENT_UNKNOWN: {
        break;
      }
      default: {
        break;
      }
    }
    return null;
  }

  private Map<Integer, List<ExprNodeGenericFuncDesc>> genPartSpecs(List<Map<String, String>> partitions) throws SemanticException {
    Map<Integer,List<ExprNodeGenericFuncDesc>> partSpecs = new HashMap<Integer,List<ExprNodeGenericFuncDesc>>();

    int partPrefixLength = 0;
    if ((partitions != null) && (partitions.size() > 0)) {
      partPrefixLength = partitions.get(0).size();
      // pick the length of the first ptn, we expect all ptns listed to have the same number of key-vals.
    }
    List<ExprNodeGenericFuncDesc> ptnDescs = new ArrayList<ExprNodeGenericFuncDesc>();
    for (Map<String,String> ptn : partitions) {
      // convert each key-value-map to appropriate expression.

      ExprNodeGenericFuncDesc expr = null;
      for (Map.Entry<String,String> kvp : ptn.entrySet()) {
        String key = kvp.getKey();
        Object val = kvp.getValue();
        // FIXME : bug here, value is being placed as a String, but should actually be the underlying type
        // as converted to it by looking at the table's col schema. To do that, however, we need the
        // tableObjJson from the DropTableMessage. So, currently, this will only work for partitions for
        // which the partition keys are all strings. So, for now, we hardcode it, but we need to fix this.
        String type = "string";

        PrimitiveTypeInfo pti = TypeInfoFactory.getPrimitiveTypeInfo(type);
        ExprNodeColumnDesc column = new ExprNodeColumnDesc(pti, key, null, true);
        ExprNodeGenericFuncDesc op = DDLSemanticAnalyzer.makeBinaryPredicate("=", column, new ExprNodeConstantDesc(pti, val));
        expr = (expr == null) ? op : DDLSemanticAnalyzer.makeBinaryPredicate("and", expr, op);
      }
      if (expr != null) {
        ptnDescs.add(expr);
      }
    }
    if (ptnDescs.size() > 0){
      partSpecs.put(partPrefixLength, ptnDescs);
    }
    return partSpecs;
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

      EximUtil.ReadMetaData rv = new EximUtil.ReadMetaData();
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

      CreateDatabaseDesc createDbDesc = new CreateDatabaseDesc();
      createDbDesc.setName(dbName);
      createDbDesc.setComment(dbObj.getDescription());
      createDbDesc.setDatabaseProperties(dbObj.getParameters());
      // note that we do not set location - for repl load, we want that auto-created.

      createDbDesc.setIfNotExists(false);
      // If it exists, we want this to be an error condition. Repl Load is not intended to replace a
      // db.
      // TODO: we might revisit this in create-drop-recreate cases, needs some thinking on.
      Task<? extends Serializable> createDbTask = TaskFactory.get(new DDLWork(inputs, outputs, createDbDesc), conf);
      rootTasks.add(createDbTask);

      FileStatus[] dirsInDbPath = fs.listStatus(dir.getPath(), EximUtil.getDirectoryFilter(fs));

      for (FileStatus tableDir : dirsInDbPath) {
        analyzeTableLoad(
            dbName, null, tableDir.getPath().toUri().toString(), createDbTask);
      }
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  private List<Task<? extends Serializable>> analyzeTableLoad(
      String dbName, String tblName, String locn,
      Task<? extends Serializable> precursor) throws SemanticException {
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
          (precursor != null), parsedLocation, tblName, dbName, parsedPartSpec, locn, x);

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
    LOG.debug("ReplicationSemanticAnalyzer.analyzeReplStatus: writing repl.last.id={} out to {}",
        String.valueOf(replLastId), ctx.getResFile());
  }

  private void prepareReturnValues(List<String> values, String schema) throws SemanticException {
    LOG.debug("prepareReturnValues : " + schema);
    for (String s : values) {
      LOG.debug("    > " + s);
    }
    ctx.setResFile(ctx.getLocalTmpPath());
    writeOutput(values, ctx.getResFile());
  }

  private void writeOutput(List<String> values, Path outputFile) throws SemanticException {
    FileSystem fs = null;
    DataOutputStream outStream = null;
    try {
      fs = outputFile.getFileSystem(conf);
      outStream = fs.create(outputFile);
      outStream.writeBytes((values.get(0) == null ? Utilities.nullStringOutput : values.get(0)));
      for (int i = 1; i < values.size(); i++) {
        outStream.write(Utilities.tabCode);
        outStream.writeBytes((values.get(i) == null ? Utilities.nullStringOutput : values.get(i)));
      }
      outStream.write(Utilities.newLineCode);
    } catch (IOException e) {
      throw new SemanticException(e);
    } finally {
      IOUtils.closeStream(outStream);
    }
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
    return new ReplicationSpec(true, false, evState, objState, false, true);
  }

  // Use for replication states focussed on event only, where the obj state will be the event state
  private ReplicationSpec getNewEventOnlyReplicationSpec(String evState) throws SemanticException {
    return getNewReplicationSpec(evState, evState);
  }

  private Iterable<? extends String> matchesTbl(String dbName, String tblPattern)
      throws HiveException {
    if (tblPattern == null) {
      return db.getAllTables(dbName);
    } else {
      return db.getTablesByPattern(dbName, tblPattern);
    }
  }

  private Iterable<? extends String> matchesDb(String dbPattern) throws HiveException {
    if (dbPattern == null) {
      return db.getAllDatabases();
    } else {
      return db.getDatabasesByPattern(dbPattern);
    }
  }

}
