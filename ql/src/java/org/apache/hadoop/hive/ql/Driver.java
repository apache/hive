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

package org.apache.hadoop.hive.ql;

import java.io.DataInput;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.ExecDriver;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.TaskResult;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.history.HiveHistory.Keys;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.Hook;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.PostExecute;
import org.apache.hadoop.hive.ql.hooks.PreExecute;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.lockmgr.HiveLock;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockManager;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockManagerCtx;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockMode;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObj;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObject;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObject.HiveLockObjectData;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.DummyPartition;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContextImpl;
import org.apache.hadoop.hive.ql.parse.ImportSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.VariableSubstitution;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

public class Driver implements CommandProcessor {

  static final private Log LOG = LogFactory.getLog(Driver.class.getName());
  static final private LogHelper console = new LogHelper(LOG);

  private int maxRows = 100;
  ByteStream.Output bos = new ByteStream.Output();

  private HiveConf conf;
  private DataInput resStream;
  private Context ctx;
  private QueryPlan plan;
  private Schema schema;
  private HiveLockManager hiveLockMgr;

  private String errorMessage;
  private String SQLState;

  // A limit on the number of threads that can be launched
  private int maxthreads;
  private static final int SLEEP_TIME = 2000;
  protected int tryCount = Integer.MAX_VALUE;

  private boolean checkLockManager() {
    boolean supportConcurrency = conf.getBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY);
    if (!supportConcurrency) {
      return false;
    }
    if ((hiveLockMgr == null)) {
      try {
        setLockManager();
      } catch (SemanticException e) {
        errorMessage = "FAILED: Error in semantic analysis: " + e.getMessage();
        SQLState = ErrorMsg.findSQLState(e.getMessage());
        console.printError(errorMessage, "\n"
            + org.apache.hadoop.util.StringUtils.stringifyException(e));
        return false;
      }
    }
    // the reason that we set the lock manager for the cxt here is because each
    // query has its own ctx object. The hiveLockMgr is shared accross the
    // same instance of Driver, which can run multiple queries.
    ctx.setHiveLockMgr(hiveLockMgr);
    return hiveLockMgr != null;
  }

  private void setLockManager() throws SemanticException {
    boolean supportConcurrency = conf.getBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY);
    if (supportConcurrency) {
      String lockMgr = conf.getVar(HiveConf.ConfVars.HIVE_LOCK_MANAGER);
      if ((lockMgr == null) || (lockMgr.isEmpty())) {
        throw new SemanticException(ErrorMsg.LOCKMGR_NOT_SPECIFIED.getMsg());
      }

      try {
        hiveLockMgr = (HiveLockManager) ReflectionUtils.newInstance(conf.getClassByName(lockMgr),
            conf);
        hiveLockMgr.setContext(new HiveLockManagerCtx(conf));
      } catch (Exception e) {
        // set hiveLockMgr to null just in case this invalid manager got set to
        // next query's ctx.
        if (hiveLockMgr != null) {
          try {
            hiveLockMgr.close();
          } catch (LockException e1) {
            //nothing can do here
          }
          hiveLockMgr = null;
        }
        throw new SemanticException(ErrorMsg.LOCKMGR_NOT_INITIALIZED.getMsg() + e.getMessage());
      }
    }
  }

  public void init() {
    Operator.resetId();
  }

  /**
   * Return the status information about the Map-Reduce cluster
   */
  public ClusterStatus getClusterStatus() throws Exception {
    ClusterStatus cs;
    try {
      JobConf job = new JobConf(conf, ExecDriver.class);
      JobClient jc = new JobClient(job);
      cs = jc.getClusterStatus();
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    LOG.info("Returning cluster status: " + cs.toString());
    return cs;
  }


  public Schema getSchema() {
    return schema;
  }

  /**
   * Get a Schema with fields represented with native Hive types
   */
  public static Schema getSchema(BaseSemanticAnalyzer sem, HiveConf conf) {
    Schema schema = null;

    // If we have a plan, prefer its logical result schema if it's
    // available; otherwise, try digging out a fetch task; failing that,
    // give up.
    if (sem == null) {
      // can't get any info without a plan
    } else if (sem.getResultSchema() != null) {
      List<FieldSchema> lst = sem.getResultSchema();
      schema = new Schema(lst, null);
    } else if (sem.getFetchTask() != null) {
      FetchTask ft = sem.getFetchTask();
      TableDesc td = ft.getTblDesc();
      // partitioned tables don't have tableDesc set on the FetchTask. Instead
      // they have a list of PartitionDesc objects, each with a table desc.
      // Let's
      // try to fetch the desc for the first partition and use it's
      // deserializer.
      if (td == null && ft.getWork() != null && ft.getWork().getPartDesc() != null) {
        if (ft.getWork().getPartDesc().size() > 0) {
          td = ft.getWork().getPartDesc().get(0).getTableDesc();
        }
      }

      if (td == null) {
        LOG.info("No returning schema.");
      } else {
        String tableName = "result";
        List<FieldSchema> lst = null;
        try {
          lst = MetaStoreUtils.getFieldsFromDeserializer(tableName, td.getDeserializer());
        } catch (Exception e) {
          LOG.warn("Error getting schema: "
              + org.apache.hadoop.util.StringUtils.stringifyException(e));
        }
        if (lst != null) {
          schema = new Schema(lst, null);
        }
      }
    }
    if (schema == null) {
      schema = new Schema();
    }
    LOG.info("Returning Hive schema: " + schema);
    return schema;
  }

  /**
   * Get a Schema with fields represented with Thrift DDL types
   */
  public Schema getThriftSchema() throws Exception {
    Schema schema;
    try {
      schema = getSchema();
      if (schema != null) {
        List<FieldSchema> lst = schema.getFieldSchemas();
        // Go over the schema and convert type to thrift type
        if (lst != null) {
          for (FieldSchema f : lst) {
            f.setType(MetaStoreUtils.typeToThriftType(f.getType()));
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    LOG.info("Returning Thrift schema: " + schema);
    return schema;
  }

  /**
   * Return the maximum number of rows returned by getResults
   */
  public int getMaxRows() {
    return maxRows;
  }

  /**
   * Set the maximum number of rows returned by getResults
   */
  public void setMaxRows(int maxRows) {
    this.maxRows = maxRows;
  }

  public boolean hasReduceTasks(List<Task<? extends Serializable>> tasks) {
    if (tasks == null) {
      return false;
    }

    boolean hasReduce = false;
    for (Task<? extends Serializable> task : tasks) {
      if (task.hasReduce()) {
        return true;
      }

      hasReduce = (hasReduce || hasReduceTasks(task.getChildTasks()));
    }
    return hasReduce;
  }

  /**
   * for backwards compatibility with current tests
   */
  public Driver(HiveConf conf) {
    this.conf = conf;
  }

  public Driver() {
    if (SessionState.get() != null) {
      conf = SessionState.get().getConf();
    }
  }

  /**
   * Compile a new query. Any currently-planned query associated with this Driver is discarded.
   *
   * @param command
   *          The SQL query to compile.
   */
  public int compile(String command) {
    return compile(command, true);
  }

  /**
   * Hold state variables specific to each query being executed, that may not
   * be consistent in the overall SessionState
   */
  private static class QueryState {
    private HiveOperation op;
    private String cmd;
    private boolean init = false;

    /**
     * Initialize the queryState with the query state variables
     */
    public void init(HiveOperation op, String cmd) {
      this.op = op;
      this.cmd = cmd;
      this.init = true;
    }

    public boolean isInitialized() {
      return this.init;
    }

    public HiveOperation getOp() {
      return this.op;
    }

    public String getCmd() {
      return this.cmd;
    }
  }

  public void saveSession(QueryState qs) {
    SessionState oldss = SessionState.get();
    if (oldss != null && oldss.getHiveOperation() != null) {
      qs.init(oldss.getHiveOperation(), oldss.getCmd());
    }
  }

  public void restoreSession(QueryState qs) {
    SessionState ss = SessionState.get();
    if (ss != null && qs != null && qs.isInitialized()) {
      ss.setCmd(qs.getCmd());
      ss.setCommandType(qs.getOp());
    }
  }

  /**
   * Compile a new query, but potentially reset taskID counter.  Not resetting task counter
   * is useful for generating re-entrant QL queries.
   * @param command  The HiveQL query to compile
   * @param resetTaskIds Resets taskID counter if true.
   * @return
   */
  public int compile(String command, boolean resetTaskIds) {
    PerfLogger perfLogger = PerfLogger.getPerfLogger();
    perfLogger.PerfLogBegin(LOG, PerfLogger.COMPILE);

    //holder for parent command type/string when executing reentrant queries
    QueryState queryState = new QueryState();

    if (plan != null) {
      close();
      plan = null;
    }

    if (resetTaskIds) {
      TaskFactory.resetId();
    }
    saveSession(queryState);

    try {
      command = new VariableSubstitution().substitute(conf,command);
      ctx = new Context(conf);
      ctx.setTryCount(getTryCount());
      ctx.setCmd(command);

      ParseDriver pd = new ParseDriver();
      ASTNode tree = pd.parse(command, ctx);
      tree = ParseUtils.findRootNonNullToken(tree);

      BaseSemanticAnalyzer sem = SemanticAnalyzerFactory.get(conf, tree);
      List<AbstractSemanticAnalyzerHook> saHooks = getSemanticAnalyzerHooks();

      // Do semantic analysis and plan generation
      if (saHooks != null) {
        HiveSemanticAnalyzerHookContext hookCtx = new HiveSemanticAnalyzerHookContextImpl();
        hookCtx.setConf(conf);
        for (AbstractSemanticAnalyzerHook hook : saHooks) {
          tree = hook.preAnalyze(hookCtx, tree);
        }
        sem.analyze(tree, ctx);
        for (AbstractSemanticAnalyzerHook hook : saHooks) {
          hook.postAnalyze(hookCtx, sem.getRootTasks());
        }
      } else {
        sem.analyze(tree, ctx);
      }

      LOG.info("Semantic Analysis Completed");

      // validate the plan
      sem.validate();

      plan = new QueryPlan(command, sem);
      // initialize FetchTask right here
      if (plan.getFetchTask() != null) {
        plan.getFetchTask().initialize(conf, plan, null);
      }

      // get the output schema
      schema = getSchema(sem, conf);

      // test Only - serialize the query plan and deserialize it
      if ("true".equalsIgnoreCase(System.getProperty("test.serialize.qplan"))) {

        String queryPlanFileName = ctx.getLocalScratchDir(true) + Path.SEPARATOR_CHAR
            + "queryplan.xml";
        LOG.info("query plan = " + queryPlanFileName);
        queryPlanFileName = new Path(queryPlanFileName).toUri().getPath();

        // serialize the queryPlan
        FileOutputStream fos = new FileOutputStream(queryPlanFileName);
        Utilities.serializeQueryPlan(plan, fos);
        fos.close();

        // deserialize the queryPlan
        FileInputStream fis = new FileInputStream(queryPlanFileName);
        QueryPlan newPlan = Utilities.deserializeQueryPlan(fis, conf);
        fis.close();

        // Use the deserialized plan
        plan = newPlan;
      }

      // initialize FetchTask right here
      if (plan.getFetchTask() != null) {
        plan.getFetchTask().initialize(conf, plan, null);
      }

      //do the authorization check
      if (HiveConf.getBoolVar(conf,
          HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED)) {
        try {
          perfLogger.PerfLogBegin(LOG, PerfLogger.DO_AUTHORIZATION);
          doAuthorization(sem);
        } catch (AuthorizationException authExp) {
          console.printError("Authorization failed:" + authExp.getMessage()
              + ". Use show grant to get more details.");
          return 403;
        } finally {
          perfLogger.PerfLogEnd(LOG, PerfLogger.DO_AUTHORIZATION);
        }
      }

      //restore state after we're done executing a specific query

      return 0;
    } catch (SemanticException e) {
      errorMessage = "FAILED: Error in semantic analysis: " + e.getMessage();
      SQLState = ErrorMsg.findSQLState(e.getMessage());
      console.printError(errorMessage, "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return (10);
    } catch (ParseException e) {
      errorMessage = "FAILED: Parse Error: " + e.getMessage();
      SQLState = ErrorMsg.findSQLState(e.getMessage());
      console.printError(errorMessage, "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return (11);
    } catch (Exception e) {
      errorMessage = "FAILED: Hive Internal Error: " + Utilities.getNameMessage(e);
      SQLState = ErrorMsg.findSQLState(e.getMessage());
      console.printError(errorMessage + "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return (12);
    } finally {
      perfLogger.PerfLogEnd(LOG, PerfLogger.COMPILE);
      restoreSession(queryState);
    }
  }

  private void doAuthorization(BaseSemanticAnalyzer sem)
      throws HiveException, AuthorizationException {
    HashSet<ReadEntity> inputs = sem.getInputs();
    HashSet<WriteEntity> outputs = sem.getOutputs();
    SessionState ss = SessionState.get();
    HiveOperation op = ss.getHiveOperation();
    Hive db = sem.getDb();
    if (op != null) {
      if (op.equals(HiveOperation.CREATETABLE_AS_SELECT)
          || op.equals(HiveOperation.CREATETABLE)) {
        ss.getAuthorizer().authorize(
            db.getDatabase(db.getCurrentDatabase()), null,
            HiveOperation.CREATETABLE_AS_SELECT.getOutputRequiredPrivileges());
      } else {
        if (op.equals(HiveOperation.IMPORT)) {
          ImportSemanticAnalyzer isa = (ImportSemanticAnalyzer) sem;
          if (!isa.existsTable()) {
            ss.getAuthorizer().authorize(
                db.getDatabase(db.getCurrentDatabase()), null,
                HiveOperation.CREATETABLE_AS_SELECT.getOutputRequiredPrivileges());
          }
        }
      }
      if (outputs != null && outputs.size() > 0) {
        for (WriteEntity write : outputs) {

          if (write.getType() == WriteEntity.Type.PARTITION) {
            Partition part = db.getPartition(write.getTable(), write
                .getPartition().getSpec(), false);
            if (part != null) {
              ss.getAuthorizer().authorize(write.getPartition(), null,
                      op.getOutputRequiredPrivileges());
              continue;
            }
          }

          if (write.getTable() != null) {
            ss.getAuthorizer().authorize(write.getTable(), null,
                    op.getOutputRequiredPrivileges());
          }
        }

      }
    }

    if (inputs != null && inputs.size() > 0) {

      Map<Table, List<String>> tab2Cols = new HashMap<Table, List<String>>();
      Map<Partition, List<String>> part2Cols = new HashMap<Partition, List<String>>();

      Map<String, Boolean> tableUsePartLevelAuth = new HashMap<String, Boolean>();
      for (ReadEntity read : inputs) {
        if (read.getPartition() != null) {
          Table tbl = read.getTable();
          String tblName = tbl.getTableName();
          if (tableUsePartLevelAuth.get(tblName) == null) {
            boolean usePartLevelPriv = (tbl.getParameters().get(
                "PARTITION_LEVEL_PRIVILEGE") != null && ("TRUE"
                .equalsIgnoreCase(tbl.getParameters().get(
                    "PARTITION_LEVEL_PRIVILEGE"))));
            if (usePartLevelPriv) {
              tableUsePartLevelAuth.put(tblName, Boolean.TRUE);
            } else {
              tableUsePartLevelAuth.put(tblName, Boolean.FALSE);
            }
          }
        }
      }

      if (op.equals(HiveOperation.CREATETABLE_AS_SELECT)
          || op.equals(HiveOperation.QUERY)) {
        SemanticAnalyzer querySem = (SemanticAnalyzer) sem;
        ParseContext parseCtx = querySem.getParseContext();
        Map<TableScanOperator, Table> tsoTopMap = parseCtx.getTopToTable();

        for (Map.Entry<String, Operator<? extends Serializable>> topOpMap : querySem
            .getParseContext().getTopOps().entrySet()) {
          Operator<? extends Serializable> topOp = topOpMap.getValue();
          if (topOp instanceof TableScanOperator
              && tsoTopMap.containsKey(topOp)) {
            TableScanOperator tableScanOp = (TableScanOperator) topOp;
            Table tbl = tsoTopMap.get(tableScanOp);
            List<Integer> neededColumnIds = tableScanOp.getNeededColumnIDs();
            List<FieldSchema> columns = tbl.getCols();
            List<String> cols = new ArrayList<String>();
            if (neededColumnIds != null && neededColumnIds.size() > 0) {
              for (int i = 0; i < neededColumnIds.size(); i++) {
                cols.add(columns.get(neededColumnIds.get(i)).getName());
              }
            } else {
              for (int i = 0; i < columns.size(); i++) {
                cols.add(columns.get(i).getName());
              }
            }
            if (tbl.isPartitioned() && tableUsePartLevelAuth.get(tbl.getTableName())) {
              String alias_id = topOpMap.getKey();
              PrunedPartitionList partsList = PartitionPruner.prune(parseCtx
                  .getTopToTable().get(topOp), parseCtx.getOpToPartPruner()
                  .get(topOp), parseCtx.getConf(), alias_id, parseCtx
                  .getPrunedPartitions());
              Set<Partition> parts = new HashSet<Partition>();
              parts.addAll(partsList.getConfirmedPartns());
              parts.addAll(partsList.getUnknownPartns());
              for (Partition part : parts) {
                List<String> existingCols = part2Cols.get(part);
                if (existingCols == null) {
                  existingCols = new ArrayList<String>();
                }
                existingCols.addAll(cols);
                part2Cols.put(part, existingCols);
              }
            } else {
              List<String> existingCols = tab2Cols.get(tbl);
              if (existingCols == null) {
                existingCols = new ArrayList<String>();
              }
              existingCols.addAll(cols);
              tab2Cols.put(tbl, existingCols);
            }
          }
        }
      }


      //cache the results for table authorization
      Set<String> tableAuthChecked = new HashSet<String>();
      for (ReadEntity read : inputs) {
        Table tbl = null;
        if (read.getPartition() != null) {
          tbl = read.getPartition().getTable();
          // use partition level authorization
          if (tableUsePartLevelAuth.get(tbl.getTableName())) {
            List<String> cols = part2Cols.get(read.getPartition());
            if (cols != null && cols.size() > 0) {
              ss.getAuthorizer().authorize(read.getPartition().getTable(),
                  read.getPartition(), cols, op.getInputRequiredPrivileges(),
                  null);
            } else {
              ss.getAuthorizer().authorize(read.getPartition(),
                  op.getInputRequiredPrivileges(), null);
            }
            continue;
          }
        } else if (read.getTable() != null) {
          tbl = read.getTable();
        }

        // if we reach here, it means it needs to do a table authorization
        // check, and the table authorization may already happened because of other
        // partitions
        if (tbl != null && !tableAuthChecked.contains(tbl.getTableName())) {
          List<String> cols = tab2Cols.get(tbl);
          if (cols != null && cols.size() > 0) {
            ss.getAuthorizer().authorize(tbl, null, cols,
                op.getInputRequiredPrivileges(), null);
          } else {
            ss.getAuthorizer().authorize(tbl, op.getInputRequiredPrivileges(),
                null);
          }
          tableAuthChecked.add(tbl.getTableName());
        }
      }

    }
  }

  /**
   * @return The current query plan associated with this Driver, if any.
   */
  public QueryPlan getPlan() {
    return plan;
  }

  /**
   * @param t
   *          The table to be locked
   * @param p
   *          The partition to be locked
   * @param mode
   *          The mode of the lock (SHARED/EXCLUSIVE) Get the list of objects to be locked. If a
   *          partition needs to be locked (in any mode), all its parents should also be locked in
   *          SHARED mode.
   **/
  private List<HiveLockObj> getLockObjects(Table t, Partition p, HiveLockMode mode)
      throws SemanticException {
    List<HiveLockObj> locks = new LinkedList<HiveLockObj>();

    HiveLockObjectData lockData =
      new HiveLockObjectData(plan.getQueryId(),
                             String.valueOf(System.currentTimeMillis()),
                             "IMPLICIT",
                             plan.getQueryStr());

    if (t != null) {
      locks.add(new HiveLockObj(new HiveLockObject(t, lockData), mode));
      mode = HiveLockMode.SHARED;
      locks.add(new HiveLockObj(new HiveLockObject(t.getDbName(), lockData), mode));
      return locks;
    }

    if (p != null) {
      if (!(p instanceof DummyPartition)) {
        locks.add(new HiveLockObj(new HiveLockObject(p, lockData), mode));
      }

      // All the parents are locked in shared mode
      mode = HiveLockMode.SHARED;

      // For dummy partitions, only partition name is needed
      String name = p.getName();

      if (p instanceof DummyPartition) {
        name = p.getName().split("@")[2];
      }

      String partName = name;
      String partialName = "";
      String[] partns = name.split("/");
      int len = p instanceof DummyPartition ? partns.length : partns.length - 1;
      for (int idx = 0; idx < len; idx++) {
        String partn = partns[idx];
        partialName += partn;
        try {
          locks.add(new HiveLockObj(
                      new HiveLockObject(new DummyPartition(p.getTable(), p.getTable().getDbName()
                                                            + "/" + p.getTable().getTableName()
                                                            + "/" + partialName), lockData), mode));
          partialName += "/";
        } catch (HiveException e) {
          throw new SemanticException(e.getMessage());
        }
      }

      locks.add(new HiveLockObj(new HiveLockObject(p.getTable(), lockData), mode));
      locks.add(new HiveLockObj(new HiveLockObject(p.getTable().getDbName(), lockData), mode));
    }
    return locks;
  }

  /**
   * Acquire read and write locks needed by the statement. The list of objects to be locked are
   * obtained from he inputs and outputs populated by the compiler. The lock acuisition scheme is
   * pretty simple. If all the locks cannot be obtained, error out. Deadlock is avoided by making
   * sure that the locks are lexicographically sorted.
   **/
  public int acquireReadWriteLocks() {
    PerfLogger perfLogger = PerfLogger.getPerfLogger();
    perfLogger.PerfLogBegin(LOG, PerfLogger.ACQUIRE_READ_WRITE_LOCKS);

    try {
      int sleepTime = conf.getIntVar(HiveConf.ConfVars.HIVE_LOCK_SLEEP_BETWEEN_RETRIES) * 1000;
      int numRetries = conf.getIntVar(HiveConf.ConfVars.HIVE_LOCK_NUMRETRIES);

      boolean supportConcurrency = conf.getBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY);
      if (!supportConcurrency) {
        return 0;
      }

      List<HiveLockObj> lockObjects = new ArrayList<HiveLockObj>();

      // Sort all the inputs, outputs.
      // If a lock needs to be acquired on any partition, a read lock needs to be acquired on all
      // its parents also
      for (ReadEntity input : plan.getInputs()) {
        if (input.getType() == ReadEntity.Type.TABLE) {
          lockObjects.addAll(getLockObjects(input.getTable(), null, HiveLockMode.SHARED));
        } else {
          lockObjects.addAll(getLockObjects(null, input.getPartition(), HiveLockMode.SHARED));
        }
      }

      for (WriteEntity output : plan.getOutputs()) {
        if (output.getTyp() == WriteEntity.Type.TABLE) {
          lockObjects.addAll(getLockObjects(output.getTable(), null,
              output.isComplete() ? HiveLockMode.EXCLUSIVE : HiveLockMode.SHARED));
        } else if (output.getTyp() == WriteEntity.Type.PARTITION) {
          lockObjects.addAll(getLockObjects(null, output.getPartition(), HiveLockMode.EXCLUSIVE));
        }
        // In case of dynamic queries, it is possible to have incomplete dummy partitions
        else if (output.getTyp() == WriteEntity.Type.DUMMYPARTITION) {
          lockObjects.addAll(getLockObjects(null, output.getPartition(), HiveLockMode.SHARED));
        }
      }

      if (lockObjects.isEmpty() && !ctx.isNeedLockMgr()) {
        return 0;
      }

      HiveLockObjectData lockData =
        new HiveLockObjectData(plan.getQueryId(),
                               String.valueOf(System.currentTimeMillis()),
                               "IMPLICIT",
                               plan.getQueryStr());

      // Lock the database also
      try {
        Hive db = Hive.get(conf);
        lockObjects.add(new HiveLockObj(
                                        new HiveLockObject(db.getCurrentDatabase(), lockData),
                                        HiveLockMode.SHARED));
      } catch (HiveException e) {
        throw new SemanticException(e.getMessage());
      }

      List<HiveLock> hiveLocks = null;

      int tryNum = 1;
      do {

        ctx.getHiveLockMgr().prepareRetry();
        hiveLocks = ctx.getHiveLockMgr().lock(lockObjects, false);

        if (hiveLocks != null) {
          break;
        }

        tryNum++;
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
        }
      } while (tryNum < numRetries);

      if (hiveLocks == null) {
        throw new SemanticException(ErrorMsg.LOCK_CANNOT_BE_ACQUIRED.getMsg());
      } else {
        ctx.setHiveLocks(hiveLocks);
      }

      return (0);
    } catch (SemanticException e) {
      errorMessage = "FAILED: Error in acquiring locks: " + e.getMessage();
      SQLState = ErrorMsg.findSQLState(e.getMessage());
      console.printError(errorMessage, "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return (10);
    } catch (LockException e) {
      errorMessage = "FAILED: Error in acquiring locks: " + e.getMessage();
      SQLState = ErrorMsg.findSQLState(e.getMessage());
      console.printError(errorMessage, "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return (10);
    } finally {
      perfLogger.PerfLogEnd(LOG, PerfLogger.ACQUIRE_READ_WRITE_LOCKS);
    }
  }

  /**
   * Release all the locks acquired implicitly by the statement. Note that the locks acquired with
   * 'keepAlive' set to True are not released.
   **/
  private void releaseLocks() {
    if (ctx != null && ctx.getHiveLockMgr() != null) {
      try {
        ctx.getHiveLockMgr().close();
        ctx.setHiveLocks(null);
      } catch (LockException e) {
      }
    }
  }

  /**
   * @param hiveLocks
   *          list of hive locks to be released Release all the locks specified. If some of the
   *          locks have already been released, ignore them
   **/
  private void releaseLocks(List<HiveLock> hiveLocks) {
    PerfLogger perfLogger = PerfLogger.getPerfLogger();
    perfLogger.PerfLogBegin(LOG, PerfLogger.RELEASE_LOCKS);

    if (hiveLocks != null) {
      ctx.getHiveLockMgr().releaseLocks(hiveLocks);
    }
    ctx.setHiveLocks(null);

    perfLogger.PerfLogEnd(LOG, PerfLogger.RELEASE_LOCKS);
  }

  public CommandProcessorResponse run(String command) throws CommandNeedRetryException {
    errorMessage = null;
    SQLState = null;
    // Reset the perf logger
    PerfLogger.getPerfLogger(true);

    int ret = compile(command);
    if (ret != 0) {
      releaseLocks(ctx.getHiveLocks());
      return new CommandProcessorResponse(ret, errorMessage, SQLState);
    }

    boolean requireLock = false;
    boolean ckLock = checkLockManager();

    if (ckLock) {
      boolean lockOnlyMapred = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_LOCK_MAPRED_ONLY);
      if(lockOnlyMapred) {
        Queue<Task<? extends Serializable>> taskQueue = new LinkedList<Task<? extends Serializable>>();
        taskQueue.addAll(plan.getRootTasks());
        while (taskQueue.peek() != null) {
          Task<? extends Serializable> tsk = taskQueue.remove();
          requireLock = requireLock || tsk.requireLock();
          if(requireLock) {
            break;
          }
          if (tsk instanceof ConditionalTask) {
            taskQueue.addAll(((ConditionalTask)tsk).getListTasks());
          }
          if(tsk.getChildTasks()!= null) {
            taskQueue.addAll(tsk.getChildTasks());
          }
          // does not add back up task here, because back up task should be the same
          // type of the original task.
        }
      } else {
        requireLock = true;
      }
    }

    if (requireLock) {
      ret = acquireReadWriteLocks();
      if (ret != 0) {
        releaseLocks(ctx.getHiveLocks());
        return new CommandProcessorResponse(ret, errorMessage, SQLState);
      }
    }

    ret = execute();
    if (ret != 0) {
      //if needRequireLock is false, the release here will do nothing because there is no lock
      releaseLocks(ctx.getHiveLocks());
      return new CommandProcessorResponse(ret, errorMessage, SQLState);
    }

    //if needRequireLock is false, the release here will do nothing because there is no lock
    releaseLocks(ctx.getHiveLocks());
    return new CommandProcessorResponse(ret);
  }

  private List<AbstractSemanticAnalyzerHook> getSemanticAnalyzerHooks() throws Exception {
    ArrayList<AbstractSemanticAnalyzerHook> saHooks = new ArrayList<AbstractSemanticAnalyzerHook>();
    String pestr = conf.getVar(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK);
    if(pestr == null) {
      return saHooks;
    }
    pestr = pestr.trim();
    if (pestr.equals("")) {
      return saHooks;
    }

    String[] peClasses = pestr.split(",");

    for (String peClass : peClasses) {
      try {
        AbstractSemanticAnalyzerHook hook = HiveUtils.getSemanticAnalyzerHook(conf, peClass);
        saHooks.add(hook);
      } catch (HiveException e) {
        console.printError("Pre Exec Hook Class not found:" + e.getMessage());
        throw e;
      }
    }

    return saHooks;
  }


  private List<Hook> getPreExecHooks() throws Exception {
    List<Hook> pehooks = new ArrayList<Hook>();
    String pestr = conf.getVar(HiveConf.ConfVars.PREEXECHOOKS);
    pestr = pestr.trim();
    if (pestr.equals("")) {
      return pehooks;
    }

    String[] peClasses = pestr.split(",");

    for (String peClass : peClasses) {
      try {
        pehooks.add((Hook) Class.forName(peClass.trim(), true, JavaUtils.getClassLoader())
            .newInstance());
      } catch (ClassNotFoundException e) {
        console.printError("Pre Exec Hook Class not found:" + e.getMessage());
        throw e;
      }
    }

    return pehooks;
  }

  private List<Hook> getPostExecHooks() throws Exception {
    List<Hook> pehooks = new ArrayList<Hook>();
    String pestr = conf.getVar(HiveConf.ConfVars.POSTEXECHOOKS);
    pestr = pestr.trim();
    if (pestr.equals("")) {
      return pehooks;
    }

    String[] peClasses = pestr.split(",");

    for (String peClass : peClasses) {
      try {
        pehooks.add((Hook) Class.forName(peClass.trim(), true, JavaUtils.getClassLoader())
            .newInstance());
      } catch (ClassNotFoundException e) {
        console.printError("Post Exec Hook Class not found:" + e.getMessage());
        throw e;
      }
    }

    return pehooks;
  }

  private List<Hook> getOnFailureHooks() throws Exception {
    List<Hook> ofhooks = new ArrayList<Hook>();
    String ofstr = conf.getVar(HiveConf.ConfVars.ONFAILUREHOOKS);
    ofstr = ofstr.trim();
    if (ofstr.equals("")) {
      return ofhooks;
    }

    String[] ofClasses = ofstr.split(",");

    for (String ofClass : ofClasses) {
      try {
        ofhooks.add((Hook) Class.forName(ofClass.trim(), true, JavaUtils.getClassLoader())
            .newInstance());
      } catch (ClassNotFoundException e) {
        console.printError("On Failure Hook Class not found:" + e.getMessage());
        throw e;
      }
    }

    return ofhooks;
  }

  public int execute() throws CommandNeedRetryException {
    PerfLogger perfLogger = PerfLogger.getPerfLogger();
    perfLogger.PerfLogBegin(LOG, PerfLogger.DRIVER_EXECUTE);

    boolean noName = StringUtils.isEmpty(conf.getVar(HiveConf.ConfVars.HADOOPJOBNAME));
    int maxlen = conf.getIntVar(HiveConf.ConfVars.HIVEJOBNAMELENGTH);

    String queryId = plan.getQueryId();
    String queryStr = plan.getQueryStr();

    conf.setVar(HiveConf.ConfVars.HIVEQUERYID, queryId);
    conf.setVar(HiveConf.ConfVars.HIVEQUERYSTRING, queryStr);
    maxthreads = HiveConf.getIntVar(conf, HiveConf.ConfVars.EXECPARALLETHREADNUMBER);

    try {
      LOG.info("Starting command: " + queryStr);

      plan.setStarted();

      if (SessionState.get() != null) {
        SessionState.get().getHiveHistory().startQuery(queryStr,
            conf.getVar(HiveConf.ConfVars.HIVEQUERYID));
        SessionState.get().getHiveHistory().logPlanProgress(plan);
      }
      resStream = null;

      HookContext hookContext = new HookContext(plan, conf, ctx.getPathToCS());
      hookContext.setHookType(HookContext.HookType.PRE_EXEC_HOOK);

      for (Hook peh : getPreExecHooks()) {
        if (peh instanceof ExecuteWithHookContext) {
          perfLogger.PerfLogBegin(LOG, PerfLogger.PRE_HOOK + peh.getClass().getName());

          ((ExecuteWithHookContext) peh).run(hookContext);

          perfLogger.PerfLogEnd(LOG, PerfLogger.PRE_HOOK + peh.getClass().getName());
        } else if (peh instanceof PreExecute) {
          perfLogger.PerfLogBegin(LOG, PerfLogger.PRE_HOOK + peh.getClass().getName());

          ((PreExecute) peh).run(SessionState.get(), plan.getInputs(), plan.getOutputs(),
              ShimLoader.getHadoopShims().getUGIForConf(conf));

          perfLogger.PerfLogEnd(LOG, PerfLogger.PRE_HOOK + peh.getClass().getName());
        }
      }


      int jobs = Utilities.getMRTasks(plan.getRootTasks()).size();
      if (jobs > 0) {
        console.printInfo("Total MapReduce jobs = " + jobs);
      }
      if (SessionState.get() != null) {
        SessionState.get().getHiveHistory().setQueryProperty(queryId, Keys.QUERY_NUM_TASKS,
            String.valueOf(jobs));
        SessionState.get().getHiveHistory().setIdToTableMap(plan.getIdToTableNameMap());
      }
      String jobname = Utilities.abbreviate(queryStr, maxlen - 6);

      // A runtime that launches runnable tasks as separate Threads through
      // TaskRunners
      // As soon as a task isRunnable, it is put in a queue
      // At any time, at most maxthreads tasks can be running
      // The main thread polls the TaskRunners to check if they have finished.

      Queue<Task<? extends Serializable>> runnable = new ConcurrentLinkedQueue<Task<? extends Serializable>>();
      Map<TaskResult, TaskRunner> running = new HashMap<TaskResult, TaskRunner>();

      DriverContext driverCxt = new DriverContext(runnable, ctx);

      SessionState.get().setLastMapRedStatsList(new ArrayList<MapRedStats>());

      // Add root Tasks to runnable

      for (Task<? extends Serializable> tsk : plan.getRootTasks()) {
        driverCxt.addToRunnable(tsk);
      }

      // Loop while you either have tasks running, or tasks queued up

      while (running.size() != 0 || runnable.peek() != null) {
        // Launch upto maxthreads tasks
        while (runnable.peek() != null && running.size() < maxthreads) {
          Task<? extends Serializable> tsk = runnable.remove();
          launchTask(tsk, queryId, noName, running, jobname, jobs, driverCxt);
        }

        // poll the Tasks to see which one completed
        TaskResult tskRes = pollTasks(running.keySet());
        TaskRunner tskRun = running.remove(tskRes);
        Task<? extends Serializable> tsk = tskRun.getTask();
        hookContext.addCompleteTask(tskRun);

        int exitVal = tskRes.getExitVal();
        if (exitVal != 0) {
          if (tsk.ifRetryCmdWhenFail()) {
            if (running.size() != 0) {
              taskCleanup();
            }
            // in case we decided to run everything in local mode, restore the
            // the jobtracker setting to its initial value
            ctx.restoreOriginalTracker();
            throw new CommandNeedRetryException();
          }
          Task<? extends Serializable> backupTask = tsk.getAndInitBackupTask();
          if (backupTask != null) {
            errorMessage = "FAILED: Execution Error, return code " + exitVal + " from "
                + tsk.getClass().getName();
            console.printError(errorMessage);

            errorMessage = "ATTEMPT: Execute BackupTask: " + backupTask.getClass().getName();
            console.printError(errorMessage);

            // add backup task to runnable
            if (DriverContext.isLaunchable(backupTask)) {
              driverCxt.addToRunnable(backupTask);
            }
            continue;

          } else {
            hookContext.setHookType(HookContext.HookType.ON_FAILURE_HOOK);
            // Get all the failure execution hooks and execute them.
            for (Hook ofh : getOnFailureHooks()) {
              perfLogger.PerfLogBegin(LOG, PerfLogger.FAILURE_HOOK + ofh.getClass().getName());

              ((ExecuteWithHookContext) ofh).run(hookContext);

              perfLogger.PerfLogEnd(LOG, PerfLogger.FAILURE_HOOK + ofh.getClass().getName());
            }

            // TODO: This error messaging is not very informative. Fix that.
            errorMessage = "FAILED: Execution Error, return code " + exitVal + " from "
                + tsk.getClass().getName();
            SQLState = "08S01";
            console.printError(errorMessage);
            if (running.size() != 0) {
              taskCleanup();
            }
            // in case we decided to run everything in local mode, restore the
            // the jobtracker setting to its initial value
            ctx.restoreOriginalTracker();
            return 9;
          }
        }

        if (SessionState.get() != null) {
          SessionState.get().getHiveHistory().setTaskProperty(queryId, tsk.getId(),
              Keys.TASK_RET_CODE, String.valueOf(exitVal));
          SessionState.get().getHiveHistory().endTask(queryId, tsk);
        }

        if (tsk.getChildTasks() != null) {
          for (Task<? extends Serializable> child : tsk.getChildTasks()) {
            if (DriverContext.isLaunchable(child)) {
              driverCxt.addToRunnable(child);
            }
          }
        }
      }

      // in case we decided to run everything in local mode, restore the
      // the jobtracker setting to its initial value
      ctx.restoreOriginalTracker();

      // remove incomplete outputs.
      // Some incomplete outputs may be added at the beginning, for eg: for dynamic partitions.
      // remove them
      HashSet<WriteEntity> remOutputs = new HashSet<WriteEntity>();
      for (WriteEntity output : plan.getOutputs()) {
        if (!output.isComplete()) {
          remOutputs.add(output);
        }
      }

      for (WriteEntity output : remOutputs) {
        plan.getOutputs().remove(output);
      }

      hookContext.setHookType(HookContext.HookType.POST_EXEC_HOOK);
      // Get all the post execution hooks and execute them.
      for (Hook peh : getPostExecHooks()) {
        if (peh instanceof ExecuteWithHookContext) {
          perfLogger.PerfLogBegin(LOG, PerfLogger.POST_HOOK + peh.getClass().getName());

          ((ExecuteWithHookContext) peh).run(hookContext);

          perfLogger.PerfLogEnd(LOG, PerfLogger.POST_HOOK + peh.getClass().getName());
        } else if (peh instanceof PostExecute) {
          perfLogger.PerfLogBegin(LOG, PerfLogger.POST_HOOK + peh.getClass().getName());

          ((PostExecute) peh).run(SessionState.get(), plan.getInputs(), plan.getOutputs(),
              (SessionState.get() != null ? SessionState.get().getLineageState().getLineageInfo()
                  : null), ShimLoader.getHadoopShims().getUGIForConf(conf));

          perfLogger.PerfLogEnd(LOG, PerfLogger.POST_HOOK + peh.getClass().getName());
        }
      }


      if (SessionState.get() != null) {
        SessionState.get().getHiveHistory().setQueryProperty(queryId, Keys.QUERY_RET_CODE,
            String.valueOf(0));
        SessionState.get().getHiveHistory().printRowCount(queryId);
      }
    } catch (CommandNeedRetryException e) {
      throw e;
    } catch (Exception e) {
      ctx.restoreOriginalTracker();
      if (SessionState.get() != null) {
        SessionState.get().getHiveHistory().setQueryProperty(queryId, Keys.QUERY_RET_CODE,
            String.valueOf(12));
      }
      // TODO: do better with handling types of Exception here
      errorMessage = "FAILED: Hive Internal Error: " + Utilities.getNameMessage(e);
      SQLState = "08S01";
      console.printError(errorMessage + "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return (12);
    } finally {
      if (SessionState.get() != null) {
        SessionState.get().getHiveHistory().endQuery(queryId);
      }
      if (noName) {
        conf.setVar(HiveConf.ConfVars.HADOOPJOBNAME, "");
      }
      perfLogger.PerfLogEnd(LOG, PerfLogger.DRIVER_EXECUTE);

      if (SessionState.get().getLastMapRedStatsList() != null
          && SessionState.get().getLastMapRedStatsList().size() > 0) {
        long totalCpu = 0;
        console.printInfo("MapReduce Jobs Launched: ");
        for (int i = 0; i < SessionState.get().getLastMapRedStatsList().size(); i++) {
          console.printInfo("Job " + i + ": " + SessionState.get().getLastMapRedStatsList().get(i));
          totalCpu += SessionState.get().getLastMapRedStatsList().get(i).getCpuMSec();
        }
        console.printInfo("Total MapReduce CPU Time Spent: " + Utilities.formatMsecToStr(totalCpu));
      }
    }
    plan.setDone();

    if (SessionState.get() != null) {
      try {
        SessionState.get().getHiveHistory().logPlanProgress(plan);
      } catch (Exception e) {
      }
    }
    console.printInfo("OK");

    return (0);
  }

  /**
   * Launches a new task
   *
   * @param tsk
   *          task being launched
   * @param queryId
   *          Id of the query containing the task
   * @param noName
   *          whether the task has a name set
   * @param running
   *          map from taskresults to taskrunners
   * @param jobname
   *          name of the task, if it is a map-reduce job
   * @param jobs
   *          number of map-reduce jobs
   * @param curJobNo
   *          the sequential number of the next map-reduce job
   * @return the updated number of last the map-reduce job launched
   */

  public void launchTask(Task<? extends Serializable> tsk, String queryId, boolean noName,
      Map<TaskResult, TaskRunner> running, String jobname, int jobs, DriverContext cxt) {

    if (SessionState.get() != null) {
      SessionState.get().getHiveHistory().startTask(queryId, tsk, tsk.getClass().getName());
    }
    if (tsk.isMapRedTask() && !(tsk instanceof ConditionalTask)) {
      if (noName) {
        conf.setVar(HiveConf.ConfVars.HADOOPJOBNAME, jobname + "(" + tsk.getId() + ")");
      }
      cxt.incCurJobNo(1);
      console.printInfo("Launching Job " + cxt.getCurJobNo() + " out of " + jobs);
    }
    tsk.initialize(conf, plan, cxt);
    TaskResult tskRes = new TaskResult();
    TaskRunner tskRun = new TaskRunner(tsk, tskRes);

    // Launch Task
    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.EXECPARALLEL) && tsk.isMapRedTask()) {
      // Launch it in the parallel mode, as a separate thread only for MR tasks
      tskRun.start();
    } else {
      tskRun.runSequential();
    }
    running.put(tskRes, tskRun);
    return;
  }

  /**
   * Cleans up remaining tasks in case of failure
   */

  public void taskCleanup() {
    // The currently existing Shutdown hooks will be automatically called,
    // killing the map-reduce processes.
    // The non MR processes will be killed as well.
    System.exit(9);
  }

  /**
   * Polls running tasks to see if a task has ended.
   *
   * @param results
   *          Set of result objects for running tasks
   * @return The result object for any completed/failed task
   */

  public TaskResult pollTasks(Set<TaskResult> results) {
    Iterator<TaskResult> resultIterator = results.iterator();
    while (true) {
      while (resultIterator.hasNext()) {
        TaskResult tskRes = resultIterator.next();
        if (tskRes.isRunning() == false) {
          return tskRes;
        }
      }

      // In this loop, nothing was found
      // Sleep 10 seconds and restart
      try {
        Thread.sleep(SLEEP_TIME);
      } catch (InterruptedException ie) {
        // Do Nothing
        ;
      }
      resultIterator = results.iterator();
    }
  }

  public boolean getResults(ArrayList<String> res) throws IOException, CommandNeedRetryException {
    if (plan != null && plan.getFetchTask() != null) {
      FetchTask ft = plan.getFetchTask();
      ft.setMaxRows(maxRows);
      return ft.fetch(res);
    }

    if (resStream == null) {
      resStream = ctx.getStream();
    }
    if (resStream == null) {
      return false;
    }

    int numRows = 0;
    String row = null;

    while (numRows < maxRows) {
      if (resStream == null) {
        if (numRows > 0) {
          return true;
        } else {
          return false;
        }
      }

      bos.reset();
      Utilities.StreamStatus ss;
      try {
        ss = Utilities.readColumn(resStream, bos);
        if (bos.getCount() > 0) {
          row = new String(bos.getData(), 0, bos.getCount(), "UTF-8");
        } else if (ss == Utilities.StreamStatus.TERMINATED) {
          row = new String();
        }

        if (row != null) {
          numRows++;
          res.add(row);
        }
      } catch (IOException e) {
        console.printError("FAILED: Unexpected IO exception : " + e.getMessage());
        res = null;
        return false;
      }

      if (ss == Utilities.StreamStatus.EOF) {
        resStream = ctx.getStream();
      }
    }
    return true;
  }

  public int getTryCount() {
    return tryCount;
  }

  public void setTryCount(int tryCount) {
    this.tryCount = tryCount;
  }


  public int close() {
    try {
      if (plan != null) {
        FetchTask fetchTask = plan.getFetchTask();
        if (null != fetchTask) {
          try {
            fetchTask.clearFetch();
          } catch (Exception e) {
            LOG.debug(" Exception while clearing the Fetch task ", e);
          }
        }
      }
      if (ctx != null) {
        ctx.clear();
      }
      if (null != resStream) {
        try {
          ((FSDataInputStream) resStream).close();
        } catch (Exception e) {
          LOG.debug(" Exception while closing the resStream ", e);
        }
      }
    } catch (Exception e) {
      console.printError("FAILED: Hive Internal Error: " + Utilities.getNameMessage(e) + "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return 13;
    }

    return 0;
  }

  public void destroy() {
    releaseLocks();
  }

  public org.apache.hadoop.hive.ql.plan.api.Query getQueryPlan() throws IOException {
    return plan.getQueryPlan();
  }
}
