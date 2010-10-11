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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.ExecDriver;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.TaskResult;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.history.HiveHistory.Keys;
import org.apache.hadoop.hive.ql.hooks.PostExecute;
import org.apache.hadoop.hive.ql.hooks.PreExecute;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.lockmgr.HiveLock;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockManager;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockManagerCtx;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockMode;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObject;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.metadata.DummyPartition;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContextImpl;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;
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
  private final int sleeptime = 2000;

  private int checkLockManager() {
    boolean supportConcurrency = conf.getBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY);
    if (supportConcurrency && (hiveLockMgr == null)) {
      try {
        setLockManager();
      } catch (SemanticException e) {
        errorMessage = "FAILED: Error in semantic analysis: " + e.getMessage();
        SQLState = ErrorMsg.findSQLState(e.getMessage());
        console.printError(errorMessage, "\n"
                           + org.apache.hadoop.util.StringUtils.stringifyException(e));
        return (12);
      }
    }
    return (0);
  }

  private void setLockManager() throws SemanticException {
    boolean supportConcurrency = conf.getBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY);
    if (supportConcurrency) {
      String lockMgr = conf.getVar(HiveConf.ConfVars.HIVE_LOCK_MANAGER);
      if ((lockMgr == null) || (lockMgr.isEmpty())) {
        throw new SemanticException(ErrorMsg.LOCKMGR_NOT_SPECIFIED.getMsg());
      }

      try {
        hiveLockMgr = (HiveLockManager)
          ReflectionUtils.newInstance(conf.getClassByName(lockMgr), conf);
        hiveLockMgr.setContext(new HiveLockManagerCtx(conf));
      } catch (Exception e) {
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
      if (td == null && ft.getWork() != null
          && ft.getWork().getPartDesc() != null) {
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
          lst = MetaStoreUtils.getFieldsFromDeserializer(
              tableName, td.getDeserializer());
        } catch (Exception e) {
          LOG.warn("Error getting schema: " +
              org.apache.hadoop.util.StringUtils.stringifyException(e));
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
   * Compile a new query. Any currently-planned query associated with this
   * Driver is discarded.
   *
   * @param command
   *          The SQL query to compile.
   */
  public int compile(String command) {
    if (plan != null) {
      close();
      plan = null;
    }

    TaskFactory.resetId();

    try {
      ctx = new Context(conf);

      ParseDriver pd = new ParseDriver();
      ASTNode tree = pd.parse(command, ctx);
      tree = ParseUtils.findRootNonNullToken(tree);

      BaseSemanticAnalyzer sem = SemanticAnalyzerFactory.get(conf, tree);
      String hookName = HiveConf.getVar(conf, ConfVars.SEMANTIC_ANALYZER_HOOK);

      // Do semantic analysis and plan generation
      if (hookName != null){
        AbstractSemanticAnalyzerHook hook =   HiveUtils.getSemanticAnalyzerHook(conf, hookName);
        HiveSemanticAnalyzerHookContext hookCtx = new HiveSemanticAnalyzerHookContextImpl();
        hookCtx.setConf(conf);
        hook.preAnalyze(hookCtx, tree);
        sem.analyze(tree, ctx);
        hook.postAnalyze(hookCtx, sem.getRootTasks());
      }
      else{
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
      if("true".equalsIgnoreCase(System.getProperty("test.serialize.qplan"))) {

        String queryPlanFileName = ctx.getLocalScratchDir(true) + Path.SEPARATOR_CHAR
          + "queryplan.xml";
        LOG.info("query plan = " + queryPlanFileName);
        queryPlanFileName = new Path(queryPlanFileName).toUri().getPath();

        //   serialize the queryPlan
        FileOutputStream fos = new FileOutputStream(queryPlanFileName);
        Utilities.serializeQueryPlan(plan, fos);
        fos.close();

        //   deserialize the queryPlan
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

      return (0);
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
    }
  }

  /**
   * @return The current query plan associated with this Driver, if any.
   */
  public QueryPlan getPlan() {
    return plan;
  }

  public static class LockObject {
    HiveLockObject obj;
    HiveLockMode   mode;

    public LockObject(HiveLockObject obj, HiveLockMode mode) {
      this.obj  = obj;
      this.mode = mode;
    }

    public HiveLockObject getObj() {
      return obj;
    }

    public HiveLockMode getMode() {
      return mode;
    }

    public String getName() {
      return obj.getName();
    }
  }

  /**
   * @param t     The table to be locked
   * @param p     The partition to be locked
   * @param mode  The mode of the lock (SHARED/EXCLUSIVE)
   * Get the list of objects to be locked. If a partition needs to be locked (in any mode), all its parents
   * should also be locked in SHARED mode.
   **/
  private List<LockObject> getLockObjects(Table t, Partition p, HiveLockMode mode) {
    List<LockObject> locks = new LinkedList<LockObject>();

    if (t != null) {
      locks.add(new LockObject(new HiveLockObject(t), mode));
      return locks;
    }

    if (p != null) {
      locks.add(new LockObject(new HiveLockObject(p), mode));

      // All the parents are locked in shared mode
      mode = HiveLockMode.SHARED;

      String partName = p.getName();
      String partialName = "";
      String[] partns = p.getName().split("/");
      for (int idx = 0; idx < partns.length -1; idx++) {
        String partn = partns[idx];
        partialName += partialName + partn;
        locks.add(new LockObject(new HiveLockObject(new DummyPartition(p.getTable().getDbName() + "@" + p.getTable().getTableName() + "@" + partialName)), mode));
        partialName += "/";
      }

      locks.add(new LockObject(new HiveLockObject(p.getTable()), mode));
    }
    return locks;
  }

  /**
   * Acquire read and write locks needed by the statement. The list of objects to be locked are obtained
   * from he inputs and outputs populated by the compiler. The lock acuisition scheme is pretty simple.
   * If all the locks cannot be obtained, error out. Deadlock is avoided by making sure that the locks
   * are lexicographically sorted.
   **/
  public int acquireReadWriteLocks() {
    try {
      int tryNum = 1;
      int sleepTime = conf.getIntVar(HiveConf.ConfVars.HIVE_LOCK_SLEEP_BETWEEN_RETRIES) * 1000;
      int numRetries = conf.getIntVar(HiveConf.ConfVars.HIVE_LOCK_NUMRETRIES);

      boolean supportConcurrency = conf.getBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY);
      if (!supportConcurrency) {
        return 0;
      }

      List<LockObject> lockObjects = new ArrayList<LockObject>();

      // Sort all the inputs, outputs.
      // If a lock needs to be acquired on any partition, a read lock needs to be acquired on all its parents also
      for (ReadEntity input : plan.getInputs()) {
        if (input.getType() == ReadEntity.Type.TABLE) {
          lockObjects.addAll(getLockObjects(input.getTable(), null, HiveLockMode.SHARED));
        }
        else {
          lockObjects.addAll(getLockObjects(null, input.getPartition(), HiveLockMode.SHARED));
        }
      }

      for (WriteEntity output : plan.getOutputs()) {
        if (output.getTyp() == WriteEntity.Type.TABLE) {
          lockObjects.addAll(getLockObjects(output.getTable(), null, HiveLockMode.EXCLUSIVE));
        }
        else if (output.getTyp() == WriteEntity.Type.PARTITION) {
          lockObjects.addAll(getLockObjects(null, output.getPartition(), HiveLockMode.EXCLUSIVE));
        }
      }

      if (lockObjects.isEmpty() && !ctx.isNeedLockMgr()) {
        return 0;
      }

      int ret = checkLockManager();
      if (ret != 0) {
        return ret;
      }


      ctx.setHiveLockMgr(hiveLockMgr);

      Collections.sort(lockObjects, new Comparator<LockObject>() {

          @Override
            public int compare(LockObject o1, LockObject o2) {
            int cmp = o1.getName().compareTo(o2.getName());
            if (cmp == 0) {
              if (o1.getMode() == o2.getMode()) {
                return cmp;
              }
              // EXCLUSIVE locks occur before SHARED locks
              if (o1.getMode() == HiveLockMode.EXCLUSIVE) {
                return -1;
              }
              return +1;
            }
            return cmp;
          }

        });

      // walk the list and acquire the locks - if any lock cant be acquired, release all locks, sleep and retry
      while (true) {
        List<HiveLock> hiveLocks = acquireLocks(lockObjects);

        if (hiveLocks == null) {
          if (tryNum == numRetries) {
            throw new SemanticException(ErrorMsg.LOCK_CANNOT_BE_ACQUIRED.getMsg());
          }
          tryNum++;

          try {
            Thread.sleep(sleepTime);
          } catch (InterruptedException e) {
          }
        }
        else {
          ctx.setHiveLocks(hiveLocks);
          break;
        }
      }
      return (0);
    } catch (SemanticException e) {
      errorMessage = "FAILED: Error in acquiring locks: " + e.getMessage();
      SQLState = ErrorMsg.findSQLState(e.getMessage());
      console.printError(errorMessage, "\n"
                         + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return (10);
    }
  }

  /**
   * @param lockObjects   The list of objects to be locked
   * Lock the objects specified in the list. The same object is not locked twice, and the list passed is sorted
   * such that EXCLUSIVE locks occur before SHARED locks.
   **/
  private List<HiveLock> acquireLocks(List<LockObject> lockObjects) throws SemanticException {
    // walk the list and acquire the locks - if any lock cant be acquired, release all locks, sleep and retry
    LockObject prevLockObj = null;
    List<HiveLock> hiveLocks = new ArrayList<HiveLock>();

    for (LockObject lockObject: lockObjects) {
      // No need to acquire a lock twice on the same object
      // It is ensured that EXCLUSIVE locks occur before SHARED locks on the same object
      if ((prevLockObj != null) && (prevLockObj.getName().equals(lockObject.getName()))) {
        prevLockObj = lockObject;
        continue;
      }

      HiveLock lock = null;
      try {
        lock = ctx.getHiveLockMgr().lock(lockObject.getObj(), lockObject.getMode(), false);
      } catch (LockException e) {
        lock = null;
      }

      if (lock == null) {
        releaseLocks(hiveLocks);
        return null;
      }

      hiveLocks.add(lock);
      prevLockObj = lockObject;
    }

    return hiveLocks;
  }

  /**
   * Release all the locks acquired implicitly by the statement. Note that the locks acquired
   * with 'keepAlive' set to True are not released.
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
   * @param hiveLocks  list of hive locks to be released
   * Release all the locks specified. If some of the locks have already been released, ignore them
   **/
  private void releaseLocks(List<HiveLock> hiveLocks) {
    if (hiveLocks != null) {
      for (HiveLock hiveLock: hiveLocks) {
        try {
          ctx.getHiveLockMgr().unlock(hiveLock);
        } catch (LockException e) {
          // The lock may have been released. Ignore and continue
        }
      }
      ctx.setHiveLocks(null);
    }
  }

  public CommandProcessorResponse run(String command) {
    errorMessage = null;
    SQLState = null;

    int ret = compile(command);
    if (ret != 0) {
      releaseLocks(ctx.getHiveLocks());
      return new CommandProcessorResponse(ret, errorMessage, SQLState);
    }

    ret = acquireReadWriteLocks();
    if (ret != 0) {
      releaseLocks(ctx.getHiveLocks());
      return new CommandProcessorResponse(ret, errorMessage, SQLState);
    }

    ret = execute();
    if (ret != 0) {
      releaseLocks(ctx.getHiveLocks());
      return new CommandProcessorResponse(ret, errorMessage, SQLState);
    }

    releaseLocks(ctx.getHiveLocks());
    return new CommandProcessorResponse(ret);
  }

  private List<PreExecute> getPreExecHooks() throws Exception {
    ArrayList<PreExecute> pehooks = new ArrayList<PreExecute>();
    String pestr = conf.getVar(HiveConf.ConfVars.PREEXECHOOKS);
    pestr = pestr.trim();
    if (pestr.equals("")) {
      return pehooks;
    }

    String[] peClasses = pestr.split(",");

    for (String peClass : peClasses) {
      try {
        pehooks.add((PreExecute) Class.forName(peClass.trim(), true,
            JavaUtils.getClassLoader()).newInstance());
      } catch (ClassNotFoundException e) {
        console.printError("Pre Exec Hook Class not found:" + e.getMessage());
        throw e;
      }
    }

    return pehooks;
  }

  private List<PostExecute> getPostExecHooks() throws Exception {
    ArrayList<PostExecute> pehooks = new ArrayList<PostExecute>();
    String pestr = conf.getVar(HiveConf.ConfVars.POSTEXECHOOKS);
    pestr = pestr.trim();
    if (pestr.equals("")) {
      return pehooks;
    }

    String[] peClasses = pestr.split(",");

    for (String peClass : peClasses) {
      try {
        pehooks.add((PostExecute) Class.forName(peClass.trim(), true,
            JavaUtils.getClassLoader()).newInstance());
      } catch (ClassNotFoundException e) {
        console.printError("Post Exec Hook Class not found:" + e.getMessage());
        throw e;
      }
    }

    return pehooks;
  }

  public int execute() {
    boolean noName = StringUtils.isEmpty(conf
        .getVar(HiveConf.ConfVars.HADOOPJOBNAME));
    int maxlen = conf.getIntVar(HiveConf.ConfVars.HIVEJOBNAMELENGTH);

    int curJobNo = 0;

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

      // Get all the pre execution hooks and execute them.
      for (PreExecute peh : getPreExecHooks()) {
        peh.run(SessionState.get(), plan.getInputs(), plan.getOutputs(),
                ShimLoader.getHadoopShims().getUGIForConf(conf));
      }

      int jobs = Utilities.getMRTasks(plan.getRootTasks()).size();
      if (jobs > 0) {
        console.printInfo("Total MapReduce jobs = " + jobs);
      }
      if (SessionState.get() != null) {
        SessionState.get().getHiveHistory().setQueryProperty(queryId,
            Keys.QUERY_NUM_TASKS, String.valueOf(jobs));
        SessionState.get().getHiveHistory().setIdToTableMap(
            plan.getIdToTableNameMap());
      }
      String jobname = Utilities.abbreviate(queryStr, maxlen - 6);

      // A runtime that launches runnable tasks as separate Threads through
      // TaskRunners
      // As soon as a task isRunnable, it is put in a queue
      // At any time, at most maxthreads tasks can be running
      // The main thread polls the TaskRunners to check if they have finished.

      Queue<Task<? extends Serializable>> runnable = new LinkedList<Task<? extends Serializable>>();
      Map<TaskResult, TaskRunner> running = new HashMap<TaskResult, TaskRunner>();

      DriverContext driverCxt = new DriverContext(runnable, ctx);

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

        int exitVal = tskRes.getExitVal();
        if (exitVal != 0) {
          // TODO: This error messaging is not very informative. Fix that.
          errorMessage = "FAILED: Execution Error, return code " + exitVal
              + " from " + tsk.getClass().getName();
          SQLState = "08S01";
          console.printError(errorMessage);
          if (running.size() != 0) {
            taskCleanup();
          }
          return 9;
        }

        if (SessionState.get() != null) {
          SessionState.get().getHiveHistory().setTaskProperty(queryId,
              tsk.getId(), Keys.TASK_RET_CODE, String.valueOf(exitVal));
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

      // Get all the post execution hooks and execute them.
      for (PostExecute peh : getPostExecHooks()) {
        peh.run(SessionState.get(), plan.getInputs(), plan.getOutputs(),
            (SessionState.get() != null ? SessionState.get().getLineageState().getLineageInfo() : null),
                ShimLoader.getHadoopShims().getUGIForConf(conf));
      }

      if (SessionState.get() != null) {
        SessionState.get().getHiveHistory().setQueryProperty(queryId,
            Keys.QUERY_RET_CODE, String.valueOf(0));
        SessionState.get().getHiveHistory().printRowCount(queryId);
      }
    } catch (Exception e) {
      if (SessionState.get() != null) {
        SessionState.get().getHiveHistory().setQueryProperty(queryId,
            Keys.QUERY_RET_CODE, String.valueOf(12));
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

  public void launchTask(Task<? extends Serializable> tsk, String queryId,
      boolean noName, Map<TaskResult, TaskRunner> running, String jobname,
      int jobs, DriverContext cxt) {

    if (SessionState.get() != null) {
      SessionState.get().getHiveHistory().startTask(queryId, tsk,
          tsk.getClass().getName());
    }
    if (tsk.isMapRedTask() && !(tsk instanceof ConditionalTask)) {
      if (noName) {
        conf.setVar(HiveConf.ConfVars.HADOOPJOBNAME, jobname + "("
            + tsk.getId() + ")");
      }
      cxt.incCurJobNo(1);
      console.printInfo("Launching Job " + cxt.getCurJobNo() + " out of " + jobs);
    }
    tsk.initialize(conf, plan, cxt);
    TaskResult tskRes = new TaskResult();
    TaskRunner tskRun = new TaskRunner(tsk, tskRes);

    // Launch Task
    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.EXECPARALLEL)
        && tsk.isMapRedTask()) {
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
        Thread.sleep(sleeptime);
      } catch (InterruptedException ie) {
        // Do Nothing
        ;
      }
      resultIterator = results.iterator();
    }
  }

  public boolean getResults(ArrayList<String> res) throws IOException {
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
        console.printError("FAILED: Unexpected IO exception : "
            + e.getMessage());
        res = null;
        return false;
      }

      if (ss == Utilities.StreamStatus.EOF) {
        resStream = ctx.getStream();
      }
    }
    return true;
  }

  public int close() {
    try {
      if (ctx != null) {
        ctx.clear();
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

  public org.apache.hadoop.hive.ql.plan.api.Query getQueryPlan()
      throws IOException {
    return plan.getQueryPlan();
  }
}
