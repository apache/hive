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
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import org.apache.hadoop.hive.ql.parse.ASTNode;

import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.parse.ErrorMsg;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.exec.ExecDriver;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.exec.TaskResult;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.PreExecute;
import org.apache.hadoop.hive.ql.hooks.PostExecute;

import org.apache.hadoop.hive.ql.history.HiveHistory.Keys;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UnixUserGroupInformation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Driver implements CommandProcessor {

  static final private Log LOG = LogFactory.getLog(Driver.class.getName());
  static final private LogHelper console = new LogHelper(LOG);

  private int maxRows = 100;
  ByteStream.Output bos = new ByteStream.Output();

  private HiveConf conf;
  private DataInput resStream;
  private Context ctx;
  private QueryPlan plan;
  private String errorMessage;
  private String SQLState;

  // A limit on the number of threads that can be launched
  private int maxthreads = 8;
  private int sleeptime = 2000;

  public int countJobs(List<Task<? extends Serializable>> tasks) {
    return countJobs(tasks, new ArrayList<Task<? extends Serializable>>());
  }

  public int countJobs(List<Task<? extends Serializable>> tasks, List<Task<? extends Serializable>> seenTasks) {
    if (tasks == null)
      return 0;
    int jobs = 0;
    for (Task<? extends Serializable> task : tasks) {
      if (!seenTasks.contains(task)) {
        seenTasks.add(task);
        if (task.isMapRedTask()) {
          jobs++;
        }
        jobs += countJobs(task.getChildTasks(), seenTasks);
      }
    }
    return jobs;
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
    }
    catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    LOG.info("Returning cluster status: " + cs.toString());
    return cs;
  }

  /**
   * Get a Schema with fields represented with native Hive types
   */
  public Schema getSchema() throws Exception {
    Schema schema;
    try {
      if (plan != null && plan.getPlan().getFetchTask() != null) {
        BaseSemanticAnalyzer sem = plan.getPlan();

        if (!sem.getFetchTaskInit()) {
          sem.setFetchTaskInit(true);
          sem.getFetchTask().initialize(conf, plan);
        }
        FetchTask ft = (FetchTask) sem.getFetchTask();

        tableDesc td = ft.getTblDesc();
        // partitioned tables don't have tableDesc set on the FetchTask. Instead
        // they have a list of PartitionDesc objects, each with a table desc. Let's
        // try to fetch the desc for the first partition and use it's deserializer.
        if (td == null && ft.getWork() != null && ft.getWork().getPartDesc() != null) {
          if (ft.getWork().getPartDesc().size() > 0) {
            td = ft.getWork().getPartDesc().get(0).getTableDesc();
          }
        }

        if (td == null) {
          throw new Exception("No table description found for fetch task: " + ft);
        }

        String tableName = "result";
        List<FieldSchema> lst = MetaStoreUtils.getFieldsFromDeserializer(
            tableName, td.getDeserializer());
        schema = new Schema(lst, null);
      }
      else {
        schema = new Schema();
      }
    }
    catch (Exception e) {
      e.printStackTrace();
      throw e;
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
      schema = this.getSchema();
      if (schema != null) {
	    List<FieldSchema> lst = schema.getFieldSchemas();
	    // Go over the schema and convert type to thrift type
	    if (lst != null) {
	      for (FieldSchema f : lst) {
	        f.setType(MetaStoreUtils.typeToThriftType(f.getType()));
          }
	    }
      }
    }
    catch (Exception e) {
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
    if (tasks == null)
      return false;

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
    try {
      UnixUserGroupInformation.login(conf, true);
    } catch (Exception e) {
      LOG.warn("Ignoring " + e.getMessage());
    }
  }

  public Driver() {
    if (SessionState.get() != null) {
      conf = SessionState.get().getConf();
      try {
        UnixUserGroupInformation.login(conf, true);
      } catch (Exception e) {
        LOG.warn("Ignoring " + e.getMessage());
      }
    }
  }

  /**
   * Compile a new query. Any currently-planned query associated with this Driver is discarded.
   *
   * @param command The SQL query to compile.
   */
  public int compile(String command) {
    if (plan != null) {
      close();
      plan = null;
    }

    TaskFactory.resetId();

    try {
      ctx = new Context (conf);

      ParseDriver pd = new ParseDriver();
      ASTNode tree = pd.parse(command);

      while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
        tree = (ASTNode) tree.getChild(0);
      }

      BaseSemanticAnalyzer sem = SemanticAnalyzerFactory.get(conf, tree);
      // Do semantic analysis and plan generation
      sem.analyze(tree, ctx);
      LOG.info("Semantic Analysis Completed");

      // validate the plan
      sem.validate();

      plan = new QueryPlan(command, sem);
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
      errorMessage = "FAILED: Unknown exception: " + e.getMessage();
      SQLState = ErrorMsg.findSQLState(e.getMessage());
      console.printError(errorMessage, "\n"
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

  public int run(String command) {
    DriverResponse response = runCommand(command);
    return response.getResponseCode();
  }

  public DriverResponse runCommand(String command) {
    errorMessage = null;
    SQLState = null;

    int ret = compile(command);
    if (ret != 0)
      return new DriverResponse(ret, errorMessage, SQLState);

    ret = execute();
    if (ret != 0)
      return new DriverResponse(ret, errorMessage, SQLState);

    return new DriverResponse(ret);
  }

  /**
   * Encapsulates the basic response info returned by the Driver. Typically
   * <code>errorMessage</code> and <code>SQLState</code> will only be set if
   * the <code>responseCode</code> is not 0.
   */
  public class DriverResponse {
    private int responseCode;
    private String errorMessage;
    private String SQLState;

    public DriverResponse(int responseCode) {
      this(responseCode, null, null);
    }

    public DriverResponse(int responseCode, String errorMessage, String SQLState) {
      this.responseCode = responseCode;
      this.errorMessage = errorMessage;
      this.SQLState = SQLState;
    }

    public int getResponseCode() { return responseCode; }
    public String getErrorMessage() { return errorMessage; }
    public String getSQLState() { return SQLState; }
  }

  private List<PreExecute> getPreExecHooks() throws Exception {
    ArrayList<PreExecute> pehooks = new ArrayList<PreExecute>();
    String pestr = conf.getVar(HiveConf.ConfVars.PREEXECHOOKS);
    pestr = pestr.trim();
    if (pestr.equals(""))
      return pehooks;

    String[] peClasses = pestr.split(",");

    for(String peClass: peClasses) {
      try {
        pehooks.add((PreExecute)Class.forName(peClass.trim(), true, JavaUtils.getClassLoader()).newInstance());
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
    if (pestr.equals(""))
      return pehooks;

    String[] peClasses = pestr.split(",");

    for(String peClass: peClasses) {
      try {
        pehooks.add((PostExecute)Class.forName(peClass.trim(), true, JavaUtils.getClassLoader()).newInstance());
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

    int curJobNo=0;

    String queryId = plan.getQueryId();
    String queryStr = plan.getQueryStr();

    conf.setVar(HiveConf.ConfVars.HIVEQUERYID, queryId);
    conf.setVar(HiveConf.ConfVars.HIVEQUERYSTRING, queryStr);

    try {
      LOG.info("Starting command: " + queryStr);

      plan.setStarted();

      if (SessionState.get() != null) {
        SessionState.get().getHiveHistory().startQuery(queryStr, conf.getVar(HiveConf.ConfVars.HIVEQUERYID) );
        SessionState.get().getHiveHistory().logPlanProgress(plan);
      }
      resStream = null;

      BaseSemanticAnalyzer sem = plan.getPlan();

      // Get all the pre execution hooks and execute them.
      for(PreExecute peh: getPreExecHooks()) {
        peh.run(SessionState.get(),
                sem.getInputs(), sem.getOutputs(),
                UnixUserGroupInformation.readFromConf(conf, UnixUserGroupInformation.UGI_PROPERTY_NAME));
      }

      int jobs = countJobs(sem.getRootTasks());
      if (jobs > 0) {
        console.printInfo("Total MapReduce jobs = " + jobs);
      }
      if (SessionState.get() != null){
        SessionState.get().getHiveHistory().setQueryProperty(queryId,
            Keys.QUERY_NUM_TASKS, String.valueOf(jobs));
        SessionState.get().getHiveHistory().setIdToTableMap(sem.getIdToTableNameMap());
      }
      String jobname = Utilities.abbreviate(queryStr, maxlen - 6);

      // A runtime that launches runnable tasks as separate Threads through TaskRunners
      // As soon as a task isRunnable, it is put in a queue
      // At any time, at most maxthreads tasks can be running
      // The main thread polls the TaskRunners to check if they have finished.

      Queue<Task<? extends Serializable>> runnable = new LinkedList<Task<? extends Serializable>>();
      Map<TaskResult, TaskRunner> running = new HashMap<TaskResult, TaskRunner> ();

      //Add root Tasks to runnable

      for (Task<? extends Serializable> tsk : sem.getRootTasks()) {
        addToRunnable(runnable,tsk);
      }

      // Loop while you either have tasks running, or tasks queued up

      while (running.size() != 0 || runnable.peek()!=null) {
        // Launch upto maxthreads tasks
        while(runnable.peek() != null && running.size() < maxthreads) {
          Task<? extends Serializable> tsk = runnable.remove();
          curJobNo = launchTask(tsk, queryId, noName,running, jobname, jobs, curJobNo);
        }

        // poll the Tasks to see which one completed
        TaskResult tskRes = pollTasks(running.keySet());
        TaskRunner tskRun = running.remove(tskRes);
        Task<? extends Serializable> tsk = tskRun.getTask();

        int exitVal = tskRes.getExitVal();
        if(exitVal != 0) {
          //TODO: This error messaging is not very informative. Fix that.
          errorMessage = "FAILED: Execution Error, return code " + exitVal
                         + " from " + tsk.getClass().getName();
          SQLState = "08S01";
          console.printError(errorMessage);
          if(running.size() !=0) {
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
            if(isLaunchable(child)) { 
              addToRunnable(runnable,child);
            }
          }
        }
      }

      // Get all the post execution hooks and execute them.
      for(PostExecute peh: getPostExecHooks()) {
        peh.run(SessionState.get(),
                sem.getInputs(), sem.getOutputs(),
                UnixUserGroupInformation.readFromConf(conf, UnixUserGroupInformation.UGI_PROPERTY_NAME));
      }

      if (SessionState.get() != null){
        SessionState.get().getHiveHistory().setQueryProperty(queryId,
            Keys.QUERY_RET_CODE, String.valueOf(0));
        SessionState.get().getHiveHistory().printRowCount(queryId);
      }
    } catch (Exception e) {
      if (SessionState.get() != null)
        SessionState.get().getHiveHistory().setQueryProperty(queryId,
            Keys.QUERY_RET_CODE, String.valueOf(12));
      //TODO: do better with handling types of Exception here
      errorMessage = "FAILED: Unknown exception : " + e.getMessage();
      SQLState = "08S01";
      console.printError(errorMessage, "\n"
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
   * @param tsk      task being launched
   * @param queryId  Id of the query containing the task
   * @param noName   whether the task has a name set
   * @param running map from taskresults to taskrunners
   * @param jobname  name of the task, if it is a map-reduce job
   * @param jobs     number of map-reduce jobs
   * @param curJobNo the sequential number of the next map-reduce job
   * @return         the updated number of last the map-reduce job launched
   */



  public int launchTask(Task<? extends Serializable> tsk, String queryId, 
    boolean noName, Map<TaskResult,TaskRunner> running, String jobname, 
    int jobs, int curJobNo) {
    
    if (SessionState.get() != null) {
      SessionState.get().getHiveHistory().startTask(queryId, tsk,
        tsk.getClass().getName());
    }
    if (tsk.isMapRedTask()) {
      if (noName) {
        conf.setVar(HiveConf.ConfVars.HADOOPJOBNAME, jobname + "(" 
          + tsk.getId() + ")");
      }
      curJobNo++;
      console.printInfo("Launching Job " + curJobNo + " out of "+jobs);
    }
    tsk.initialize(conf, plan);
    TaskResult tskRes = new TaskResult();
    TaskRunner tskRun = new TaskRunner(tsk,tskRes);

    //Launch Task
    if(HiveConf.getBoolVar(conf, HiveConf.ConfVars.EXECPARALLEL) && tsk.isMapRedTask()) {
      // Launch it in the parallel mode, as a separate thread only for MR tasks
      tskRun.start();
    }
    else
    {
      tskRun.runSequential();
    }
    running.put(tskRes,tskRun);        
    return curJobNo;
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
   * @param results  Set of result objects for running tasks
   * @return         The result object for any completed/failed task
   */

  public TaskResult pollTasks(Set<TaskResult> results) {
    Iterator<TaskResult> resultIterator = results.iterator();
    while(true) {
      while(resultIterator.hasNext()) {
        TaskResult tskRes = resultIterator.next();
        if(tskRes.isRunning() == false) {
          return tskRes;
        }
      }

      // In this loop, nothing was found
      // Sleep 10 seconds and restart
      try {
        Thread.sleep(sleeptime);
      }
      catch (InterruptedException ie) {
        //Do Nothing
        ;
      }
      resultIterator = results.iterator();
    }
  }

  /**
   * Checks if a task can be launched
   * 
   * @param tsk the task to be checked 
   * @return    true if the task is launchable, false otherwise
   */

  public boolean isLaunchable(Task<? extends Serializable> tsk) {
    // A launchable task is one that hasn't been queued, hasn't been initialized, and is runnable.
    return !tsk.getQueued() && !tsk.getInitialized() && tsk.isRunnable();
  }

  public void addToRunnable(Queue<Task<? extends Serializable>> runnable,
    Task<? extends Serializable> tsk) {
    runnable.add(tsk);
    tsk.setQueued();
 }

  public boolean getResults(Vector<String> res) throws IOException {
    if (plan != null && plan.getPlan().getFetchTask() != null) {
      BaseSemanticAnalyzer sem = plan.getPlan();
      if (!sem.getFetchTaskInit()) {
        sem.setFetchTaskInit(true);
        sem.getFetchTask().initialize(conf, plan);
      }
      FetchTask ft = (FetchTask) sem.getFetchTask();
      ft.setMaxRows(maxRows);
      return ft.fetch(res);
    }

    if (resStream == null)
      resStream = ctx.getStream();
    if (resStream == null)
      return false;

    int numRows = 0;
    String row = null;

    while (numRows < maxRows) {
      if (resStream == null) {
        if (numRows > 0)
          return true;
        else
          return false;
      }

      bos.reset();
      Utilities.streamStatus ss;
      try {
        ss = Utilities.readColumn(resStream, bos);
        if (bos.getCount() > 0)
          row = new String(bos.getData(), 0, bos.getCount(), "UTF-8");
        else if (ss == Utilities.streamStatus.TERMINATED)
          row = new String();

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

      if (ss == Utilities.streamStatus.EOF)
        resStream = ctx.getStream();
    }
    return true;
  }

  public int close() {
    try {
      ctx.clear();
    } catch (Exception e) {
      console.printError("FAILED: Unknown exception : " + e.getMessage(), "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return (13);
    }

    return (0);
  }

  public org.apache.hadoop.hive.ql.plan.api.Query getQueryPlan() throws IOException {
    return plan.getQueryPlan();
  }
}
