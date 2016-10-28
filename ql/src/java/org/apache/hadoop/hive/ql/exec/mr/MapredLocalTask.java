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
package org.apache.hadoop.hive.ql.exec.mr;

import static org.apache.hadoop.hive.ql.exec.mr.MapRedTask.HADOOP_CLIENT_OPTS;
import static org.apache.hadoop.hive.ql.exec.mr.MapRedTask.HADOOP_MEM_KEY;
import static org.apache.hadoop.hive.ql.exec.mr.MapRedTask.HADOOP_OPTS_KEY;
import static org.apache.hadoop.hive.ql.exec.mr.MapRedTask.HIVE_SYS_PROP;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.common.io.CachingPrintStream;
import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.BucketMatcher;
import org.apache.hadoop.hive.ql.exec.FetchOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.SecureCmdDoAs;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mapjoin.MapJoinMemoryExhaustionException;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BucketMapJoinContext;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.OperationLog;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hive.common.util.StreamPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MapredLocalTask represents any local work (i.e.: client side work) that hive needs to
 * execute. E.g.: This is used for generating Hashtables for Mapjoins on the client
 * before the Join is executed on the cluster.
 *
 * MapRedLocalTask does not actually execute the work in process, but rather generates
 * a command using ExecDriver. ExecDriver is what will finally drive processing the records.
 */
public class MapredLocalTask extends Task<MapredLocalWork> implements Serializable {

  private static final long serialVersionUID = 1L;

  private final Map<String, FetchOperator> fetchOperators = new HashMap<String, FetchOperator>();
  protected HadoopJobExecHelper jobExecHelper;
  private JobConf job;
  public static transient final Logger l4j = LoggerFactory.getLogger(MapredLocalTask.class);
  static final String HIVE_LOCAL_TASK_CHILD_OPTS_KEY = "HIVE_LOCAL_TASK_CHILD_OPTS";
  public static MemoryMXBean memoryMXBean;
  private static final Logger LOG = LoggerFactory.getLogger(MapredLocalTask.class);

  // not sure we need this exec context; but all the operators in the work
  // will pass this context throught
  private ExecMapperContext execContext = null;

  private Process executor;
  private SecureCmdDoAs secureDoAs;

  public MapredLocalTask() {
    super();
  }

  public MapredLocalTask(MapredLocalWork plan, JobConf job, boolean isSilent) throws HiveException {
    setWork(plan);
    this.job = job;
    console = new LogHelper(LOG, isSilent);
  }

  public void setExecContext(ExecMapperContext execContext) {
    this.execContext = execContext;
  }

  @Override
  public void updateTaskMetrics(Metrics metrics) {
    metrics.incrementCounter(MetricsConstant.HIVE_MR_TASKS);
  }

  @Override
  public void initialize(QueryState queryState, QueryPlan queryPlan, DriverContext driverContext,
      CompilationOpContext opContext) {
    super.initialize(queryState, queryPlan, driverContext, opContext);
    job = new JobConf(conf, ExecDriver.class);
    execContext = new ExecMapperContext(job);
    //we don't use the HadoopJobExecHooks for local tasks
    this.jobExecHelper = new HadoopJobExecHelper(job, console, this, null);
  }

  public static String now() {
    Calendar cal = Calendar.getInstance();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    return sdf.format(cal.getTime());
  }

  @Override
  public boolean requireLock() {
    return true;
  }

  @Override
  public int execute(DriverContext driverContext) {
    if (conf.getBoolVar(HiveConf.ConfVars.SUBMITLOCALTASKVIACHILD)) {
      // send task off to another jvm
      return executeInChildVM(driverContext);
    } else {
      // execute in process
      return executeInProcess(driverContext);
    }
  }

  public int executeInChildVM(DriverContext driverContext) {
    // execute in child jvm
    try {
      // generate the cmd line to run in the child jvm
      Context ctx = driverContext.getCtx();
      String hiveJar = conf.getJar();

      String hadoopExec = conf.getVar(HiveConf.ConfVars.HADOOPBIN);
      conf.setVar(ConfVars.HIVEADDEDJARS, Utilities.getResourceFiles(conf, SessionState.ResourceType.JAR));
      // write out the plan to a local file
      Path planPath = new Path(ctx.getLocalTmpPath(), "plan.xml");
      MapredLocalWork plan = getWork();
      LOG.info("Generating plan file " + planPath.toString());

      OutputStream out = null;
      try {
        out = FileSystem.getLocal(conf).create(planPath);
        SerializationUtilities.serializePlan(plan, out);
        out.close();
        out = null;
      } finally {
        IOUtils.closeQuietly(out);
      }


      String isSilent = "true".equalsIgnoreCase(System.getProperty("test.silent")) ? "-nolog" : "";

      String jarCmd;

      jarCmd = hiveJar + " " + ExecDriver.class.getName();
      String hiveConfArgs = ExecDriver.generateCmdLine(conf, ctx);
      String cmdLine = hadoopExec + " jar " + jarCmd + " -localtask -plan " + planPath.toString()
          + " " + isSilent + " " + hiveConfArgs;

      String workDir = (new File(".")).getCanonicalPath();
      String files = Utilities.getResourceFiles(conf, SessionState.ResourceType.FILE);

      if (!files.isEmpty()) {
        cmdLine = cmdLine + " -files " + files;

        workDir = ctx.getLocalTmpPath().toUri().getPath();

        if (!(new File(workDir)).mkdir()) {
          throw new IOException("Cannot create tmp working dir: " + workDir);
        }

        for (String f : StringUtils.split(files, ',')) {
          Path p = new Path(f);
          String target = p.toUri().getPath();
          String link = workDir + Path.SEPARATOR + p.getName();
          if (FileUtil.symLink(target, link) != 0) {
            throw new IOException("Cannot link to added file: " + target + " from: " + link);
          }
        }
      }

      // Inherit Java system variables
      String hadoopOpts;
      StringBuilder sb = new StringBuilder();
      Properties p = System.getProperties();
      for (String element : HIVE_SYS_PROP) {
        if (p.containsKey(element)) {
          sb.append(" -D" + element + "=" + p.getProperty(element));
        }
      }
      hadoopOpts = sb.toString();
      // Inherit the environment variables
      String[] env;
      Map<String, String> variables = new HashMap<String, String>(System.getenv());
      // The user can specify the hadoop memory

      // if ("local".equals(conf.getVar(HiveConf.ConfVars.HADOOPJT))) {
      // if we are running in local mode - then the amount of memory used
      // by the child jvm can no longer default to the memory used by the
      // parent jvm
      // int hadoopMem = conf.getIntVar(HiveConf.ConfVars.HIVEHADOOPMAXMEM);
      int hadoopMem = conf.getIntVar(HiveConf.ConfVars.HIVEHADOOPMAXMEM);
      if (hadoopMem == 0) {
        // remove env var that would default child jvm to use parent's memory
        // as default. child jvm would use default memory for a hadoop client
        variables.remove(HADOOP_MEM_KEY);
      } else {
        // user specified the memory for local mode hadoop run
        console.printInfo(" set heap size\t" + hadoopMem + "MB");
        variables.put(HADOOP_MEM_KEY, String.valueOf(hadoopMem));
      }
      // } else {
      // nothing to do - we are not running in local mode - only submitting
      // the job via a child process. in this case it's appropriate that the
      // child jvm use the same memory as the parent jvm

      // }

      //Set HADOOP_USER_NAME env variable for child process, so that
      // it also runs with hadoop permissions for the user the job is running as
      // This will be used by hadoop only in unsecure(/non kerberos) mode
      String endUserName = Utils.getUGI().getShortUserName();
      LOG.debug("setting HADOOP_USER_NAME\t" + endUserName);
      variables.put("HADOOP_USER_NAME", endUserName);

      if (variables.containsKey(HADOOP_OPTS_KEY)) {
        variables.put(HADOOP_OPTS_KEY, variables.get(HADOOP_OPTS_KEY) + hadoopOpts);
      } else {
        variables.put(HADOOP_OPTS_KEY, hadoopOpts);
      }

      //For Windows OS, we need to pass HIVE_HADOOP_CLASSPATH Java parameter while starting
      //Hiveserver2 using "-hiveconf hive.hadoop.classpath=%HIVE_LIB%". This is to combine path(s).
      if (HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_HADOOP_CLASSPATH)!= null)
      {
        if (variables.containsKey("HADOOP_CLASSPATH"))
        {
          variables.put("HADOOP_CLASSPATH", variables.get("HADOOP_CLASSPATH") + ";" + HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_HADOOP_CLASSPATH));
        } else {
          variables.put("HADOOP_CLASSPATH", HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_HADOOP_CLASSPATH));
        }
      }

      if(variables.containsKey(MapRedTask.HIVE_DEBUG_RECURSIVE)) {
        MapRedTask.configureDebugVariablesForChildJVM(variables);
      }


      if(UserGroupInformation.isSecurityEnabled() &&
           UserGroupInformation.isLoginKeytabBased()) {
        //If kerberos security is enabled, and HS2 doAs is enabled,
        // then additional params need to be set so that the command is run as
        // intended user
        secureDoAs = new SecureCmdDoAs(conf);
        secureDoAs.addEnv(variables);
      }

      // If HIVE_LOCAL_TASK_CHILD_OPTS is set, child VM environment setting
      // HADOOP_CLIENT_OPTS will be replaced with HIVE_LOCAL_TASK_CHILD_OPTS.
      // HADOOP_OPTS is updated too since HADOOP_CLIENT_OPTS is appended
      // to HADOOP_OPTS in most cases. This way, the local task JVM can
      // have different settings from those of HiveServer2.
      if (variables.containsKey(HIVE_LOCAL_TASK_CHILD_OPTS_KEY)) {
        String childOpts = variables.get(HIVE_LOCAL_TASK_CHILD_OPTS_KEY);
        if (childOpts == null) {
          childOpts = "";
        }
        String clientOpts = variables.put(HADOOP_CLIENT_OPTS, childOpts);
        String tmp = variables.get(HADOOP_OPTS_KEY);
        if (tmp != null && !StringUtils.isBlank(clientOpts)) {
          tmp = tmp.replace(clientOpts, childOpts);
          variables.put(HADOOP_OPTS_KEY, tmp);
        }
      }

      env = new String[variables.size()];
      int pos = 0;
      for (Map.Entry<String, String> entry : variables.entrySet()) {
        String name = entry.getKey();
        String value = entry.getValue();
        env[pos++] = name + "=" + value;
        LOG.debug("Setting env: " + name + "=" + LogUtils.maskIfPassword(name, value));
      }

      LOG.info("Executing: " + cmdLine);

      // Run ExecDriver in another JVM
      executor = Runtime.getRuntime().exec(cmdLine, env, new File(workDir));

      CachingPrintStream errPrintStream = new CachingPrintStream(System.err);

      StreamPrinter outPrinter;
      StreamPrinter errPrinter;
      OperationLog operationLog = OperationLog.getCurrentOperationLog();
      if (operationLog != null) {
        outPrinter = new StreamPrinter(executor.getInputStream(), null, System.out,
            operationLog.getPrintStream());
        errPrinter = new StreamPrinter(executor.getErrorStream(), null, errPrintStream,
            operationLog.getPrintStream());
      } else {
        outPrinter = new StreamPrinter(executor.getInputStream(), null, System.out);
        errPrinter = new StreamPrinter(executor.getErrorStream(), null, errPrintStream);
      }

      outPrinter.start();
      errPrinter.start();

      int exitVal = jobExecHelper.progressLocal(executor, getId());

      // wait for stream threads to finish
      outPrinter.join();
      errPrinter.join();

      if (exitVal != 0) {
        LOG.error("Execution failed with exit status: " + exitVal);
        if (SessionState.get() != null) {
          SessionState.get().addLocalMapRedErrors(getId(), errPrintStream.getOutput());
        }
      } else {
        LOG.info("Execution completed successfully");
      }

      return exitVal;
    } catch (Exception e) {
      LOG.error("Exception: ", e);
      return (1);
    } finally {
      if (secureDoAs != null) {
        secureDoAs.close();
      }
    }
  }

  public int executeInProcess(DriverContext driverContext) {
    // check the local work
    if (work == null) {
      return -1;
    }

    if (execContext == null) {
      execContext = new ExecMapperContext(job);
    }

    memoryMXBean = ManagementFactory.getMemoryMXBean();
    long startTime = System.currentTimeMillis();
    console.printInfo(Utilities.now()
        + "\tStarting to launch local task to process map join;\tmaximum memory = "
        + memoryMXBean.getHeapMemoryUsage().getMax());
    execContext.setJc(job);
    // set the local work, so all the operator can get this context
    execContext.setLocalWork(work);
    try {
      startForward(null);
      long currentTime = System.currentTimeMillis();
      long elapsed = currentTime - startTime;
      console.printInfo(Utilities.now() + "\tEnd of local task; Time Taken: "
          + Utilities.showTime(elapsed) + " sec.");
    } catch (Throwable throwable) {
      if (throwable instanceof OutOfMemoryError
          || (throwable instanceof MapJoinMemoryExhaustionException)) {
        l4j.error("Hive Runtime Error: Map local work exhausted memory", throwable);
        return 3;
      } else {
        l4j.error("Hive Runtime Error: Map local work failed", throwable);
        return 2;
      }
    }
    return 0;
  }

  public void startForward(String bigTableBucket) throws Exception {
    boolean inputFileChangeSenstive = work.getInputFileChangeSensitive();
    initializeOperators(new HashMap<FetchOperator, JobConf>());
    // for each big table's bucket, call the start forward
    if (inputFileChangeSenstive) {
      for (Map<String, List<String>> bigTableBucketFiles : work
          .getBucketMapjoinContext().getAliasBucketFileNameMapping().values()) {
        if (bigTableBucket == null) {
          for (String bigTableBucketFile : bigTableBucketFiles.keySet()) {
            startForward(inputFileChangeSenstive, bigTableBucketFile);
          }
        } else if (bigTableBucketFiles.keySet().contains(bigTableBucket)) {
          startForward(inputFileChangeSenstive, bigTableBucket);
        }
      }
    } else {
      startForward(inputFileChangeSenstive, null);
    }
  }

  private void startForward(boolean inputFileChangeSenstive, String bigTableBucket)
      throws Exception {
    for (Operator<?> source : work.getAliasToWork().values()) {
      source.reset();
    }
    if (inputFileChangeSenstive) {
      execContext.setCurrentBigBucketFile(bigTableBucket);
    }
    for (Map.Entry<String, FetchOperator> entry : fetchOperators.entrySet()) {
      String alias = entry.getKey();
      FetchOperator fetchOp = entry.getValue();

      if (inputFileChangeSenstive) {
        fetchOp.clearFetchContext();
        setUpFetchOpContext(fetchOp, alias, bigTableBucket);
      }

      // get the root operator
      Operator<? extends OperatorDesc> forwardOp = work.getAliasToWork().get(alias);
      // walk through the operator tree
      while (!forwardOp.getDone()) {
        InspectableObject row = fetchOp.getNextRow();
        if (row == null) {
          break;
        }
        forwardOp.process(row.o, 0);
      }
      forwardOp.flush();
    }
    for (Operator<?> source : work.getAliasToWork().values()) {
      source.close(false);
    }
  }

  private void initializeOperators(Map<FetchOperator, JobConf> fetchOpJobConfMap)
      throws HiveException {
    for (Map.Entry<String, Operator<? extends OperatorDesc>> entry : work.getAliasToWork().entrySet()) {
      LOG.debug("initializeOperators: " +  entry.getKey() + ", children = "  + entry.getValue().getChildOperators());
    }
    // this mapper operator is used to initialize all the operators
    for (Map.Entry<String, FetchWork> entry : work.getAliasToFetchWork().entrySet()) {
      if (entry.getValue() == null) {
        continue;
      }
      JobConf jobClone = new JobConf(job);

      TableScanOperator ts = (TableScanOperator)work.getAliasToWork().get(entry.getKey());
      // push down projections
      ColumnProjectionUtils.appendReadColumns(
          jobClone, ts.getNeededColumnIDs(), ts.getNeededColumns(), ts.getNeededNestedColumnPaths());
      // push down filters
      HiveInputFormat.pushFilters(jobClone, ts);

      AcidUtils.setTransactionalTableScan(jobClone, ts.getConf().isAcidTable());
      AcidUtils.setAcidOperationalProperties(jobClone, ts.getConf().getAcidOperationalProperties());

      // create a fetch operator
      FetchOperator fetchOp = new FetchOperator(entry.getValue(), jobClone);
      fetchOpJobConfMap.put(fetchOp, jobClone);
      fetchOperators.put(entry.getKey(), fetchOp);
      l4j.info("fetchoperator for " + entry.getKey() + " created");
    }
    // initialize all forward operator
    for (Map.Entry<String, FetchOperator> entry : fetchOperators.entrySet()) {
      // get the forward op
      String alias = entry.getKey();
      Operator<? extends OperatorDesc> forwardOp = work.getAliasToWork().get(alias);

      // put the exe context into all the operators
      forwardOp.passExecContext(execContext);
      // All the operators need to be initialized before process
      FetchOperator fetchOp = entry.getValue();
      JobConf jobConf = fetchOpJobConfMap.get(fetchOp);

      if (jobConf == null) {
        jobConf = job;
      }
      // initialize the forward operator
      ObjectInspector objectInspector = fetchOp.getOutputObjectInspector();
      forwardOp.initialize(jobConf, new ObjectInspector[] {objectInspector});
      l4j.info("fetchoperator for " + entry.getKey() + " initialized");
    }
  }

  private void setUpFetchOpContext(FetchOperator fetchOp, String alias, String currentInputFile)
      throws Exception {

    BucketMapJoinContext bucketMatcherCxt = this.work.getBucketMapjoinContext();

    Class<? extends BucketMatcher> bucketMatcherCls = bucketMatcherCxt.getBucketMatcherClass();
    BucketMatcher bucketMatcher = ReflectionUtils.newInstance(bucketMatcherCls,
        null);
    bucketMatcher.setAliasBucketFileNameMapping(bucketMatcherCxt.getAliasBucketFileNameMapping());

    List<Path> aliasFiles = bucketMatcher.getAliasBucketFiles(currentInputFile, bucketMatcherCxt
        .getMapJoinBigTableAlias(), alias);
    fetchOp.setupContext(aliasFiles);
  }

  @Override
  public boolean isMapRedLocalTask() {
    return true;
  }

  @Override
  public Collection<Operator<? extends OperatorDesc>> getTopOperators() {
    return getWork().getAliasToWork().values();
  }

  @Override
  public String getName() {
    return "MAPREDLOCAL";
  }

  @Override
  public StageType getType() {
    //assert false;
    return StageType.MAPREDLOCAL;
  }

  @Override
  public void shutdown() {
    super.shutdown();
    if (executor != null) {
      executor.destroy();
      executor = null;
    }
  }
}
