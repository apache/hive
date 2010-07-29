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

package org.apache.hadoop.hive.ql.exec;

import java.io.OutputStream;
import java.io.Serializable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.Utilities.StreamPrinter;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;

/**
 * Extension of ExecDriver:
 * - can optionally spawn a map-reduce task from a separate jvm
 * - will make last minute adjustments to map-reduce job parameters, viz:
 *   * estimating number of reducers
 *   * estimating whether job should run locally
 **/
public class MapRedTask extends ExecDriver implements Serializable {

  private static final long serialVersionUID = 1L;

  static final String HADOOP_MEM_KEY = "HADOOP_HEAPSIZE";
  static final String HADOOP_OPTS_KEY = "HADOOP_OPTS";
  static final String[] HIVE_SYS_PROP = {"build.dir", "build.dir.hive"};

  private transient ContentSummary inputSummary = null;
  private transient boolean runningViaChild = false;

  public MapRedTask() {
    super();
  }

  public MapRedTask(MapredWork plan, JobConf job, boolean isSilent) throws HiveException {
    throw new RuntimeException("Illegal Constructor call");
  }

  @Override
  public int execute(DriverContext driverContext) {

    Context ctx = driverContext.getCtx();
    boolean ctxCreated = false;

    try {
      if (ctx == null) {
        ctx = new Context(conf);
        ctxCreated = true;
      }

      // estimate number of reducers
      setNumberOfReducers();

      // auto-determine local mode if allowed
      if (!ctx.isLocalOnlyExecutionMode() &&
          conf.getBoolVar(HiveConf.ConfVars.LOCALMODEAUTO)) {

        if (inputSummary == null)
          inputSummary = Utilities.getInputSummary(driverContext.getCtx(), work, null);

        // at this point the number of reducers is precisely defined in the plan
        int numReducers = work.getNumReduceTasks();

        if (LOG.isDebugEnabled()) {
          LOG.debug("Task: " + getId() + ", Summary: " + 
                    inputSummary.getLength() + "," + inputSummary.getFileCount() + ","
                    + numReducers);
        }

	String reason = MapRedTask.isEligibleForLocalMode(conf, inputSummary, numReducers);
        if (reason == null) {
	  // set the JT to local for the duration of this job
          ctx.setOriginalTracker(conf.getVar(HiveConf.ConfVars.HADOOPJT));
          conf.setVar(HiveConf.ConfVars.HADOOPJT, "local");
          console.printInfo("Selecting local mode for task: " + getId());
        } else {
          console.printInfo("Cannot run job locally: " + reason);
	}
      }

      runningViaChild =
        "local".equals(conf.getVar(HiveConf.ConfVars.HADOOPJT)) ||
        conf.getBoolVar(HiveConf.ConfVars.SUBMITVIACHILD);

      if(!runningViaChild) {
        // we are not running this mapred task via child jvm
        // so directly invoke ExecDriver
        return super.execute(driverContext);
      }

      // enable assertion
      String hadoopExec = conf.getVar(HiveConf.ConfVars.HADOOPBIN);
      String hiveJar = conf.getJar();

      String libJarsOption;
      String addedJars = getResourceFiles(conf, SessionState.ResourceType.JAR);
      conf.setVar(ConfVars.HIVEADDEDJARS, addedJars);
      String auxJars = conf.getAuxJars();
      // Put auxjars and addedjars together into libjars
      if (StringUtils.isEmpty(addedJars)) {
        if (StringUtils.isEmpty(auxJars)) {
          libJarsOption = " ";
        } else {
          libJarsOption = " -libjars " + auxJars + " ";
        }
      } else {
        if (StringUtils.isEmpty(auxJars)) {
          libJarsOption = " -libjars " + addedJars + " ";
        } else {
          libJarsOption = " -libjars " + addedJars + "," + auxJars + " ";
        }
      }
      // Generate the hiveConfArgs after potentially adding the jars
      String hiveConfArgs = generateCmdLine(conf);

      // write out the plan to a local file
      Path planPath = new Path(ctx.getLocalTmpFileURI(), "plan.xml");
      OutputStream out = FileSystem.getLocal(conf).create(planPath);
      MapredWork plan = getWork();
      LOG.info("Generating plan file " + planPath.toString());
      Utilities.serializeMapRedWork(plan, out);

      String isSilent = "true".equalsIgnoreCase(System
          .getProperty("test.silent")) ? "-nolog" : "";

      String jarCmd;
      if (ShimLoader.getHadoopShims().usesJobShell()) {
        jarCmd = libJarsOption + hiveJar + " " + ExecDriver.class.getName();
      } else {
        jarCmd = hiveJar + " " + ExecDriver.class.getName() + libJarsOption;
      }

      String cmdLine = hadoopExec + " jar " + jarCmd + " -plan "
          + planPath.toString() + " " + isSilent + " " + hiveConfArgs;

      String files = getResourceFiles(conf, SessionState.ResourceType.FILE);
      if (!files.isEmpty()) {
        cmdLine = cmdLine + " -files " + files;
      }

      LOG.info("Executing: " + cmdLine);
      Process executor = null;

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
      Map<String, String> variables = new HashMap(System.getenv());
      // The user can specify the hadoop memory
      int hadoopMem = conf.getIntVar(HiveConf.ConfVars.HIVEHADOOPMAXMEM);
      if (hadoopMem == 0) {
        variables.remove(HADOOP_MEM_KEY);
      } else {
        // user specified the memory - only applicable for local mode
        variables.put(HADOOP_MEM_KEY, String.valueOf(hadoopMem));
      }
      if (variables.containsKey(HADOOP_OPTS_KEY)) {
        variables.put(HADOOP_OPTS_KEY, variables.get(HADOOP_OPTS_KEY)
            + hadoopOpts);
      } else {
        variables.put(HADOOP_OPTS_KEY, hadoopOpts);
      }
      env = new String[variables.size()];
      int pos = 0;
      for (Map.Entry<String, String> entry : variables.entrySet()) {
        String name = entry.getKey();
        String value = entry.getValue();
        env[pos++] = name + "=" + value;
      }
      // Run ExecDriver in another JVM
      executor = Runtime.getRuntime().exec(cmdLine, env);

      StreamPrinter outPrinter = new StreamPrinter(executor.getInputStream(),
          null, System.out);
      StreamPrinter errPrinter = new StreamPrinter(executor.getErrorStream(),
          null, System.err);

      outPrinter.start();
      errPrinter.start();

      int exitVal = executor.waitFor();

      if (exitVal != 0) {
        LOG.error("Execution failed with exit status: " + exitVal);
      } else {
        LOG.info("Execution completed successfully");
      }

      return exitVal;
    } catch (Exception e) {
      e.printStackTrace();
      LOG.error("Exception: " + e.getMessage());
      return (1);
    } finally {
      try {
        // in case we decided to run everything in local mode, restore the
        // the jobtracker setting to its initial value
        ctx.restoreOriginalTracker();

        // creating the context can create a bunch of files. So make
        // sure to clear it out
        if(ctxCreated) 
          ctx.clear();

      } catch (Exception e) {
        LOG.error("Exception: " + e.getMessage());
      }
    }
  }

  @Override
  public boolean mapStarted() {
    boolean b = super.mapStarted();
    return runningViaChild ? isdone : b;
  }

  @Override
  public boolean reduceStarted() {
    boolean b = super.reduceStarted();
    return runningViaChild ? isdone : b;
  }

  @Override
  public boolean mapDone() {
    boolean b = super.mapDone();
    return runningViaChild ? isdone : b;
  }

  @Override
  public boolean reduceDone() {
    boolean b = super.reduceDone();
    return runningViaChild ? isdone : b;
  }

  /**
   * Set the number of reducers for the mapred work.
   */
  private void setNumberOfReducers() throws IOException {
    // this is a temporary hack to fix things that are not fixed in the compiler
    Integer numReducersFromWork = work.getNumReduceTasks();

    if (work.getReducer() == null) {
      console
          .printInfo("Number of reduce tasks is set to 0 since there's no reduce operator");
      work.setNumReduceTasks(Integer.valueOf(0));
    } else {
      if (numReducersFromWork >= 0) {
        console.printInfo("Number of reduce tasks determined at compile time: "
            + work.getNumReduceTasks());
      } else if (job.getNumReduceTasks() > 0) {
        int reducers = job.getNumReduceTasks();
        work.setNumReduceTasks(reducers);
        console
            .printInfo("Number of reduce tasks not specified. Defaulting to jobconf value of: "
            + reducers);
      } else {
        int reducers = estimateNumberOfReducers();
        work.setNumReduceTasks(reducers);
        console
            .printInfo("Number of reduce tasks not specified. Estimated from input data size: "
            + reducers);

      }
      console
          .printInfo("In order to change the average load for a reducer (in bytes):");
      console.printInfo("  set " + HiveConf.ConfVars.BYTESPERREDUCER.varname
          + "=<number>");
      console.printInfo("In order to limit the maximum number of reducers:");
      console.printInfo("  set " + HiveConf.ConfVars.MAXREDUCERS.varname
          + "=<number>");
      console.printInfo("In order to set a constant number of reducers:");
      console.printInfo("  set " + HiveConf.ConfVars.HADOOPNUMREDUCERS
          + "=<number>");
    }
  }

  /**
   * Estimate the number of reducers needed for this job, based on job input,
   * and configuration parameters.
   *
   * @return the number of reducers.
   */
  private int estimateNumberOfReducers() throws IOException {
    long bytesPerReducer = conf.getLongVar(HiveConf.ConfVars.BYTESPERREDUCER);
    int maxReducers = conf.getIntVar(HiveConf.ConfVars.MAXREDUCERS);

    if(inputSummary == null)
      // compute the summary and stash it away
      inputSummary =  Utilities.getInputSummary(driverContext.getCtx(), work, null);

    long totalInputFileSize = inputSummary.getLength();

    LOG.info("BytesPerReducer=" + bytesPerReducer + " maxReducers="
        + maxReducers + " totalInputFileSize=" + totalInputFileSize);

    int reducers = (int) ((totalInputFileSize + bytesPerReducer - 1) / bytesPerReducer);
    reducers = Math.max(1, reducers);
    reducers = Math.min(maxReducers, reducers);
    return reducers;
  }

  /**
   * Find out if a job can be run in local mode based on it's characteristics
   *
   * @param conf Hive Configuration
   * @param inputSummary summary about the input files for this job
   * @param numReducers total number of reducers for this job
   * @return String null if job is eligible for local mode, reason otherwise
   */
  public static String isEligibleForLocalMode(HiveConf conf,
                                               ContentSummary inputSummary,
                                               int numReducers) {

    long maxBytes = conf.getLongVar(HiveConf.ConfVars.LOCALMODEMAXBYTES);
    long maxTasks = conf.getIntVar(HiveConf.ConfVars.LOCALMODEMAXTASKS);

    // check for max input size
    if (inputSummary.getLength() > maxBytes)
	return "Input Size (= " + maxBytes + ") is larger than " +
	    HiveConf.ConfVars.LOCALMODEMAXBYTES.varname + " (= " + maxBytes + ")";

    // ideally we would like to do this check based on the number of splits
    // in the absence of an easy way to get the number of splits - do this
    // based on the total number of files (pessimistically assumming that
    // splits are equal to number of files in worst case)
    if (inputSummary.getFileCount() > maxTasks)
	return "Number of Input Files (= " + inputSummary.getFileCount() +
	    ") is larger than " + 
	    HiveConf.ConfVars.LOCALMODEMAXTASKS.varname + "(= " + maxTasks + ")";

    // since local mode only runs with 1 reducers - make sure that the
    // the number of reducers (set by user or inferred) is <=1
    if (numReducers > 1) 
	return "Number of reducers (= " + numReducers + ") is more than 1";

    return null;
  }
}
