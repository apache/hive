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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.CachingPrintStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.Utilities.StreamPrinter;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.ShimLoader;
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
  static final String HADOOP_CLIENT_OPTS = "HADOOP_CLIENT_OPTS";
  static final String HIVE_DEBUG_RECURSIVE = "HIVE_DEBUG_RECURSIVE";
  static final String HIVE_MAIN_CLIENT_DEBUG_OPTS = "HIVE_MAIN_CLIENT_DEBUG_OPTS";
  static final String HIVE_CHILD_CLIENT_DEBUG_OPTS = "HIVE_CHILD_CLIENT_DEBUG_OPTS";
  static final String[] HIVE_SYS_PROP = {"build.dir", "build.dir.hive"};

  private transient ContentSummary inputSummary = null;
  private transient boolean runningViaChild = false;

  private transient boolean inputSizeEstimated = false;
  private transient long totalInputFileSize;
  private transient long totalInputNumFiles;

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

        if (inputSummary == null) {
          inputSummary = Utilities.getInputSummary(driverContext.getCtx(), work, null);
        }

        // set the values of totalInputFileSize and totalInputNumFiles, estimating them
        // if percentage block sampling is being used
        estimateInputSize();

        // at this point the number of reducers is precisely defined in the plan
        int numReducers = work.getNumReduceTasks();

        if (LOG.isDebugEnabled()) {
          LOG.debug("Task: " + getId() + ", Summary: " +
                    totalInputFileSize + "," + totalInputNumFiles + ","
                    + numReducers);
        }

        String reason = MapRedTask.isEligibleForLocalMode(conf, numReducers,
            totalInputFileSize, totalInputNumFiles);
        if (reason == null) {
          // clone configuration before modifying it on per-task basis
          cloneConf();
          conf.setVar(HiveConf.ConfVars.HADOOPJT, "local");
          console.printInfo("Selecting local mode for task: " + getId());
          this.setLocalMode(true);
        } else {
          console.printInfo("Cannot run job locally: " + reason);
          this.setLocalMode(false);
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

      // we need to edit the configuration to setup cmdline. clone it first
      cloneConf();

      // propagate input format if necessary
      super.setInputAttributes(conf);

      // enable assertion
      String hadoopExec = conf.getVar(HiveConf.ConfVars.HADOOPBIN);
      String hiveJar = conf.getJar();

      String libJarsOption;
      String addedJars = Utilities.getResourceFiles(conf, SessionState.ResourceType.JAR);
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
      String hiveConfArgs = generateCmdLine(conf, ctx);

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

      String workDir = (new File(".")).getCanonicalPath();
      String files = Utilities.getResourceFiles(conf, SessionState.ResourceType.FILE);
      if (!files.isEmpty()) {
        cmdLine = cmdLine + " -files " + files;

        workDir = (new Path(ctx.getLocalTmpFileURI())).toUri().getPath();

        if (! (new File(workDir)).mkdir()) {
          throw new IOException ("Cannot create tmp working dir: " + workDir);
        }

        for (String f: StringUtils.split(files, ',')) {
          Path p = new Path(f);
          String target = p.toUri().getPath();
          String link = workDir + Path.SEPARATOR + p.getName();
          if (FileUtil.symLink(target, link) != 0) {
            throw new IOException ("Cannot link to added file: " + target + " from: " + link);
          }
        }
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

      if ("local".equals(conf.getVar(HiveConf.ConfVars.HADOOPJT))) {
        // if we are running in local mode - then the amount of memory used
        // by the child jvm can no longer default to the memory used by the
        // parent jvm
        int hadoopMem = conf.getIntVar(HiveConf.ConfVars.HIVEHADOOPMAXMEM);
        if (hadoopMem == 0) {
          // remove env var that would default child jvm to use parent's memory
          // as default. child jvm would use default memory for a hadoop client
          variables.remove(HADOOP_MEM_KEY);
        } else {
          // user specified the memory for local mode hadoop run
          variables.put(HADOOP_MEM_KEY, String.valueOf(hadoopMem));
        }
      } else {
        // nothing to do - we are not running in local mode - only submitting
        // the job via a child process. in this case it's appropriate that the
        // child jvm use the same memory as the parent jvm
      }

      if (variables.containsKey(HADOOP_OPTS_KEY)) {
        variables.put(HADOOP_OPTS_KEY, variables.get(HADOOP_OPTS_KEY)
            + hadoopOpts);
      } else {
        variables.put(HADOOP_OPTS_KEY, hadoopOpts);
      }

      if(variables.containsKey(HIVE_DEBUG_RECURSIVE)) {
        configureDebugVariablesForChildJVM(variables);
      }

      env = new String[variables.size()];
      int pos = 0;
      for (Map.Entry<String, String> entry : variables.entrySet()) {
        String name = entry.getKey();
        String value = entry.getValue();
        env[pos++] = name + "=" + value;
      }
      // Run ExecDriver in another JVM
      executor = Runtime.getRuntime().exec(cmdLine, env, new File(workDir));

      CachingPrintStream errPrintStream =
          new CachingPrintStream(SessionState.getConsole().getChildErrStream());

      StreamPrinter outPrinter = new StreamPrinter(
          executor.getInputStream(), null,
          SessionState.getConsole().getChildOutStream());
      StreamPrinter errPrinter = new StreamPrinter(
          executor.getErrorStream(), null,
          errPrintStream);

      outPrinter.start();
      errPrinter.start();

      int exitVal = jobExecHelper.progressLocal(executor, getId());

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
      e.printStackTrace();
      LOG.error("Exception: " + e.getMessage());
      return (1);
    } finally {
      try {
        // creating the context can create a bunch of files. So make
        // sure to clear it out
        if(ctxCreated) {
          ctx.clear();
        }

      } catch (Exception e) {
        LOG.error("Exception: " + e.getMessage());
      }
    }
  }

  static void configureDebugVariablesForChildJVM(Map<String, String> environmentVariables) {
    // this method contains various asserts to warn if environment variables are in a buggy state
    assert environmentVariables.containsKey(HADOOP_CLIENT_OPTS)
        && environmentVariables.get(HADOOP_CLIENT_OPTS) != null : HADOOP_CLIENT_OPTS
        + " environment variable must be set when JVM in debug mode";

    String hadoopClientOpts = environmentVariables.get(HADOOP_CLIENT_OPTS);

    assert environmentVariables.containsKey(HIVE_MAIN_CLIENT_DEBUG_OPTS)
        && environmentVariables.get(HIVE_MAIN_CLIENT_DEBUG_OPTS) != null : HIVE_MAIN_CLIENT_DEBUG_OPTS
        + " environment variable must be set when JVM in debug mode";

    assert hadoopClientOpts.contains(environmentVariables.get(HIVE_MAIN_CLIENT_DEBUG_OPTS)) : HADOOP_CLIENT_OPTS
        + " environment variable must contain debugging parameters, when JVM in debugging mode";

    assert "y".equals(environmentVariables.get(HIVE_DEBUG_RECURSIVE))
        || "n".equals(environmentVariables.get(HIVE_DEBUG_RECURSIVE)) : HIVE_DEBUG_RECURSIVE
        + " environment variable must be set to \"y\" or \"n\" when debugging";

    if (environmentVariables.get(HIVE_DEBUG_RECURSIVE).equals("y")) {
      // swap debug options in HADOOP_CLIENT_OPTS to those that the child JVM should have
      assert environmentVariables.containsKey(HIVE_CHILD_CLIENT_DEBUG_OPTS)
          && environmentVariables.get(HIVE_MAIN_CLIENT_DEBUG_OPTS) != null : HIVE_CHILD_CLIENT_DEBUG_OPTS
          + " environment variable must be set when JVM in debug mode";
      String newHadoopClientOpts = hadoopClientOpts.replace(
          environmentVariables.get(HIVE_MAIN_CLIENT_DEBUG_OPTS),
          environmentVariables.get(HIVE_CHILD_CLIENT_DEBUG_OPTS));
      environmentVariables.put(HADOOP_CLIENT_OPTS, newHadoopClientOpts);
    } else {
      // remove from HADOOP_CLIENT_OPTS any debug related options
      String newHadoopClientOpts = hadoopClientOpts.replace(
          environmentVariables.get(HIVE_MAIN_CLIENT_DEBUG_OPTS), "").trim();
      if (newHadoopClientOpts.isEmpty()) {
        environmentVariables.remove(HADOOP_CLIENT_OPTS);
      } else {
        environmentVariables.put(HADOOP_CLIENT_OPTS, newHadoopClientOpts);
      }
    }
    // child JVM won't need to change debug parameters when creating it's own children
    environmentVariables.remove(HIVE_DEBUG_RECURSIVE);
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

    if(inputSummary == null) {
      // compute the summary and stash it away
      inputSummary =  Utilities.getInputSummary(driverContext.getCtx(), work, null);
    }

    // if all inputs are sampled, we should shrink the size of reducers accordingly.
    estimateInputSize();

    if (totalInputFileSize != inputSummary.getLength()) {
      LOG.info("BytesPerReducer=" + bytesPerReducer + " maxReducers="
          + maxReducers + " estimated totalInputFileSize=" + totalInputFileSize);
    } else {
      LOG.info("BytesPerReducer=" + bytesPerReducer + " maxReducers="
        + maxReducers + " totalInputFileSize=" + totalInputFileSize);
    }

    int reducers = (int) ((totalInputFileSize + bytesPerReducer - 1) / bytesPerReducer);
    reducers = Math.max(1, reducers);
    reducers = Math.min(maxReducers, reducers);
    return reducers;
  }

  /**
   * Sets the values of totalInputFileSize and totalInputNumFiles.  If percentage
   * block sampling is used, these values are estimates based on the highest
   * percentage being used for sampling multiplied by the value obtained from the
   * input summary.  Otherwise, these values are set to the exact value obtained
   * from the input summary.
   *
   * Once the function completes, inputSizeEstimated is set so that the logic is
   * never run more than once.
   */
  private void estimateInputSize() {
    if (inputSizeEstimated) {
      // If we've already run this function, return
      return;
    }

    // Initialize the values to be those taken from the input summary
    totalInputFileSize = inputSummary.getLength();
    totalInputNumFiles = inputSummary.getFileCount();

    if (work.getNameToSplitSample() == null || work.getNameToSplitSample().isEmpty()) {
      // If percentage block sampling wasn't used, we don't need to do any estimation
      inputSizeEstimated = true;
      return;
    }

    // if all inputs are sampled, we should shrink the size of the input accordingly
    double highestSamplePercentage = 0;
    boolean allSample = false;
    for (String alias : work.getAliasToWork().keySet()) {
      if (work.getNameToSplitSample().containsKey(alias)) {
        allSample = true;
        double rate = work.getNameToSplitSample().get(alias).getPercent();
        if (rate > highestSamplePercentage) {
          highestSamplePercentage = rate;
        }
      } else {
        allSample = false;
        break;
      }
    }
    if (allSample) {
      // This is a little bit dangerous if inputs turns out not to be able to be sampled.
      // In that case, we significantly underestimate the input.
      // It's the same as estimateNumberOfReducers(). It's just our best
      // guess and there is no guarantee.
      totalInputFileSize = Math.min((long) (totalInputFileSize * highestSamplePercentage / 100D)
          , totalInputFileSize);
      totalInputNumFiles = Math.min((long) (totalInputNumFiles * highestSamplePercentage / 100D)
          , totalInputNumFiles);
    }

    inputSizeEstimated = true;
  }

  /**
   * Find out if a job can be run in local mode based on it's characteristics
   *
   * @param conf Hive Configuration
   * @param numReducers total number of reducers for this job
   * @param inputLength the size of the input
   * @param inputFileCount the number of files of input
   * @return String null if job is eligible for local mode, reason otherwise
   */
  public static String isEligibleForLocalMode(HiveConf conf,
                                              int numReducers,
                                              long inputLength,
                                              long inputFileCount) {

    long maxBytes = conf.getLongVar(HiveConf.ConfVars.LOCALMODEMAXBYTES);
    long maxInputFiles = conf.getIntVar(HiveConf.ConfVars.LOCALMODEMAXINPUTFILES);

    // check for max input size
    if (inputLength > maxBytes) {
      return "Input Size (= " + inputLength + ") is larger than " +
        HiveConf.ConfVars.LOCALMODEMAXBYTES.varname + " (= " + maxBytes + ")";
    }

    // ideally we would like to do this check based on the number of splits
    // in the absence of an easy way to get the number of splits - do this
    // based on the total number of files (pessimistically assumming that
    // splits are equal to number of files in worst case)
    if (inputFileCount > maxInputFiles) {
      return "Number of Input Files (= " + inputFileCount +
        ") is larger than " +
        HiveConf.ConfVars.LOCALMODEMAXINPUTFILES.varname + "(= " + maxInputFiles + ")";
    }

    // since local mode only runs with 1 reducers - make sure that the
    // the number of reducers (set by user or inferred) is <=1
    if (numReducers > 1) {
      return "Number of reducers (= " + numReducers + ") is more than 1";
    }

    return null;
  }

  @Override
  public Operator<? extends Serializable> getReducer() {
    return getWork().getReducer();
  }
}
