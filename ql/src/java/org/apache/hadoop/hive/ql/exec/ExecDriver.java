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

import java.io.*;
import java.util.*;
import java.net.URI;
import java.net.URLEncoder;
import java.net.URLDecoder;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.history.HiveHistory.Keys;
import org.apache.hadoop.hive.ql.io.*;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.varia.NullAppender;

public class ExecDriver extends Task<mapredWork> implements Serializable {

  private static final long serialVersionUID = 1L;

  transient protected JobConf job;

  /**
   * Constructor when invoked from QL
   */
  public ExecDriver() {
    super();
  }

  public static String getRealFiles(Configuration conf) {
    // fill in local files to be added to the task environment
    SessionState ss = SessionState.get();
    Set<String> files = (ss == null) ? null : ss.list_resource(
        SessionState.ResourceType.FILE, null);
    if (files != null) {
      ArrayList<String> realFiles = new ArrayList<String>(files.size());
      for (String one : files) {
        try {
          realFiles.add(Utilities.realFile(one, conf));
        } catch (IOException e) {
          throw new RuntimeException("Cannot validate file " + one
              + "due to exception: " + e.getMessage(), e);
        }
      }
      return StringUtils.join(realFiles, ",");
    } else {
      return "";
    }
  }

  /**
   * Initialization when invoked from QL
   */
  public void initialize(HiveConf conf) {
    super.initialize(conf);
    job = new JobConf(conf, ExecDriver.class);
    String realFiles = getRealFiles(job);
    if (realFiles != null && realFiles.length() > 0) {
      job.set("tmpfiles", realFiles);

      // workaround for hadoop-17 - jobclient only looks at commandlineconfig
      Configuration commandConf = JobClient.getCommandLineConfig();
      if (commandConf != null) {
        commandConf.set("tmpfiles", realFiles);
      }
    }
  }

  /**
   * Constructor/Initialization for invocation as independent utility
   */
  public ExecDriver(mapredWork plan, JobConf job, boolean isSilent)
      throws HiveException {
    setWork(plan);
    this.job = job;
    LOG = LogFactory.getLog(this.getClass().getName());
    console = new LogHelper(LOG, isSilent);
  }

  /**
   * A list of the currently running jobs spawned in this Hive instance that is
   * used to kill all running jobs in the event of an unexpected shutdown -
   * i.e., the JVM shuts down while there are still jobs running.
   */
  public static HashMap<String, String> runningJobKillURIs = new HashMap<String, String>();

  /**
   * In Hive, when the user control-c's the command line, any running jobs
   * spawned from that command line are best-effort killed.
   * 
   * This static constructor registers a shutdown thread to iterate over all the
   * running job kill URLs and do a get on them.
   * 
   */
  static {
    if (new org.apache.hadoop.conf.Configuration().getBoolean(
        "webinterface.private.actions", false)) {
      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          for (Iterator<String> elems = runningJobKillURIs.values().iterator(); elems
              .hasNext();) {
            String uri = elems.next();
            try {
              System.err.println("killing job with: " + uri);
              int retCode = ((java.net.HttpURLConnection) new java.net.URL(uri)
                  .openConnection()).getResponseCode();
              if (retCode != 200) {
                System.err.println("Got an error trying to kill job with URI: "
                    + uri + " = " + retCode);
              }
            } catch (Exception e) {
              System.err.println("trying to kill job, caught: " + e);
              // do nothing
            }
          }
        }
      });
    }
  }

  /**
   * from StreamJob.java
   */
  public void jobInfo(RunningJob rj) {
    if (job.get("mapred.job.tracker", "local").equals("local")) {
      console.printInfo("Job running in-process (local Hadoop)");
    } else {
      String hp = job.get("mapred.job.tracker");
      if (SessionState.get() != null) {
        SessionState.get().getHiveHistory().setTaskProperty(
            SessionState.get().getQueryId(), getId(),
            Keys.TASK_HADOOP_ID, rj.getJobID());
      }
      console.printInfo("Starting Job = " + rj.getJobID() + ", Tracking URL = "
          + rj.getTrackingURL());
      console.printInfo("Kill Command = "
          + HiveConf.getVar(job, HiveConf.ConfVars.HADOOPBIN)
          + " job  -Dmapred.job.tracker=" + hp + " -kill " + rj.getJobID());
    }
  }

  /**
   * from StreamJob.java
   */
  public RunningJob jobProgress(JobClient jc, RunningJob rj) throws IOException {
    String lastReport = "";
    while (!rj.isComplete()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
      rj = jc.getJob(rj.getJobID());
      String report = null;
      report = " map = " + Math.round(rj.mapProgress() * 100) + "%,  reduce ="
          + Math.round(rj.reduceProgress() * 100) + "%";

      if (!report.equals(lastReport)) {

        SessionState ss = SessionState.get();
        if (ss != null) {
          ss.getHiveHistory().setTaskCounters(
              SessionState.get().getQueryId(), getId(), rj);
          ss.getHiveHistory().setTaskProperty(
              SessionState.get().getQueryId(), getId(),
              Keys.TASK_HADOOP_PROGRESS, report);
          ss.getHiveHistory().progressTask(
              SessionState.get().getQueryId(), this);
        }
        console.printInfo(report);
        lastReport = report;
      }
    }
    return rj;
  }

  /**
   * Estimate the number of reducers needed for this job, based on job input,
   * and configuration parameters.
   * @return the number of reducers.
   */
  public int estimateNumberOfReducers(HiveConf hive, JobConf job, mapredWork work) throws IOException {
    if (hive == null) {
      hive = new HiveConf();
    }
    long bytesPerReducer = hive.getLongVar(HiveConf.ConfVars.BYTESPERREDUCER);
    int maxReducers = hive.getIntVar(HiveConf.ConfVars.MAXREDUCERS);
    long totalInputFileSize = getTotalInputFileSize(job, work);

    LOG.info("BytesPerReducer=" + bytesPerReducer + " maxReducers=" + maxReducers 
        + " totalInputFileSize=" + totalInputFileSize);

    int reducers = (int)((totalInputFileSize + bytesPerReducer - 1) / bytesPerReducer);
    reducers = Math.max(1, reducers);
    reducers = Math.min(maxReducers, reducers);
    return reducers;    
  }

  /**
   * Set the number of reducers for the mapred work.
   */
  protected void setNumberOfReducers() throws IOException {
    // this is a temporary hack to fix things that are not fixed in the compiler
    Integer numReducersFromWork = work.getNumReduceTasks();
    
    if(work.getReducer() == null) {
      console.printInfo("Number of reduce tasks is set to 0 since there's no reduce operator");
      work.setNumReduceTasks(Integer.valueOf(0));
    } else {
      if (numReducersFromWork >= 0) {
        console.printInfo("Number of reduce tasks determined at compile time: " + work.getNumReduceTasks()); 
      } else if (job.getNumReduceTasks() > 0) {
        int reducers = job.getNumReduceTasks();
        work.setNumReduceTasks(reducers);
        console.printInfo("Number of reduce tasks not specified. Defaulting to jobconf value of: " + reducers);
      } else {
        int reducers = estimateNumberOfReducers(conf, job, work);
        work.setNumReduceTasks(reducers);
        console.printInfo("Number of reduce tasks not specified. Estimated from input data size: " + reducers);

      }
      console.printInfo("In order to change the average load for a reducer (in bytes):");
      console.printInfo("  set " + HiveConf.ConfVars.BYTESPERREDUCER.varname + "=<number>");
      console.printInfo("In order to limit the maximum number of reducers:");
      console.printInfo("  set " + HiveConf.ConfVars.MAXREDUCERS.varname + "=<number>");
      console.printInfo("In order to set a constant number of reducers:");
      console.printInfo("  set " + HiveConf.ConfVars.HADOOPNUMREDUCERS + "=<number>");
    }
  }


  /**
   * Add new elements to the classpath
   * 
   * @param newPaths
   *          Array of classpath elements
   */
  private static void addToClassPath(String[] newPaths) throws Exception {
    Thread curThread = Thread.currentThread();
    URLClassLoader loader = (URLClassLoader) curThread.getContextClassLoader();
    List<URL> curPath = Arrays.asList(loader.getURLs());
    ArrayList<URL> newPath = new ArrayList<URL>();

    for (String onestr : newPaths) {
      URL oneurl = (new File(onestr)).toURL();
      if (!curPath.contains(oneurl)) {
        newPath.add(oneurl);
      }
    }

    loader = new URLClassLoader(newPath.toArray(new URL[0]), loader);
    curThread.setContextClassLoader(loader);
  }

  /**
   * Calculate the total size of input files.
   * @param job the hadoop job conf.
   * @return the total size in bytes.
   * @throws IOException 
   */
  public long getTotalInputFileSize(JobConf job, mapredWork work) throws IOException {
    long r = 0;
    FileSystem fs = FileSystem.get(job);
    // For each input path, calculate the total size.
    for (String path: work.getPathToAliases().keySet()) {
      try {
        ContentSummary cs = fs.getContentSummary(new Path(path));
        r += cs.getLength();
      } catch (IOException e) {
        LOG.info("Cannot get size of " + path + ". Safely ignored.");
      }
    }
    return r;
  }

  /**
   * Execute a query plan using Hadoop
   */
  public int execute() {

    try {
      setNumberOfReducers();
    } catch(IOException e) {
      String statusMesg = "IOException while accessing HDFS to estimate the number of reducers: "
        + e.getMessage();
      console.printError(statusMesg, "\n"
                         + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return 1;
    }

    String invalidReason = work.isInvalid();
    if (invalidReason != null) {
      throw new RuntimeException("Plan invalid, Reason: " + invalidReason);
    }

    Utilities.setMapRedWork(job, work);

    for (String onefile : work.getPathToAliases().keySet()) {
      LOG.info("Adding input file " + onefile);
      FileInputFormat.addInputPaths(job, onefile);
    }

    String hiveScratchDir = HiveConf.getVar(job, HiveConf.ConfVars.SCRATCHDIR);
    String jobScratchDir = hiveScratchDir + Utilities.randGen.nextInt();
    FileOutputFormat.setOutputPath(job, new Path(jobScratchDir));
    job.setMapperClass(ExecMapper.class);

    job.setMapOutputKeyClass(HiveKey.class);
    // LazySimpleSerDe writes to Text
    // Revert to DynamicSerDe: job.setMapOutputValueClass(BytesWritable.class); 
    job.setMapOutputValueClass(Text.class);

    job.setNumReduceTasks(work.getNumReduceTasks().intValue());
    job.setReducerClass(ExecReducer.class);

    job.setInputFormat(org.apache.hadoop.hive.ql.io.HiveInputFormat.class);

    // No-Op - we don't really write anything here ..
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    String auxJars = HiveConf.getVar(job, HiveConf.ConfVars.HIVEAUXJARS);
    if (StringUtils.isNotBlank(auxJars)) {
      LOG.info("adding libjars: " + auxJars);
      job.set("tmpjars", auxJars);
    }

    int returnVal = 0;
    FileSystem fs = null;
    RunningJob rj = null, orig_rj = null;
    boolean success = false;

    try {
      fs = FileSystem.get(job);

      // if the input is empty exit gracefully
      Path[] inputPaths = FileInputFormat.getInputPaths(job);
      boolean emptyInput = true;
      for (Path inputP : inputPaths) {
        if (!fs.exists(inputP))
          continue;

        FileStatus[] fStats = fs.listStatus(inputP);
        for (FileStatus fStat : fStats) {
          if (fStat.getLen() > 0) {
            emptyInput = false;
            break;
          }
        }
      }

      if (emptyInput) {
        console.printInfo("Job need not be submitted: no output: Success");
        return 0;
      }

      // remove the pwd from conf file so that job tracker doesn't show this logs
      String pwd = job.get(HiveConf.ConfVars.METASTOREPWD.varname);
      if (pwd != null)
        job.set(HiveConf.ConfVars.METASTOREPWD.varname, "HIVE");
      JobClient jc = new JobClient(job);

      // make this client wait if job trcker is not behaving well.
      Throttle.checkJobTracker(job, LOG);

      orig_rj = rj = jc.submitJob(job);
      // replace it back
      if (pwd != null)
        job.set(HiveConf.ConfVars.METASTOREPWD.varname, pwd);

      // add to list of running jobs so in case of abnormal shutdown can kill
      // it.
      runningJobKillURIs.put(rj.getJobID(), rj.getTrackingURL()
          + "&action=kill");

      jobInfo(rj);
      rj = jobProgress(jc, rj);

      if(rj == null) {
        // in the corner case where the running job has disappeared from JT memory
        // remember that we did actually submit the job.
        rj = orig_rj;
        success = false;
      } else {
        success = rj.isSuccessful();
      }

      String statusMesg = "Ended Job = " + rj.getJobID();
      if (!success) {
        statusMesg += " with errors";
        returnVal = 2;
        console.printError(statusMesg);
      } else {
        console.printInfo(statusMesg);
      }
    } catch (Exception e) {
      String mesg = " with exception '" + Utilities.getNameMessage(e) + "'";
      if (rj != null) {
        mesg = "Ended Job = " + rj.getJobID() + mesg;
      } else {
        mesg = "Job Submission failed" + mesg;
      }
      // Has to use full name to make sure it does not conflict with
      // org.apache.commons.lang.StringUtils
      console.printError(mesg, "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));

      success = false;
      returnVal = 1;
    } finally {
      Utilities.clearMapRedWork(job);
      try {
        fs.delete(new Path(jobScratchDir), true);
        if (returnVal != 0 && rj != null) {
          rj.killJob();
        }
        runningJobKillURIs.remove(rj.getJobID());
      } catch (Exception e) {
      }
    }

    try {
      if (rj != null) {
        if(work.getAliasToWork() != null) {
          for(Operator<? extends Serializable> op:
                work.getAliasToWork().values()) {
            op.jobClose(job, success);
          }
        }
        if(work.getReducer() != null) {
          work.getReducer().jobClose(job, success);
        }
      }
    } catch (Exception e) {
      // jobClose needs to execute successfully otherwise fail task
      if(success) {
        success = false;
        returnVal = 3;
        String mesg = "Job Commit failed with exception '" + Utilities.getNameMessage(e) + "'";
        console.printError(mesg, "\n"
                           + org.apache.hadoop.util.StringUtils.stringifyException(e));
      }
    }

    return (returnVal);
  }

  private static void printUsage() {
    System.out
        .println("ExecDriver -plan <plan-file> [-jobconf k1=v1 [-jobconf k2=v2] ...] "
            + "[-files <file1>[,<file2>] ...]");
    System.exit(1);
  }

  public static void main(String[] args) throws IOException, HiveException {

    String planFileName = null;
    ArrayList<String> jobConfArgs = new ArrayList<String>();
    boolean isSilent = false;
    String files = null;

    try {
      for (int i = 0; i < args.length; i++) {
        if (args[i].equals("-plan")) {
          planFileName = args[++i];
          System.out.println("plan = " + planFileName);
        } else if (args[i].equals("-jobconf")) {
          jobConfArgs.add(args[++i]);
        } else if (args[i].equals("-silent")) {
          isSilent = true;
        } else if (args[i].equals("-files")) {
          files = args[++i];
        }
      }
    } catch (IndexOutOfBoundsException e) {
      System.err.println("Missing argument to option");
      printUsage();
    }

    // If started from main(), and isSilent is on, we should not output
    // any logs.
    if (isSilent) {
      BasicConfigurator.resetConfiguration();
      BasicConfigurator.configure(new NullAppender());
    }
    
    if (planFileName == null) {
      System.err.println("Must specify Plan File Name");
      printUsage();
    }

    JobConf conf = new JobConf(ExecDriver.class);
    for (String one : jobConfArgs) {
      int eqIndex = one.indexOf('=');
      if (eqIndex != -1) {
        try {
          conf.set(one.substring(0, eqIndex), URLDecoder.decode(one
              .substring(eqIndex + 1), "UTF-8"));
        } catch (UnsupportedEncodingException e) {
          System.err.println("Unexpected error " + e.getMessage()
              + " while encoding " + one.substring(eqIndex + 1));
          System.exit(3);
        }
      }
    }

    if (files != null) {
      conf.set("tmpfiles", files);
    }

    URI pathURI = (new Path(planFileName)).toUri();
    InputStream pathData;
    if (StringUtils.isEmpty(pathURI.getScheme())) {
      // default to local file system
      pathData = new FileInputStream(planFileName);
    } else {
      // otherwise may be in hadoop ..
      FileSystem fs = FileSystem.get(conf);
      pathData = fs.open(new Path(planFileName));
    }

    // workaround for hadoop-17 - libjars are not added to classpath. this
    // affects local
    // mode execution
    boolean localMode = HiveConf.getVar(conf, HiveConf.ConfVars.HADOOPJT)
        .equals("local");
    if (localMode) {
      String auxJars = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEAUXJARS);
      if (StringUtils.isNotBlank(auxJars)) {
        try {
          addToClassPath(StringUtils.split(auxJars, ","));
        } catch (Exception e) {
          throw new HiveException(e.getMessage(), e);
        }
      }
    }

    mapredWork plan = Utilities.deserializeMapRedWork(pathData);
    ExecDriver ed = new ExecDriver(plan, conf, isSilent);
    int ret = ed.execute();
    if (ret != 0) {
      System.out.println("Job Failed");
      System.exit(2);
    }
  }

  /**
   * Given a Hive Configuration object - generate a command line fragment for
   * passing such configuration information to ExecDriver
   */
  public static String generateCmdLine(HiveConf hconf) {
    try {
      StringBuilder sb = new StringBuilder();
      Properties deltaP = hconf.getChangedProperties();
      boolean localMode = hconf.getVar(HiveConf.ConfVars.HADOOPJT).equals(
          "local");
      String hadoopSysDir = "mapred.system.dir";
      String hadoopWorkDir = "mapred.local.dir";

      for (Object one : deltaP.keySet()) {
        String oneProp = (String) one;

        if (localMode
            && (oneProp.equals(hadoopSysDir) || oneProp.equals(hadoopWorkDir)))
          continue;

        String oneValue = deltaP.getProperty(oneProp);

        sb.append("-jobconf ");
        sb.append(oneProp);
        sb.append("=");
        sb.append(URLEncoder.encode(oneValue, "UTF-8"));
        sb.append(" ");
      }

      // Multiple concurrent local mode job submissions can cause collisions in
      // working dirs
      // Workaround is to rename map red working dir to a temp dir in such a
      // case
      if (localMode) {
        sb.append("-jobconf ");
        sb.append(hadoopSysDir);
        sb.append("=");
        sb.append(URLEncoder.encode(hconf.get(hadoopSysDir) + "/"
            + Utilities.randGen.nextInt(), "UTF-8"));

        sb.append(" ");
        sb.append("-jobconf ");
        sb.append(hadoopWorkDir);
        sb.append("=");
        sb.append(URLEncoder.encode(hconf.get(hadoopWorkDir) + "/"
            + Utilities.randGen.nextInt(), "UTF-8"));
      }

      return sb.toString();
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isMapRedTask() {
    return true;
  }

  @Override
  public boolean hasReduce() {
    mapredWork w = getWork();
    return w.getReducer() != null;
  }
}
