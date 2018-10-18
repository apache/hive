/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.templeton.tool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.HadoopShims.WebHCatJTShim;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.hcatalog.templeton.AppConfig;
import org.apache.hive.hcatalog.templeton.BadParam;
import org.apache.hive.hcatalog.templeton.LauncherDelegator;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Note that this class is used in a different JVM than WebHCat server.  Thus it should not call 
 * any classes not available on every node in the cluster (outside webhcat jar).
 * TempletonControllerJob#run() calls Job.setJarByClass(LaunchMapper.class) which
 * causes webhcat jar to be shipped to target node, but not it's transitive closure.
 * Long term we need to clean up this separation and create a separate jar to ship so that the
 * dependencies are clear.  (This used to be an inner class of TempletonControllerJob)
 */
@InterfaceAudience.Private
public class LaunchMapper extends Mapper<NullWritable, NullWritable, Text, Text> implements
        JobSubmissionConstants {
  /**
   * This class currently sends everything to stderr, but it should probably use Log4J - 
   * it will end up in 'syslog' of this Map task.  For example, look for KeepAlive heartbeat msgs.
   */
  private static final Logger LOG = LoggerFactory.getLogger(LaunchMapper.class);
  /**
   * When a Pig job is submitted and it uses HCat, WebHCat may be configured to ship hive tar
   * to the target node.  Pig on the target node needs some env vars configured.
   */
  private static void handlePigEnvVars(Configuration conf, Map<String, String> env) {
    if(conf.get(PigConstants.HIVE_HOME) != null) {
      env.put(PigConstants.HIVE_HOME, new File(conf.get(PigConstants.HIVE_HOME)).getAbsolutePath());
    }
    if(conf.get(PigConstants.HCAT_HOME) != null) {
      env.put(PigConstants.HCAT_HOME, new File(conf.get(PigConstants.HCAT_HOME)).getAbsolutePath());
    }
    if(conf.get(PigConstants.PIG_OPTS) != null) {
      StringBuilder pigOpts = new StringBuilder();
      for(String prop : StringUtils.split(conf.get(PigConstants.PIG_OPTS))) {
        pigOpts.append("-D").append(StringUtils.unEscapeString(prop)).append(" ");
      }
      env.put(PigConstants.PIG_OPTS, pigOpts.toString());
    }
  }

  /**
   * {@link #copyLocal(String, org.apache.hadoop.conf.Configuration)} should be called before this
   * See {@link org.apache.hive.hcatalog.templeton.SqoopDelegator#makeBasicArgs(String, String, String, String, boolean, String)}
   * for more comments
   */
  private static void handleSqoop(Configuration conf, Map<String, String> env) throws IOException {
    if(TempletonUtils.isset(conf.get(Sqoop.LIB_JARS))) {
      //LIB_JARS should only be set if Sqoop is auto-shipped
      LOG.debug(Sqoop.LIB_JARS + "=" + conf.get(Sqoop.LIB_JARS));
      String[] files = conf.getStrings(Sqoop.LIB_JARS);
      StringBuilder jdbcJars = new StringBuilder();
      for(String f : files) {
        jdbcJars.append(f).append(File.pathSeparator);
      }
      jdbcJars.setLength(jdbcJars.length() - 1);
      //this makes the jars available to Sqoop client
      prependPathToVariable(HADOOP_CLASSPATH, env, jdbcJars.toString());
    }
  }
  private static void handleHadoopClasspathExtras(Configuration conf, Map<String, String> env)
    throws IOException {
    if(!TempletonUtils.isset(conf.get(JobSubmissionConstants.HADOOP_CLASSPATH_EXTRAS))) {
      return;
    }
    LOG.debug(HADOOP_CLASSPATH_EXTRAS + "=" + conf.get(HADOOP_CLASSPATH_EXTRAS));
    String[] files = conf.getStrings(HADOOP_CLASSPATH_EXTRAS);
    StringBuilder paths = new StringBuilder();
    FileSystem fs = FileSystem.getLocal(conf);//these have been localized already
    for(String f : files) {
      Path p = new Path(f);
      FileStatus fileStatus = fs.getFileStatus(p);
      paths.append(f);
      if(fileStatus.isDir()) {
        paths.append(File.separator).append("*");
      }
      paths.append(File.pathSeparator);
    }
    paths.setLength(paths.length() - 1);
    prependPathToVariable(HADOOP_CLASSPATH, env, paths.toString());
  }
  /**
   * Ensures that {@code paths} are prepended to {@code pathVarName} and made available to forked child
   * process.
   * @param paths properly separated list of paths
   */
  private static void prependPathToVariable(String pathVarName, Map<String, String> env, String paths) {
    if(!TempletonUtils.isset(pathVarName) || !TempletonUtils.isset(paths) || env == null) {
      return;
    }
    if(TempletonUtils.isset(env.get(pathVarName))) {
      env.put(pathVarName, paths + File.pathSeparator + env.get(pathVarName));
    }
    else if(TempletonUtils.isset(System.getenv(pathVarName))) {
      env.put(pathVarName, paths + File.pathSeparator + System.getenv(pathVarName));
    }
    else {
      env.put(pathVarName, paths);
    }
  }
  protected Process startJob(Configuration conf, String jobId, String user, String overrideClasspath)
    throws IOException, InterruptedException {

    copyLocal(COPY_NAME, conf);
    String[] jarArgs = TempletonUtils.decodeArray(conf.get(JAR_ARGS_NAME));

    ArrayList<String> removeEnv = new ArrayList<String>();
    //todo: we really need some comments to explain exactly why each of these is removed
    removeEnv.add("HADOOP_ROOT_LOGGER");
    removeEnv.add("hadoop-command");
    removeEnv.add("CLASS");
    removeEnv.add("mapredcommand");
    Map<String, String> env = TempletonUtils.hadoopUserEnv(user, overrideClasspath);
    handlePigEnvVars(conf, env);
    handleSqoop(conf, env);
    handleHadoopClasspathExtras(conf, env);    
    List<String> jarArgsList = new LinkedList<String>(Arrays.asList(jarArgs));
    handleTokenFile(jarArgsList, JobSubmissionConstants.TOKEN_FILE_ARG_PLACEHOLDER, "mapreduce.job.credentials.binary");
    handleTokenFile(jarArgsList, JobSubmissionConstants.TOKEN_FILE_ARG_PLACEHOLDER_TEZ, "tez.credentials.path");
    handleMapReduceJobTag(jarArgsList, JobSubmissionConstants.MAPREDUCE_JOB_TAGS_ARG_PLACEHOLDER,
        JobSubmissionConstants.MAPREDUCE_JOB_TAGS, jobId);
    return TrivialExecService.getInstance().run(jarArgsList, removeEnv, env);
  }

  /**
   * Kills child jobs of this launcher that have been tagged with this job's ID.
   */
  private void killLauncherChildJobs(Configuration conf, String jobId) throws IOException {
    // Extract the launcher job submit/start time and use that to scope down
    // the search interval when we look for child jobs
    long startTime = getTempletonLaunchTime(conf);
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    WebHCatJTShim tracker = ShimLoader.getHadoopShims().getWebHCatShim(conf, ugi);
    try {
      tracker.killJobs(jobId, startTime);
    } finally {
      tracker.close();
    }
  }

  /**
   * Retrieves the templeton launcher job submit time from the configuration.
   * If not available throws.
   */
  private long getTempletonLaunchTime(Configuration conf) {
    long startTime = 0L;
    try {
      String launchTimeStr = conf.get(JobSubmissionConstants.TEMPLETON_JOB_LAUNCH_TIME_NAME);
      LOG.info("Launch time = " + launchTimeStr);
      if (launchTimeStr != null && launchTimeStr.length() > 0) {
        startTime = Long.parseLong(launchTimeStr);
      }
    } catch(NumberFormatException nfe) {
      throw new RuntimeException("Could not parse Templeton job launch time", nfe);
    }

    if (startTime == 0L) {
      throw new RuntimeException(String.format("Launch time property '%s' not found",
          JobSubmissionConstants.TEMPLETON_JOB_LAUNCH_TIME_NAME));
    }

    return startTime;
  }

  /**
   * Replace placeholder with actual "prop=file".  This is done multiple times (possibly) since
   * Tez and MR use different property names
   */
  private static void handleTokenFile(List<String> jarArgsList, String tokenPlaceHolder, String tokenProperty) throws IOException {
    String tokenFile = System.getenv("HADOOP_TOKEN_FILE_LOCATION");
    if (tokenFile != null) {
      //Token is available, so replace the placeholder
      tokenFile = tokenFile.replaceAll("\"", "");
      String tokenArg = tokenProperty + "=" + tokenFile;
      for(int i=0; i<jarArgsList.size(); i++){
        String newArg =
          jarArgsList.get(i).replace(tokenPlaceHolder, tokenArg);
        jarArgsList.set(i, newArg);
      }
    }else{
      //No token, so remove the placeholder arg
      Iterator<String> it = jarArgsList.iterator();
      while(it.hasNext()){
        String arg = it.next();
        if(arg.contains(tokenPlaceHolder)){
          it.remove();
        }
      }
    }
  }

  /**
   * Replace the placeholder mapreduce tags with our MR jobid so that all child jobs
   * get tagged with it. This is used on launcher task restart to prevent from having
   * same jobs running in parallel.
   */
  private static void handleMapReduceJobTag(List<String> jarArgsList, String placeholder,
      String mapReduceJobTagsProp, String currentJobId) throws IOException {
    String arg = String.format("%s=%s", mapReduceJobTagsProp, currentJobId);
    for(int i = 0; i < jarArgsList.size(); i++) {
      if (jarArgsList.get(i).contains(placeholder)) {
        String newArg = jarArgsList.get(i).replace(placeholder, arg);
        jarArgsList.set(i, newArg);
        return;
      }
    }

    // Unexpected error, placeholder tag is not found, throw
    throw new RuntimeException(
        String.format("Unexpected Error: Tag '%s' not found in the list of launcher args", placeholder));
  }

  private void copyLocal(String var, Configuration conf) throws IOException {
    String[] filenames = TempletonUtils.decodeArray(conf.get(var));
    if (filenames != null) {
      for (String filename : filenames) {
        Path src = new Path(filename);
        Path dst = new Path(src.getName());
        FileSystem fs = src.getFileSystem(conf);
        LOG.info("templeton: copy " + src + " => " + dst);
        fs.copyToLocalFile(src, dst);
      }
    }
  }

  /**
   * Checks if reconnection to an already running job is enabled and supported for a given
   * job type.
   */
  private boolean reconnectToRunningJobEnabledAndSupported(Configuration conf,
      LauncherDelegator.JobType jobType) {
    if (conf.get(ENABLE_JOB_RECONNECT) == null) {
      return false;
    }

    Boolean enableJobReconnect = Boolean.parseBoolean(conf.get(ENABLE_JOB_RECONNECT));
    if (!enableJobReconnect) {
      return false;
    }

    // Reconnect is only supported for MR and Streaming jobs at this time
    return jobType.equals(LauncherDelegator.JobType.JAR)
        || jobType.equals(LauncherDelegator.JobType.STREAMING);
  }

  /**
   * Attempts to reconnect to an already running child job of the templeton launcher. This
   * is used in cases where the templeton launcher task has failed and is retried by the
   * MR framework. If reconnect to the child job is possible, the method will continue
   * tracking its progress until completion.
   * @return Returns true if reconnect was successful, false if not supported or
   *         no child jobs were found.
   */
  private boolean tryReconnectToRunningJob(Configuration conf, Context context,
      LauncherDelegator.JobType jobType, String statusdir) throws IOException, InterruptedException {
    if (!reconnectToRunningJobEnabledAndSupported(conf, jobType)) {
      return false;
    }

    long startTime = getTempletonLaunchTime(conf);
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    WebHCatJTShim tracker = ShimLoader.getHadoopShims().getWebHCatShim(conf, ugi);
    try {
      Set<String> childJobs = tracker.getJobs(context.getJobID().toString(), startTime);
      if (childJobs.size() == 0) {
        LOG.info("No child jobs found to reconnect with");
        return false;
      }

      if (childJobs.size() > 1) {
        LOG.warn(String.format(
            "Found more than one child job to reconnect with: %s, skipping reconnect",
            Arrays.toString(childJobs.toArray())));
        return false;
      }

      String childJobIdString = childJobs.iterator().next();
      org.apache.hadoop.mapred.JobID childJobId =
          org.apache.hadoop.mapred.JobID.forName(childJobIdString);
      LOG.info(String.format("Reconnecting to an existing job %s", childJobIdString));

      // Update job state with the childJob id
      updateJobStatePercentAndChildId(conf, context.getJobID().toString(), null, childJobIdString);

      do {
        org.apache.hadoop.mapred.JobStatus jobStatus = tracker.getJobStatus(childJobId);
        if (jobStatus.isJobComplete()) {
          LOG.info(String.format("Child job %s completed", childJobIdString));
          int exitCode = 0;
          if (jobStatus.getRunState() != org.apache.hadoop.mapred.JobStatus.SUCCEEDED) {
            exitCode = 1;
          }
          updateJobStateToDoneAndWriteExitValue(conf, statusdir, context.getJobID().toString(),
              exitCode);
          break;
        }

        String percent = String.format("map %s%%, reduce %s%%",
            jobStatus.mapProgress()*100, jobStatus.reduceProgress()*100);
        updateJobStatePercentAndChildId(conf, context.getJobID().toString(), percent, null);

        LOG.info("KeepAlive Heart beat");

        context.progress();
        Thread.sleep(POLL_JOBPROGRESS_MSEC);
      } while (true);

      // Reconnect was successful
      return true;
    }
    catch (IOException ex) {
      LOG.error("Exception encountered in tryReconnectToRunningJob", ex);
      throw ex;
    } finally {
      tracker.close();
    }
  }

  @Override
  public void run(Context context) throws IOException, InterruptedException {

    Configuration conf = context.getConfiguration();
    LauncherDelegator.JobType jobType = LauncherDelegator.JobType.valueOf(conf.get(JOB_TYPE));
    String statusdir = conf.get(STATUSDIR_NAME);
    if (statusdir != null) {
      try {
        statusdir = TempletonUtils.addUserHomeDirectoryIfApplicable(statusdir,
                conf.get("user.name"));
      } catch (URISyntaxException e) {
        String msg = "Invalid status dir URI";
        LOG.error(msg, e);
        throw new IOException(msg, e);
      }
    }

    // Try to reconnect to a child job if one is found
    if (tryReconnectToRunningJob(conf, context, jobType, statusdir)) {
      return;
    }

    // Kill previously launched child MR jobs started by this launcher to prevent having
    // same jobs running side-by-side
    killLauncherChildJobs(conf, context.getJobID().toString());

    // Start the job
    Process proc = startJob(conf,
            context.getJobID().toString(),
            conf.get("user.name"),
            conf.get(OVERRIDE_CLASSPATH));

    ExecutorService pool = Executors.newCachedThreadPool();
    executeWatcher(pool, conf, context.getJobID(),
            proc.getInputStream(), statusdir, STDOUT_FNAME);
    executeWatcher(pool, conf, context.getJobID(),
            proc.getErrorStream(), statusdir, STDERR_FNAME);
    KeepAlive keepAlive = startCounterKeepAlive(pool, context);

    proc.waitFor();
    keepAlive.sendReport = false;
    pool.shutdown();
    if (!pool.awaitTermination(WATCHER_TIMEOUT_SECS, TimeUnit.SECONDS)) {
      pool.shutdownNow();
    }

    updateJobStateToDoneAndWriteExitValue(conf, statusdir, context.getJobID().toString(),
        proc.exitValue());

    Boolean enablelog = Boolean.parseBoolean(conf.get(ENABLE_LOG));
    if (enablelog && TempletonUtils.isset(statusdir)) {
      LOG.info("templeton: collecting logs for " + context.getJobID().toString()
               + " to " + statusdir + "/logs");
      LogRetriever logRetriever = new LogRetriever(statusdir, jobType, conf);
      logRetriever.run();
    }
  }

  private void updateJobStateToDoneAndWriteExitValue(Configuration conf,
      String statusdir, String jobId, int exitCode) throws IOException {
    writeExitValue(conf, exitCode, statusdir);
    JobState state = new JobState(jobId, conf);
    state.setExitValue(exitCode);
    state.setCompleteStatus("done");
    state.close();

    if (exitCode != 0) {
      LOG.info("templeton: job failed with exit code "
              + exitCode);
    } else {
      LOG.info("templeton: job completed with exit code 0");
    }
  }

  /**
   * Updates the job state percent and childid in the templeton storage. Update only
   * takes place for non-null values.
   */
  private static void updateJobStatePercentAndChildId(Configuration conf,
      String jobId, String percent, String childid) {
    JobState state = null;
    try {
      if (percent != null || childid != null) {
        state = new JobState(jobId, conf);
        if (percent != null) {
          state.setPercentComplete(percent);
        }
        if (childid != null) {
          JobState childState = new JobState(childid, conf);
          childState.setParent(jobId);
          state.addChild(childid);
          state.close();
        }
      }
    } catch (IOException e) {
      LOG.error("templeton: state error: ", e);
    } finally {
      if (state != null) {
        try {
          state.close();
        } catch (IOException e) {
          LOG.warn("Caught exception while closing job state ", e);
        }
      }
    }
  }

  private void executeWatcher(ExecutorService pool, Configuration conf, JobID jobid, InputStream in,
                              String statusdir, String name) throws IOException {
    Watcher w = new Watcher(conf, jobid, in, statusdir, name);
    pool.execute(w);
  }

  private KeepAlive startCounterKeepAlive(ExecutorService pool, Context context) throws IOException {
    KeepAlive k = new KeepAlive(context);
    pool.execute(k);
    return k;
  }

  private void writeExitValue(Configuration conf, int exitValue, String statusdir) 
    throws IOException {
    if (TempletonUtils.isset(statusdir)) {
      Path p = new Path(statusdir, EXIT_FNAME);
      FileSystem fs = p.getFileSystem(conf);
      OutputStream out = fs.create(p);
      LOG.info("templeton: Writing exit value " + exitValue + " to " + p);
      PrintWriter writer = new PrintWriter(out);
      writer.println(exitValue);
      writer.close();
      LOG.info("templeton: Exit value successfully written");
    }
  }


  private static class Watcher implements Runnable {
    private final InputStream in;
    private OutputStream out;
    private final JobID jobid;
    private final Configuration conf;
    boolean needCloseOutput = false;

    public Watcher(Configuration conf, JobID jobid, InputStream in, String statusdir, String name)
      throws IOException {
      this.conf = conf;
      this.jobid = jobid;
      this.in = in;

      if (name.equals(STDERR_FNAME)) {
        out = System.err;
      } else {
        out = System.out;
      }

      if (TempletonUtils.isset(statusdir)) {
        Path p = new Path(statusdir, name);
        FileSystem fs = p.getFileSystem(conf);
        out = fs.create(p);
        needCloseOutput = true;
        LOG.info("templeton: Writing status to " + p);
      }
    }

    @Override
    public void run() {
      PrintWriter writer = null;
      try {
        String enc = conf.get(AppConfig.EXEC_ENCODING_NAME);
        InputStreamReader isr = new InputStreamReader(in, enc);
        BufferedReader reader = new BufferedReader(isr);
        writer = new PrintWriter(new OutputStreamWriter(out, enc));

        String line;
        while ((line = reader.readLine()) != null) {
          writer.println(line);
          String percent = TempletonUtils.extractPercentComplete(line);
          String childid = TempletonUtils.extractChildJobId(line);
          updateJobStatePercentAndChildId(conf, jobid.toString(), percent, childid);
        }
        writer.flush();
        if(out != System.err && out != System.out) {
          //depending on FileSystem implementation flush() may or may not do anything 
          writer.close();
        }
      } catch (IOException e) {
        LOG.error("templeton: execute error: ", e);
      } finally {
        // Need to close() because in some FileSystem
        // implementations flush() is no-op.
        // Close the file handle if it is a hdfs file.
        // But if it is stderr/stdout, skip it since
        // WebHCat is not supposed to close it
        if (needCloseOutput && writer!=null) {
          writer.close();
        }
      }
    }
  }

  private static class KeepAlive implements Runnable {
    private final Context context;
    private volatile boolean sendReport;

    public KeepAlive(Context context)
    {
      this.sendReport = true;
      this.context = context;
    }
    private static StringBuilder makeDots(int count) {
      StringBuilder sb = new StringBuilder();
      for(int i = 0; i < count; i++) {
        sb.append('.');
      }
      return sb;
    }

    @Override
    public void run() {
      try {
        int count = 0;
        while (sendReport) {
          // Periodically report progress on the Context object
          // to prevent TaskTracker from killing the Templeton
          // Controller task
          context.progress();
          count++;
          String msg = "KeepAlive Heart beat" + makeDots(count);
          LOG.info(msg);
          Thread.sleep(KEEP_ALIVE_MSEC);
        }
      } catch (InterruptedException e) {
        // Ok to be interrupted
      }
    }
  }
}
