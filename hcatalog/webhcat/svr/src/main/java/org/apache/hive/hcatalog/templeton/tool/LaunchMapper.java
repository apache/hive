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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.templeton.tool;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Shell;
import org.apache.hive.hcatalog.templeton.BadParam;
import org.apache.hive.hcatalog.templeton.LauncherDelegator;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
  private static final Log LOG = LogFactory.getLog(LaunchMapper.class);
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
      for(String prop : conf.get(PigConstants.PIG_OPTS).split(",")) {
        pigOpts.append("-D").append(prop).append(" ");
      }
      env.put(PigConstants.PIG_OPTS, pigOpts.toString());
    }
  }
  protected Process startJob(Context context, String user, String overrideClasspath)
    throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
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
    List<String> jarArgsList = new LinkedList<String>(Arrays.asList(jarArgs));
    handleTokenFile(jarArgsList, JobSubmissionConstants.TOKEN_FILE_ARG_PLACEHOLDER, "mapreduce.job.credentials.binary");
    handleTokenFile(jarArgsList, JobSubmissionConstants.TOKEN_FILE_ARG_PLACEHOLDER_TEZ, "tez.credentials.path");
    boolean overrideLog4jProps = conf.get(OVERRIDE_CONTAINER_LOG4J_PROPS) == null ?
            false : Boolean.valueOf(conf.get(OVERRIDE_CONTAINER_LOG4J_PROPS));
    return TrivialExecService.getInstance().run(jarArgsList, removeEnv, env, overrideLog4jProps);
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
      if (Shell.WINDOWS) {
        try {
          tokenArg = TempletonUtils.quoteForWindows(tokenArg);
        } catch (BadParam e) {
          String msg = "cannot pass " + tokenFile + " to " + tokenProperty;
          LOG.error(msg, e);
          throw new IOException(msg, e);
        }
      }
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

  @Override
  public void run(Context context) throws IOException, InterruptedException {

    Configuration conf = context.getConfiguration();

    Process proc = startJob(context,
            conf.get("user.name"),
            conf.get(OVERRIDE_CLASSPATH));

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

    Boolean enablelog = Boolean.parseBoolean(conf.get(ENABLE_LOG));
    LauncherDelegator.JobType jobType = LauncherDelegator.JobType.valueOf(conf.get(JOB_TYPE));

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

    writeExitValue(conf, proc.exitValue(), statusdir);
    JobState state = new JobState(context.getJobID().toString(), conf);
    state.setExitValue(proc.exitValue());
    state.setCompleteStatus("done");
    state.close();

    if (enablelog && TempletonUtils.isset(statusdir)) {
      LOG.info("templeton: collecting logs for " + context.getJobID().toString()
              + " to " + statusdir + "/logs");
      LogRetriever logRetriever = new LogRetriever(statusdir, jobType, conf);
      logRetriever.run();
    }

    if (proc.exitValue() != 0) {
      LOG.info("templeton: job failed with exit code "
              + proc.exitValue());
    } else {
      LOG.info("templeton: job completed with exit code 0");
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
        InputStreamReader isr = new InputStreamReader(in);
        BufferedReader reader = new BufferedReader(isr);
        writer = new PrintWriter(out);

        String line;
        while ((line = reader.readLine()) != null) {
          writer.println(line);
          JobState state = null;
          try {
            String percent = TempletonUtils.extractPercentComplete(line);
            String childid = TempletonUtils.extractChildJobId(line);

            if (percent != null || childid != null) {
              state = new JobState(jobid.toString(), conf);
              if (percent != null) {
                state.setPercentComplete(percent);
              }
              if (childid != null) {
                JobState childState = new JobState(childid, conf);
                childState.setParent(jobid.toString());
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
                LOG.warn(e);
              }
            }
          }
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
