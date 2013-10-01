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

import java.io.BufferedReader;
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.templeton.BadParam;
import org.apache.hive.hcatalog.templeton.LauncherDelegator;

/**
 * A Map Reduce job that will start another job.
 *
 * We have a single Mapper job that starts a child MR job.  The parent
 * monitors the child child job and ends when the child job exits.  In
 * addition, we
 *
 * - write out the parent job id so the caller can record it.
 * - run a keep alive thread so the job doesn't end.
 * - Optionally, store the stdout, stderr, and exit value of the child
 *   in hdfs files.
 */
public class TempletonControllerJob extends Configured implements Tool {
  public static final String COPY_NAME = "templeton.copy";
  public static final String STATUSDIR_NAME = "templeton.statusdir";
  public static final String ENABLE_LOG = "templeton.enablelog";
  public static final String JOB_TYPE = "templeton.jobtype";
  public static final String JAR_ARGS_NAME = "templeton.args";
  public static final String OVERRIDE_CLASSPATH = "templeton.override-classpath";

  public static final String STDOUT_FNAME = "stdout";
  public static final String STDERR_FNAME = "stderr";
  public static final String EXIT_FNAME = "exit";

  public static final int WATCHER_TIMEOUT_SECS = 10;
  public static final int KEEP_ALIVE_MSEC = 60 * 1000;

  public static final String TOKEN_FILE_ARG_PLACEHOLDER 
    = "__WEBHCAT_TOKEN_FILE_LOCATION__";


  private static TrivialExecService execService = TrivialExecService.getInstance();

  private static final Log LOG = LogFactory.getLog(TempletonControllerJob.class);


  public static class LaunchMapper
    extends Mapper<NullWritable, NullWritable, Text, Text> {
    protected Process startJob(Context context, String user,
                   String overrideClasspath)
      throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      copyLocal(COPY_NAME, conf);
      String[] jarArgs
        = TempletonUtils.decodeArray(conf.get(JAR_ARGS_NAME));

      ArrayList<String> removeEnv = new ArrayList<String>();
      removeEnv.add("HADOOP_ROOT_LOGGER");
      removeEnv.add("hadoop-command");
      removeEnv.add("CLASS");
      removeEnv.add("mapredcommand");
      Map<String, String> env = TempletonUtils.hadoopUserEnv(user,
        overrideClasspath);
      List<String> jarArgsList = new LinkedList<String>(Arrays.asList(jarArgs));
      String tokenFile = System.getenv("HADOOP_TOKEN_FILE_LOCATION");


      if (tokenFile != null) {
        //Token is available, so replace the placeholder
        tokenFile = tokenFile.replaceAll("\"", "");
        String tokenArg = "mapreduce.job.credentials.binary=" + tokenFile;
        if (Shell.WINDOWS) {
          try {
            tokenArg = TempletonUtils.quoteForWindows(tokenArg);
          } catch (BadParam e) {
            throw new IOException("cannot pass " + tokenFile + " to mapreduce.job.credentials.binary", e);
          }
        }
        for(int i=0; i<jarArgsList.size(); i++){
          String newArg = 
            jarArgsList.get(i).replace(TOKEN_FILE_ARG_PLACEHOLDER, tokenArg);
          jarArgsList.set(i, newArg);
        }

      }else{
        //No token, so remove the placeholder arg
        Iterator<String> it = jarArgsList.iterator();
        while(it.hasNext()){
          String arg = it.next();
          if(arg.contains(TOKEN_FILE_ARG_PLACEHOLDER)){
            it.remove();
          }
        }
      }
      return execService.run(jarArgsList, removeEnv, env);
    }

    private void copyLocal(String var, Configuration conf)
      throws IOException {
      String[] filenames = TempletonUtils.decodeArray(conf.get(var));
      if (filenames != null) {
        for (String filename : filenames) {
          Path src = new Path(filename);
          Path dst = new Path(src.getName());
          FileSystem fs = src.getFileSystem(conf);
          System.err.println("templeton: copy " + src + " => " + dst);
          fs.copyToLocalFile(src, dst);
        }
      }
    }

    @Override
    public void run(Context context)
      throws IOException, InterruptedException {

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
          throw new IOException("Invalid status dir URI", e);
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
      if (!pool.awaitTermination(WATCHER_TIMEOUT_SECS, TimeUnit.SECONDS))
        pool.shutdownNow();

      writeExitValue(conf, proc.exitValue(), statusdir);
      JobState state = new JobState(context.getJobID().toString(), conf);
      state.setExitValue(proc.exitValue());
      state.setCompleteStatus("done");
      state.close();

      if (enablelog && TempletonUtils.isset(statusdir)) {
        System.err.println("templeton: collecting logs for " + context.getJobID().toString()
          + " to " + statusdir + "/logs");
        LogRetriever logRetriever = new LogRetriever(statusdir, jobType, conf);
        logRetriever.run();
      }

      if (proc.exitValue() != 0)
        System.err.println("templeton: job failed with exit code "
          + proc.exitValue());
      else
        System.err.println("templeton: job completed with exit code 0");
    }

    private void executeWatcher(ExecutorService pool, Configuration conf,
                  JobID jobid, InputStream in, String statusdir,
                  String name)
      throws IOException {
      Watcher w = new Watcher(conf, jobid, in, statusdir, name);
      pool.execute(w);
    }

    private KeepAlive startCounterKeepAlive(ExecutorService pool, Context context)
      throws IOException {
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
        System.err.println("templeton: Writing exit value "
          + exitValue + " to " + p);
        PrintWriter writer = new PrintWriter(out);
        writer.println(exitValue);
        writer.close();
      }
    }
  }

  private static class Watcher implements Runnable {
    private InputStream in;
    private OutputStream out;
    private JobID jobid;
    private Configuration conf;

    public Watcher(Configuration conf, JobID jobid, InputStream in,
             String statusdir, String name)
      throws IOException {
      this.conf = conf;
      this.jobid = jobid;
      this.in = in;

      if (name.equals(STDERR_FNAME))
        out = System.err;
      else
        out = System.out;

      if (TempletonUtils.isset(statusdir)) {
        Path p = new Path(statusdir, name);
        FileSystem fs = p.getFileSystem(conf);
        out = fs.create(p);
        System.err.println("templeton: Writing status to " + p);
      }
    }

    @Override
    public void run() {
      try {
        InputStreamReader isr = new InputStreamReader(in);
        BufferedReader reader = new BufferedReader(isr);
        PrintWriter writer = new PrintWriter(out);

        String line;
        while ((line = reader.readLine()) != null) {
          writer.println(line);
          JobState state = null;
          try {
            String percent = TempletonUtils.extractPercentComplete(line);
            String childid = TempletonUtils.extractChildJobId(line);

            if (percent != null || childid != null) {
              state = new JobState(jobid.toString(), conf);
              state.setPercentComplete(percent);
              state.setChildId(childid);
            }
          } catch (IOException e) {
            System.err.println("templeton: state error: " + e);
          } finally {
            if (state != null) {
              try {
                state.close();
              } catch (IOException e) {
              }
            }
          }
        }
        writer.flush();
      } catch (IOException e) {
        System.err.println("templeton: execute error: " + e);
      }
    }
  }

  public static class KeepAlive implements Runnable {
    private Context context;
    public boolean sendReport;

    public KeepAlive(Context context)
    {
      this.sendReport = true;
      this.context = context;
    }

    @Override
    public void run() {
      try {
        while (sendReport) {
          // Periodically report progress on the Context object
          // to prevent TaskTracker from killing the Templeton
          // Controller task
          context.progress();
          System.err.println("KeepAlive Heart beat");
          Thread.sleep(KEEP_ALIVE_MSEC);
        }
      } catch (InterruptedException e) {
        // Ok to be interrupted
      }
    }
  }

  private JobID submittedJobId;

  public String getSubmittedId() {
    if (submittedJobId == null)
      return null;
    else
      return submittedJobId.toString();
  }

  /**
   * Enqueue the job and print out the job id for later collection.
   */
  @Override
  public int run(String[] args)
    throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = getConf();
    conf.set(JAR_ARGS_NAME, TempletonUtils.encodeArray(args));
    conf.set("user.name", UserGroupInformation.getCurrentUser().getShortUserName());
    Job job = new Job(conf);
    job.setJarByClass(TempletonControllerJob.class);
    job.setJobName("TempletonControllerJob");
    job.setMapperClass(LaunchMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setInputFormatClass(SingleInputFormat.class);
    NullOutputFormat<NullWritable, NullWritable> of
      = new NullOutputFormat<NullWritable, NullWritable>();
    job.setOutputFormatClass(of.getClass());
    job.setNumReduceTasks(0);

    JobClient jc = new JobClient(new JobConf(job.getConfiguration()));

    Token<DelegationTokenIdentifier> mrdt = jc.getDelegationToken(new Text("mr token"));
    job.getCredentials().addToken(new Text("mr token"), mrdt);
    job.submit();

    submittedJobId = job.getJobID();

    return 0;
  }


  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new TempletonControllerJob(), args);
    if (ret != 0)
      System.err.println("TempletonControllerJob failed!");
    System.exit(ret);
  }
}
