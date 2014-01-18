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

package org.apache.hadoop.hive.ql.io.rcfile.merge;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.HadoopJobExecHelper;
import org.apache.hadoop.hive.ql.exec.mr.HadoopJobExecHook;
import org.apache.hadoop.hive.ql.exec.mr.Throttle;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveOutputFormatImpl;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.LogManager;

@SuppressWarnings( { "deprecation", "unchecked" })
public class BlockMergeTask extends Task<MergeWork> implements Serializable,
    HadoopJobExecHook {

  private static final long serialVersionUID = 1L;

  protected transient JobConf job;
  protected HadoopJobExecHelper jobExecHelper;

  @Override
  public void initialize(HiveConf conf, QueryPlan queryPlan,
      DriverContext driverContext) {
    super.initialize(conf, queryPlan, driverContext);
    job = new JobConf(conf, BlockMergeTask.class);
    jobExecHelper = new HadoopJobExecHelper(job, this.console, this, this);
  }

  @Override
  public boolean requireLock() {
    return true;
  }

  boolean success = true;

  @Override
  /**
   * start a new map-reduce job to do the merge, almost the same as ExecDriver.
   */
  public int execute(DriverContext driverContext) {
    HiveConf.setVar(job, HiveConf.ConfVars.HIVEINPUTFORMAT,
        CombineHiveInputFormat.class.getName());
    success = true;
    ShimLoader.getHadoopShims().prepareJobOutput(job);
    job.setOutputFormat(HiveOutputFormatImpl.class);
    job.setMapperClass(work.getMapperClass());

    Context ctx = driverContext.getCtx();
    boolean ctxCreated = false;
    try {
      if (ctx == null) {
        ctx = new Context(job);
        ctxCreated = true;
      }
    }catch (IOException e) {
      e.printStackTrace();
      console.printError("Error launching map-reduce job", "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return 5;
    }

    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(NullWritable.class);
    if(work.getNumMapTasks() != null) {
      job.setNumMapTasks(work.getNumMapTasks());
    }

    // zero reducers
    job.setNumReduceTasks(0);

    if (work.getMinSplitSize() != null) {
      HiveConf.setLongVar(job, HiveConf.ConfVars.MAPREDMINSPLITSIZE, work
          .getMinSplitSize().longValue());
    }

    if (work.getInputformat() != null) {
      HiveConf.setVar(job, HiveConf.ConfVars.HIVEINPUTFORMAT, work
          .getInputformat());
    }

    String inpFormat = HiveConf.getVar(job, HiveConf.ConfVars.HIVEINPUTFORMAT);
    if ((inpFormat == null) || (!StringUtils.isNotBlank(inpFormat))) {
      inpFormat = ShimLoader.getHadoopShims().getInputFormatClassName();
    }

    LOG.info("Using " + inpFormat);

    try {
      job.setInputFormat((Class<? extends InputFormat>) (Class
          .forName(inpFormat)));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e.getMessage());
    }

    Path outputPath = this.work.getOutputDir();
    Path tempOutPath = Utilities.toTempPath(outputPath);
    try {
      FileSystem fs = tempOutPath.getFileSystem(job);
      if (!fs.exists(tempOutPath)) {
        fs.mkdirs(tempOutPath);
      }
    } catch (IOException e) {
      console.printError("Can't make path " + outputPath + " : " + e.getMessage());
      return 6;
    }

    RCFileBlockMergeOutputFormat.setMergeOutputPath(job, outputPath);

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);

    HiveConf.setBoolVar(job,
        HiveConf.ConfVars.HIVEMERGECURRENTJOBHASDYNAMICPARTITIONS,
        work.hasDynamicPartitions());

    HiveConf.setBoolVar(job,
        HiveConf.ConfVars.HIVEMERGECURRENTJOBCONCATENATELISTBUCKETING,
        work.isListBucketingAlterTableConcatenate());

    HiveConf.setIntVar(
        job,
        HiveConf.ConfVars.HIVEMERGECURRENTJOBCONCATENATELISTBUCKETINGDEPTH,
        ((work.getListBucketingCtx() == null) ? 0 : work.getListBucketingCtx()
            .calculateListBucketingLevel()));

    int returnVal = 0;
    RunningJob rj = null;
    boolean noName = StringUtils.isEmpty(HiveConf.getVar(job,
        HiveConf.ConfVars.HADOOPJOBNAME));

    String jobName = null;
    if (noName && this.getQueryPlan() != null) {
      int maxlen = conf.getIntVar(HiveConf.ConfVars.HIVEJOBNAMELENGTH);
      jobName = Utilities.abbreviate(this.getQueryPlan().getQueryStr(),
          maxlen - 6);
    }

    if (noName) {
      // This is for a special case to ensure unit tests pass
      HiveConf.setVar(job, HiveConf.ConfVars.HADOOPJOBNAME,
          jobName != null ? jobName : "JOB" + Utilities.randGen.nextInt());
    }

    try {
      addInputPaths(job, work);

      Utilities.setMapWork(job, work, ctx.getMRTmpPath(), true);

      // remove the pwd from conf file so that job tracker doesn't show this
      // logs
      String pwd = HiveConf.getVar(job, HiveConf.ConfVars.METASTOREPWD);
      if (pwd != null) {
        HiveConf.setVar(job, HiveConf.ConfVars.METASTOREPWD, "HIVE");
      }
      JobClient jc = new JobClient(job);

      String addedJars = Utilities.getResourceFiles(job, SessionState.ResourceType.JAR);
      if (!addedJars.isEmpty()) {
        job.set("tmpjars", addedJars);
      }

      // make this client wait if job trcker is not behaving well.
      Throttle.checkJobTracker(job, LOG);

      // Finally SUBMIT the JOB!
      rj = jc.submitJob(job);

      returnVal = jobExecHelper.progress(rj, jc);
      success = (returnVal == 0);

    } catch (Exception e) {
      e.printStackTrace();
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
      try {
        if (ctxCreated) {
          ctx.clear();
        }
        if (rj != null) {
          if (returnVal != 0) {
            rj.killJob();
          }
          HadoopJobExecHelper.runningJobKillURIs.remove(rj.getJobID());
          jobID = rj.getID().toString();
        }
        RCFileMergeMapper.jobClose(outputPath, success, job, console,
          work.getDynPartCtx(), null);
      } catch (Exception e) {
      }
    }

    return (returnVal);
  }

  private void addInputPaths(JobConf job, MergeWork work) {
    for (Path path : work.getInputPaths()) {
      FileInputFormat.addInputPath(job, path);
    }
  }

  @Override
  public String getName() {
    return "RCFile Merge";
  }

  public static String INPUT_SEPERATOR = ":";

  public static void main(String[] args) {
    String inputPathStr = null;
    String outputDir = null;
    String jobConfFileName = null;

    try {
      for (int i = 0; i < args.length; i++) {
        if (args[i].equals("-input")) {
          inputPathStr = args[++i];
        } else if (args[i].equals("-jobconffile")) {
          jobConfFileName = args[++i];
        } else if (args[i].equals("-outputDir")) {
          outputDir = args[++i];
        }
      }
    } catch (IndexOutOfBoundsException e) {
      System.err.println("Missing argument to option");
      printUsage();
    }

    if (inputPathStr == null || outputDir == null
        || outputDir.trim().equals("")) {
      printUsage();
    }

    List<Path> inputPaths = new ArrayList<Path>();
    String[] paths = inputPathStr.split(INPUT_SEPERATOR);
    if (paths == null || paths.length == 0) {
      printUsage();
    }

    FileSystem fs = null;
    JobConf conf = new JobConf(BlockMergeTask.class);
    for (String path : paths) {
      try {
        Path pathObj = new Path(path);
        if (fs == null) {
          fs = FileSystem.get(pathObj.toUri(), conf);
        }
        FileStatus fstatus = fs.getFileStatus(pathObj);
        if (fstatus.isDir()) {
          FileStatus[] fileStatus = fs.listStatus(pathObj);
          for (FileStatus st : fileStatus) {
            inputPaths.add(st.getPath());
          }
        } else {
          inputPaths.add(fstatus.getPath());
        }
      } catch (IOException e) {
        e.printStackTrace(System.err);
      }
    }

    if (jobConfFileName != null) {
      conf.addResource(new Path(jobConfFileName));
    }
    HiveConf hiveConf = new HiveConf(conf, BlockMergeTask.class);

    Log LOG = LogFactory.getLog(BlockMergeTask.class.getName());
    boolean isSilent = HiveConf.getBoolVar(conf,
        HiveConf.ConfVars.HIVESESSIONSILENT);
    LogHelper console = new LogHelper(LOG, isSilent);

    // print out the location of the log file for the user so
    // that it's easy to find reason for local mode execution failures
    for (Appender appender : Collections
        .list((Enumeration<Appender>) LogManager.getRootLogger()
            .getAllAppenders())) {
      if (appender instanceof FileAppender) {
        console.printInfo("Execution log at: "
            + ((FileAppender) appender).getFile());
      }
    }

    MergeWork mergeWork = new MergeWork(inputPaths, new Path(outputDir));
    DriverContext driverCxt = new DriverContext();
    BlockMergeTask taskExec = new BlockMergeTask();
    taskExec.initialize(hiveConf, null, driverCxt);
    taskExec.setWork(mergeWork);
    int ret = taskExec.execute(driverCxt);

    if (ret != 0) {
      System.exit(2);
    }

  }

  private static void printUsage() {
    System.err.println("BlockMergeTask -input <colon seperated input paths>  "
        + "-outputDir outputDir [-jobconffile <job conf file>] ");
    System.exit(1);
  }

  @Override
  public StageType getType() {
    return StageType.MAPRED;
  }

  @Override
  public boolean checkFatalErrors(Counters ctrs, StringBuilder errMsg) {
    return false;
  }

  @Override
  public void logPlanProgress(SessionState ss) throws IOException {
    // no op
  }  
}
