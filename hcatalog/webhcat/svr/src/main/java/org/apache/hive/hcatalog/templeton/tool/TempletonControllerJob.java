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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Tool;
import org.apache.hive.hcatalog.templeton.AppConfig;
import org.apache.hive.hcatalog.templeton.Main;
import org.apache.hive.hcatalog.templeton.SecureProxySupport;
import org.apache.hive.hcatalog.templeton.UgiFactory;
import org.apache.thrift.TException;

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
 *   
 * A note on security.  When jobs are submitted through WebHCat that use HCatalog, it means that
 * metastore access is required.  Hive queries, of course, need metastore access.  This in turn
 * requires delegation token to be obtained for metastore in a <em>secure cluster</em>.  Since we
 * can't usually parse the job to find out if it is using metastore, we require 'usehcatalog'
 * parameter supplied in the REST call.  WebHcat takes care of cancelling the token when the job
 * is complete.
 */
@InterfaceAudience.Private
public class TempletonControllerJob extends Configured implements Tool, JobSubmissionConstants {
  private static final Log LOG = LogFactory.getLog(TempletonControllerJob.class);
  //file to add to DistributedCache
  private static URI overrideLog4jURI = null;
  private static boolean overrideContainerLog4jProps;
  //Jar cmd submission likely will be affected, Pig likely not
  private static final String affectedMsg = "Monitoring of Hadoop jobs submitted through WebHCat " +
          "may be affected.";
  private static final String TMP_DIR_PROP = "hadoop.tmp.dir";

  /**
   * Copy the file from local file system to tmp dir
   */
  private static URI copyLog4JtoFileSystem(final String localFile) throws IOException,
          InterruptedException {
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    return ugi.doAs(new PrivilegedExceptionAction<URI>() {
      @Override
      public URI run() throws IOException {
        AppConfig appConfig = Main.getAppConfigInstance();
        String fsTmpDir = appConfig.get(TMP_DIR_PROP);
        if(fsTmpDir == null || fsTmpDir.length() <= 0) {
          LOG.warn("Could not find 'hadoop.tmp.dir'; " + affectedMsg);
          return null;
        }
        FileSystem fs = FileSystem.get(appConfig);
        Path dirPath = new Path(fsTmpDir);
        if(!fs.exists(dirPath)) {
          LOG.warn(dirPath + " does not exist; " + affectedMsg);
          return null;
        }
        Path dst = fs.makeQualified(new Path(fsTmpDir, CONTAINER_LOG4J_PROPS));
        fs.copyFromLocalFile(new Path(localFile), dst);
        //make readable by all users since TempletonControllerJob#run() is run as submitting user
        fs.setPermission(dst, new FsPermission((short)0644));
        return dst.toUri();
      }
    });
  }
  /**
   * local file system
   * @return
   */
  private static String getLog4JPropsLocal() {
    return AppConfig.getWebhcatConfDir() + File.separator + CONTAINER_LOG4J_PROPS;
  }
  static {
    //initialize once-per-JVM (i.e. one running WebHCat server) state and log it once since it's 
    // the same for every job
    try {
      //safe (thread) publication 
      // http://docs.oracle.com/javase/specs/jls/se5.0/html/execution.html#12.4.2
      LOG.info("Using Hadoop Version: " + ShimLoader.getMajorVersion());
      overrideContainerLog4jProps = "0.23".equals(ShimLoader.getMajorVersion());
      if(overrideContainerLog4jProps) {
        //see detailed note in CONTAINER_LOG4J_PROPS file
        LOG.info(AppConfig.WEBHCAT_CONF_DIR + "=" + AppConfig.getWebhcatConfDir());
        File localFile = new File(getLog4JPropsLocal());
        if(localFile.exists()) {
          LOG.info("Found " + localFile.getAbsolutePath() + " to use for job submission.");
          try {
            overrideLog4jURI = copyLog4JtoFileSystem(getLog4JPropsLocal());
            LOG.info("Job submission will use log4j.properties=" + overrideLog4jURI);
          }
          catch(IOException ex) {
            LOG.warn("Will not add " + CONTAINER_LOG4J_PROPS + " to Distributed Cache.  " +
                    "Some fields in job status may be unavailable", ex);
          }
        }
        else {
          LOG.warn("Could not find " + localFile.getAbsolutePath() + ". " + affectedMsg);
        }
      }
    }
    catch(Throwable t) {
      //this intentionally doesn't use TempletonControllerJob.class.getName() to be able to
      //log errors which may be due to class loading
      String msg = "org.apache.hive.hcatalog.templeton.tool.TempletonControllerJob is not " +
              "properly initialized. " + affectedMsg;
      LOG.error(msg, t);
    }
  }

  private final boolean secureMetastoreAccess;

  /**
   * @param secureMetastoreAccess - if true, a delegation token will be created
   *                              and added to the job
   */
  public TempletonControllerJob(boolean secureMetastoreAccess) {
    super();
    this.secureMetastoreAccess = secureMetastoreAccess;
  }

  private JobID submittedJobId;

  public String getSubmittedId() {
    if (submittedJobId == null) {
      return null;
    } else {
      return submittedJobId.toString();
    }
  }

  /**
   * Enqueue the job and print out the job id for later collection.
   * @see org.apache.hive.hcatalog.templeton.CompleteDelegator
   */
  @Override
  public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException, 
          TException {
    if(LOG.isDebugEnabled()) {
      LOG.debug("Preparing to submit job: " + Arrays.toString(args));
    }
    Configuration conf = getConf();

    conf.set(JAR_ARGS_NAME, TempletonUtils.encodeArray(args));
    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    conf.set("user.name", user);
    if(overrideContainerLog4jProps && overrideLog4jURI != null) {
      //must be done before Job object is created
      conf.set(OVERRIDE_CONTAINER_LOG4J_PROPS, Boolean.TRUE.toString());
    }
    Job job = new Job(conf);
    job.setJarByClass(LaunchMapper.class);
    job.setJobName(TempletonControllerJob.class.getSimpleName());
    job.setMapperClass(LaunchMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setInputFormatClass(SingleInputFormat.class);
    if(overrideContainerLog4jProps && overrideLog4jURI != null) {
      FileSystem fs = FileSystem.get(conf);
      if(fs.exists(new Path(overrideLog4jURI))) {
        ShimLoader.getHadoopShims().getWebHCatShim(conf, UgiFactory.getUgi(user)).addCacheFile(
                overrideLog4jURI, job);
        LOG.debug("added " + overrideLog4jURI + " to Dist Cache");
      }
      else {
        //in case this file was deleted by someone issue a warning but don't try to add to 
        // DistributedCache as that will throw and fail job submission
        LOG.warn("Cannot find " + overrideLog4jURI + " which is created on WebHCat startup/job " +
                "submission.  " + affectedMsg);
      }
    }

    NullOutputFormat<NullWritable, NullWritable> of = new NullOutputFormat<NullWritable, NullWritable>();
    job.setOutputFormatClass(of.getClass());
    job.setNumReduceTasks(0);

    JobClient jc = new JobClient(new JobConf(job.getConfiguration()));

    Token<DelegationTokenIdentifier> mrdt = jc.getDelegationToken(new Text("mr token"));
    job.getCredentials().addToken(new Text("mr token"), mrdt);

    String metastoreTokenStrForm = addHMSToken(job, user);

    job.submit();

    submittedJobId = job.getJobID();
    if(metastoreTokenStrForm != null) {
      //so that it can be cancelled later from CompleteDelegator
      DelegationTokenCache.getStringFormTokenCache().storeDelegationToken(
              submittedJobId.toString(), metastoreTokenStrForm);
      LOG.debug("Added metastore delegation token for jobId=" + submittedJobId.toString() +
              " user=" + user);
    }
    if(overrideContainerLog4jProps && overrideLog4jURI == null) {
      //do this here so that log msg has JobID
      LOG.warn("Could not override container log4j properties for " + submittedJobId);
    }
    return 0;
  }
  private String addHMSToken(Job job, String user) throws IOException, InterruptedException,
          TException {
    if(!secureMetastoreAccess) {
      return null;
    }
    Token<org.apache.hadoop.hive.thrift.DelegationTokenIdentifier> hiveToken =
            new Token<org.apache.hadoop.hive.thrift.DelegationTokenIdentifier>();
    String metastoreTokenStrForm = buildHcatDelegationToken(user);
    hiveToken.decodeFromUrlString(metastoreTokenStrForm);
    job.getCredentials().addToken(new
            Text(SecureProxySupport.HCAT_SERVICE), hiveToken);
    return metastoreTokenStrForm;
  }
  private String buildHcatDelegationToken(String user) throws IOException, InterruptedException,
          TException {
    final HiveConf c = new HiveConf();
    LOG.debug("Creating hive metastore delegation token for user " + user);
    final UserGroupInformation ugi = UgiFactory.getUgi(user);
    UserGroupInformation real = ugi.getRealUser();
    return real.doAs(new PrivilegedExceptionAction<String>() {
      public String run() throws IOException, TException, InterruptedException  {
        final HiveMetaStoreClient client = new HiveMetaStoreClient(c);
        return ugi.doAs(new PrivilegedExceptionAction<String>() {
          public String run() throws IOException, TException, InterruptedException {
            String u = ugi.getUserName();
            return client.getDelegationToken(u);
          }
        });
      }
    });
  }
}
