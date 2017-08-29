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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
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
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.templeton.AppConfig;
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
  private static final Logger LOG = LoggerFactory.getLogger(TempletonControllerJob.class);
  private final boolean secureMetastoreAccess;
  private final AppConfig appConf;

  /**
   * @param secureMetastoreAccess - if true, a delegation token will be created
   *                              and added to the job
   */
  public TempletonControllerJob(boolean secureMetastoreAccess, AppConfig conf) {
    super(new Configuration(conf));
    this.secureMetastoreAccess = secureMetastoreAccess;
    this.appConf = conf;
  }

  private Job job = null;

  public String getSubmittedId() {
    if (job == null ) {
      return null;
    }

    JobID submittedJobId = job.getJobID();
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
    String memoryMb = appConf.mapperMemoryMb();
    if(memoryMb != null && memoryMb.length() != 0) {
      conf.set(AppConfig.HADOOP_MAP_MEMORY_MB, memoryMb);
    }
    String amMemoryMB = appConf.amMemoryMb();
    if (amMemoryMB != null && !amMemoryMB.isEmpty()) {
      conf.set(AppConfig.HADOOP_MR_AM_MEMORY_MB, amMemoryMB);
    }
    String amJavaOpts = appConf.controllerAMChildOpts();
    if (amJavaOpts != null && !amJavaOpts.isEmpty()) {
      conf.set(AppConfig.HADOOP_MR_AM_JAVA_OPTS, amJavaOpts);
    }

    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    conf.set("user.name", user);
    job = new Job(conf);
    job.setJarByClass(LaunchMapper.class);
    job.setJobName(TempletonControllerJob.class.getSimpleName());
    job.setMapperClass(LaunchMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setInputFormatClass(SingleInputFormat.class);

    NullOutputFormat<NullWritable, NullWritable> of = new NullOutputFormat<NullWritable, NullWritable>();
    job.setOutputFormatClass(of.getClass());
    job.setNumReduceTasks(0);

    JobClient jc = new JobClient(new JobConf(job.getConfiguration()));

    if(UserGroupInformation.isSecurityEnabled()) {
      Token<DelegationTokenIdentifier> mrdt = jc.getDelegationToken(new Text("mr token"));
      job.getCredentials().addToken(new Text("mr token"), mrdt);
    }
    String metastoreTokenStrForm = addHMSToken(job, user);

    job.submit();

    JobID submittedJobId = job.getJobID();
    if(metastoreTokenStrForm != null) {
      //so that it can be cancelled later from CompleteDelegator
      DelegationTokenCache.getStringFormTokenCache().storeDelegationToken(
              submittedJobId.toString(), metastoreTokenStrForm);
      LOG.debug("Added metastore delegation token for jobId=" + submittedJobId.toString() +
              " user=" + user);
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
      @Override
      public String run() throws IOException, TException, InterruptedException  {
        final IMetaStoreClient client = HCatUtil.getHiveMetastoreClient(c);
        return ugi.doAs(new PrivilegedExceptionAction<String>() {
          @Override
          public String run() throws IOException, TException, InterruptedException {
            String u = ugi.getUserName();
            return client.getDelegationToken(c.getUser(),u);
          }
        });
      }
    });
  }
}
