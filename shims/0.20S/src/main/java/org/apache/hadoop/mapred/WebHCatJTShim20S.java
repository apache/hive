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
package org.apache.hadoop.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.hive.shims.HadoopShims.WebHCatJTShim;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

/**
 * This is in org.apache.hadoop.mapred package because it relies on
 * JobSubmissionProtocol which is package private
 */
public class WebHCatJTShim20S implements WebHCatJTShim {
  private JobSubmissionProtocol cnx;

  /**
   * Create a connection to the Job Tracker.
   */
  public WebHCatJTShim20S(Configuration conf, UserGroupInformation ugi)
          throws IOException {
    cnx = (JobSubmissionProtocol)
            RPC.getProxy(JobSubmissionProtocol.class,
                    JobSubmissionProtocol.versionID,
                    getAddress(conf),
                    ugi,
                    conf,
                    NetUtils.getSocketFactory(conf,
                            JobSubmissionProtocol.class));
  }

  /**
   * Grab a handle to a job that is already known to the JobTracker.
   *
   * @return Profile of the job, or null if not found.
   */
  public JobProfile getJobProfile(org.apache.hadoop.mapred.JobID jobid)
          throws IOException {
    return cnx.getJobProfile(jobid);
  }

  /**
   * Grab a handle to a job that is already known to the JobTracker.
   *
   * @return Status of the job, or null if not found.
   */
  public org.apache.hadoop.mapred.JobStatus getJobStatus(org.apache.hadoop.mapred.JobID jobid)
          throws IOException {
    return cnx.getJobStatus(jobid);
  }


  /**
   * Kill a job.
   */
  public void killJob(org.apache.hadoop.mapred.JobID jobid)
          throws IOException {
    cnx.killJob(jobid);
  }

  /**
   * Get all the jobs submitted.
   */
  public org.apache.hadoop.mapred.JobStatus[] getAllJobs()
          throws IOException {
    return cnx.getAllJobs();
  }

  /**
   * Close the connection to the Job Tracker.
   */
  public void close() {
    RPC.stopProxy(cnx);
  }
  private InetSocketAddress getAddress(Configuration conf) {
    String jobTrackerStr = conf.get("mapred.job.tracker", "localhost:8012");
    return NetUtils.createSocketAddr(jobTrackerStr);
  }
  @Override
  public void addCacheFile(URI uri, Job job) {
    DistributedCache.addCacheFile(uri, job.getConfiguration());
  }
  /**
   * Kill jobs is only supported on hadoop 2.0+.
   */
  @Override
  public void killJobs(String tag, long timestamp) {
    return;
  }
  /**
   * Get jobs is only supported on hadoop 2.0+.
   */
  @Override
  public Set<String> getJobs(String tag, long timestamp)
  {
    return new HashSet<String>();
  }
}

