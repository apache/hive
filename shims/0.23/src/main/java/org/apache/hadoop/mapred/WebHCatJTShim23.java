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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hive.shims.HadoopShims.WebHCatJTShim;

import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.ApplicationsRequestScope;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class WebHCatJTShim23 implements WebHCatJTShim {
  private static final Log LOG = LogFactory.getLog(WebHCatJTShim23.class);
  private JobClient jc;
  private final Configuration conf;

  /**
   * Create a connection to the Job Tracker.
   */
  public WebHCatJTShim23(final Configuration conf, final UserGroupInformation ugi)
          throws IOException {
    try {
    this.conf = conf;
    jc = ugi.doAs(new PrivilegedExceptionAction<JobClient>() {
      public JobClient run() throws IOException, InterruptedException  {
        //create this in doAs() so that it gets a security context based passed in 'ugi'
        return new JobClient(conf);
      }
    });
    }
    catch(InterruptedException ex) {
      throw new RuntimeException("Failed to create JobClient", ex);
    }
  }

  /**
   * Grab a handle to a job that is already known to the JobTracker.
   *
   * @return Profile of the job, or null if not found.
   */
  public JobProfile getJobProfile(JobID jobid)
          throws IOException {
    RunningJob rj = getJob(jobid);
    if(rj == null) {
      return null;
    }
    JobStatus jobStatus = rj.getJobStatus();
    return new JobProfile(jobStatus.getUsername(), jobStatus.getJobID(),
            jobStatus.getJobFile(), jobStatus.getTrackingUrl(), jobStatus.getJobName());
  }

  /**
   * Grab a handle to a job that is already known to the JobTracker.
   *
   * @return Status of the job, or null if not found.
   */
  public JobStatus getJobStatus(JobID jobid)
          throws IOException {
    RunningJob rj = getJob(jobid);
    if(rj == null) {
      return null;
    }
    return rj.getJobStatus();
  }


  /**
   * Kill a job.
   */
  public void killJob(JobID jobid)
          throws IOException {
    RunningJob rj = getJob(jobid);
    if(rj == null) {
      return;
    }
    rj.killJob();
  }

  /**
   * Get all the jobs submitted.
   */
  public JobStatus[] getAllJobs()
          throws IOException {
    return jc.getAllJobs();
  }

  /**
   * Close the connection to the Job Tracker.
   */
  public void close() {
    try {
      jc.close();
    } catch (IOException e) {
    }
  }
  @Override
  public void addCacheFile(URI uri, Job job) {
    job.addCacheFile(uri);
  }
  /**
   * @return {@code null} if no such application exists
   */
  private RunningJob getJob(JobID jobid) throws IOException {
    try {
      return jc.getJob(jobid);
    }
    catch(IOException ex) {
      String msg = ex.getMessage();
      if(msg != null && msg.contains("ApplicationNotFoundException")) {
        LOG.info("Job(" + jobid + ") not found: " + msg);
        return null;
      }
      throw ex;
    }
  }

  /**
   * Kills all jobs tagged with the given tag that have been started after the
   * given timestamp.
   */
  @Override
  public void killJobs(String tag, long timestamp) {
    try {
      LOG.info("Looking for jobs to kill...");
      Set<ApplicationId> childJobs = getYarnChildJobs(tag, timestamp);
      if (childJobs.isEmpty()) {
        LOG.info("No jobs found from");
        return;
      } else {
        LOG.info(String.format("Found MR jobs count: %d", childJobs.size()));
        LOG.info("Killing all found jobs");

        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
        for (ApplicationId app: childJobs) {
          LOG.info(String.format("Killing job: %s ...", app));
          yarnClient.killApplication(app);
          LOG.info(String.format("Job %s killed", app));
        }
      }
    } catch (YarnException ye) {
      throw new RuntimeException("Exception occurred while killing child job(s)", ye);
    } catch (IOException ioe) {
      throw new RuntimeException("Exception occurred while killing child job(s)", ioe);
    }
  }

  /**
   * Queries RM for the list of applications with the given tag that have started
   * after the given timestamp.
   */
  private Set<ApplicationId> getYarnChildJobs(String tag, long timestamp) {
    Set<ApplicationId> childYarnJobs = new HashSet<ApplicationId>();

    LOG.info(String.format("Querying RM for tag = %s, starting with ts = %s", tag, timestamp));

    GetApplicationsRequest gar = GetApplicationsRequest.newInstance();
    gar.setScope(ApplicationsRequestScope.OWN);
    gar.setStartRange(timestamp, System.currentTimeMillis());
    gar.setApplicationTags(Collections.singleton(tag));
    try {
      ApplicationClientProtocol proxy = ClientRMProxy.createRMProxy(conf,
          ApplicationClientProtocol.class);
      GetApplicationsResponse apps = proxy.getApplications(gar);
      List<ApplicationReport> appsList = apps.getApplicationList();
      for(ApplicationReport appReport : appsList) {
        childYarnJobs.add(appReport.getApplicationId());
      }
    } catch (IOException ioe) {
      throw new RuntimeException("Exception occurred while finding child jobs", ioe);
    } catch (YarnException ye) {
      throw new RuntimeException("Exception occurred while finding child jobs", ye);
    }
    return childYarnJobs;
  }
}
