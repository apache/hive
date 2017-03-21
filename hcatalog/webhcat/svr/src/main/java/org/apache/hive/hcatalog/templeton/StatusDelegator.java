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
package org.apache.hive.hcatalog.templeton;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.shims.HadoopShims.WebHCatJTShim;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobProfile;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hive.hcatalog.templeton.tool.JobState;

/**
 * Fetch the status of a given job id in the queue. There are three sources of the info
 * 1. Query result from JobTracker
 * 2. JobState saved by TempletonControllerJob when monitoring the TempletonControllerJob
 * 3. TempletonControllerJob put a JobState for every job it launches, so child job can
 *    retrieve its parent job by its JobState
 * 
 * Currently there is no permission restriction, any user can query any job
 */
public class StatusDelegator extends TempletonDelegator {
  private static final Logger LOG = LoggerFactory.getLogger(StatusDelegator.class);
  private final String JOB_STATUS_EXECUTE_THREAD_PREFIX = "JobStatusExecute";

  /**
   * Current thread id used to set in execution threads.
   */
  private final String statusThreadId = Thread.currentThread().getName();

  /*
   * Job status request executor to get status of a job.
   */
  private static JobRequestExecutor<QueueStatusBean> jobRequest =
                   new JobRequestExecutor<QueueStatusBean>(JobRequestExecutor.JobRequestType.Status,
                   AppConfig.JOB_STATUS_MAX_THREADS, AppConfig.JOB_STATUS_TIMEOUT);

  public StatusDelegator(AppConfig appConf) {
    super(appConf);
  }

  /*
   * Gets status of job form job id. If maximum concurrent job status requests are configured
   * then status request will be executed on a thread from thread pool. If job status request
   * time out is configured then request execution thread will be interrupted if thread
   * times out and does no action.
   */
  public QueueStatusBean run(final String user, final String id, boolean enableThreadPool)
    throws NotAuthorizedException, BadParam, IOException, InterruptedException,
           BusyException, TimeoutException, ExecutionException, TooManyRequestsException {
    if (jobRequest.isThreadPoolEnabled() && enableThreadPool) {
      return jobRequest.execute(getJobStatusCallableTask(user, id));
    } else {
      return getJobStatus(user, id);
    }
  }

  /*
   * Job callable task for job status operation. Overrides behavior of execute() to get
   * status of a job. No need to override behavior of cleanup() as there is nothing to be
   * done if job sttaus operation is timed out or interrupted.
   */
  private JobCallable<QueueStatusBean> getJobStatusCallableTask(final String user,
                                 final String id) {
    return new JobCallable<QueueStatusBean>() {
      @Override
      public QueueStatusBean execute() throws NotAuthorizedException, BadParam, IOException,
                                    InterruptedException, BusyException {
       /*
        * Change the current thread name to include parent thread Id if it is executed
        * in thread pool. Useful to extract logs specific to a job request and helpful
        * to debug job issues.
        */
        Thread.currentThread().setName(String.format("%s-%s-%s", JOB_STATUS_EXECUTE_THREAD_PREFIX,
                                       statusThreadId, Thread.currentThread().getId()));

        return getJobStatus(user, id);
      }
    };
  }

  public QueueStatusBean run(final String user, final String id)
    throws NotAuthorizedException, BadParam, IOException, InterruptedException,
           BusyException, TimeoutException, ExecutionException, TooManyRequestsException {
    return run(user, id, true);
  }

  public QueueStatusBean getJobStatus(String user, String id)
    throws NotAuthorizedException, BadParam, IOException, InterruptedException
  {
    WebHCatJTShim tracker = null;
    JobState state = null;
    try {
      UserGroupInformation ugi = UgiFactory.getUgi(user);
      tracker = ShimLoader.getHadoopShims().getWebHCatShim(appConf, ugi);
      JobID jobid = StatusDelegator.StringToJobID(id);
      if (jobid == null)
        throw new BadParam("Invalid jobid: " + id);
      state = new JobState(id, Main.getAppConfigInstance());
      return StatusDelegator.makeStatus(tracker, jobid, state);
    } catch (IllegalStateException e) {
      throw new BadParam(e.getMessage());
    } finally {
      if (tracker != null)
        tracker.close();
      if (state != null)
        state.close();
    }
  }

  static QueueStatusBean makeStatus(WebHCatJTShim tracker,
                       JobID jobid,
                       JobState state)
    throws BadParam, IOException {

    JobStatus status = tracker.getJobStatus(jobid);
    JobProfile profile = tracker.getJobProfile(jobid);
    if (status == null || profile == null) // No such job.
      throw new BadParam("Could not find job " + jobid);

    return new QueueStatusBean(state, status, profile);
  }

  /**
   * A version of JobID.forName with our app specific error handling.
   */
  public static JobID StringToJobID(String id)
    throws BadParam {
    try {
      return JobID.forName(id);
    } catch (IllegalArgumentException e) {
      throw new BadParam(e.getMessage());
    }
  }
}
