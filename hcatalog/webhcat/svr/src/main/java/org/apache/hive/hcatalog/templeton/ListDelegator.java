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
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.shims.HadoopShims.WebHCatJTShim;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * List jobs owned by a user.
 */
public class ListDelegator extends TempletonDelegator {
  private static final Log LOG = LogFactory.getLog(ListDelegator.class);
  private final String JOB_LIST_EXECUTE_THREAD_PREFIX = "JobListExecute";

  /**
   * Current thread id used to set in execution threads.
   */
  private final String listThreadId = Thread.currentThread().getName();

  /*
   * Job request executor to list job status requests.
   */
  private static JobRequestExecutor<List<JobItemBean>> jobRequest =
                   new JobRequestExecutor<List<JobItemBean>>(JobRequestExecutor.JobRequestType.List,
                   AppConfig.JOB_LIST_MAX_THREADS, AppConfig.JOB_LIST_TIMEOUT);

  public ListDelegator(AppConfig appConf) {
    super(appConf);
  }

  /*
   * List status jobs request. If maximum concurrent job list requests are configured then
   * list request will be executed on a thread from thread pool. If job list request time out
   * is configured then request execution thread will be interrupted if thread times out and
   * does no action.
   */
  public List<JobItemBean> run(final String user, final boolean showall, final String jobId,
                               final int numRecords, final boolean showDetails)
    throws NotAuthorizedException, BadParam, IOException, InterruptedException, BusyException,
           TimeoutException, ExecutionException, TooManyRequestsException {

    if (jobRequest.isThreadPoolEnabled()) {
      return jobRequest.execute(getJobListTask(user, showall, jobId,numRecords, showDetails));
    } else {
      return listJobs(user, showall, jobId, numRecords, showDetails);
    }
  }

  /*
   * Job callable task for job list operation. Overrides behavior of execute() to list jobs.
   * No need to override behavior of cleanup() as there is nothing to be done if list jobs
   * operation is timed out or interrupted.
   */
  private JobCallable<List<JobItemBean>> getJobListTask(final String user, final boolean showall,
                      final String jobId, final int numRecords, final boolean showDetails) {
    return new JobCallable<List<JobItemBean>>() {
      @Override
      public List<JobItemBean> execute() throws NotAuthorizedException, BadParam, IOException,
                                             InterruptedException {
       /*
        * Change the current thread name to include parent thread Id if it is executed
        * in thread pool. Useful to extract logs specific to a job request and helpful
        * to debug job issues.
        */
        Thread.currentThread().setName(String.format("%s-%s-%s", JOB_LIST_EXECUTE_THREAD_PREFIX,
                                       listThreadId, Thread.currentThread().getId()));

        return listJobs(user, showall, jobId, numRecords, showDetails);
      }
    };
  }

  /*
   * Gets list of job ids and calls getJobStatus to get status for each job id.
   */
  public List<JobItemBean> listJobs(String user, boolean showall, String jobId,
                                    int numRecords, boolean showDetails)
    throws NotAuthorizedException, BadParam, IOException, InterruptedException {

    UserGroupInformation ugi = UgiFactory.getUgi(user);
    WebHCatJTShim tracker = null;
    ArrayList<String> ids = new ArrayList<String>();

    try {
      tracker = ShimLoader.getHadoopShims().getWebHCatShim(appConf, ugi);

      JobStatus[] jobs = tracker.getAllJobs();

      if (jobs != null) {
        for (JobStatus job : jobs) {
          String id = job.getJobID().toString();
          if (showall || user.equals(job.getUsername()))
            ids.add(id);
        }
      }
    } catch (IllegalStateException e) {
      throw new BadParam(e.getMessage());
    } finally {
      if (tracker != null)
        tracker.close();
    }

    return getJobStatus(ids, user, showall, jobId, numRecords, showDetails);
  }

  /*
   * Returns job status for list of input jobs as a list.
   */
  public List<JobItemBean> getJobStatus(ArrayList<String> jobIds, String user, boolean showall,
                                       String jobId, int numRecords, boolean showDetails)
                                       throws IOException, InterruptedException {

    List<JobItemBean> detailList = new ArrayList<JobItemBean>();
    int currRecord = 0;

    // Sort the list as requested
    boolean isAscendingOrder = true;
    switch (appConf.getListJobsOrder()) {
    case lexicographicaldesc:
      Collections.sort(jobIds, Collections.reverseOrder());
      isAscendingOrder = false;
      break;
    case lexicographicalasc:
    default:
      Collections.sort(jobIds);
      break;
    }

    for (String job : jobIds) {
      // If numRecords = -1, fetch all records.
      // Hence skip all the below checks when numRecords = -1.
      if (numRecords != -1) {
        // If currRecord >= numRecords, we have already fetched the top #numRecords
        if (currRecord >= numRecords) {
          break;
        }
        else if (jobId == null || jobId.trim().length() == 0) {
            currRecord++;
        }
        // If the current record needs to be returned based on the
        // filter conditions specified by the user, increment the counter
        else if (isAscendingOrder && job.compareTo(jobId) > 0 || !isAscendingOrder && job.compareTo(jobId) < 0) {
          currRecord++;
        }
        // The current record should not be included in the output detailList.
        else {
          continue;
        }
      }
      JobItemBean jobItem = new JobItemBean();
      jobItem.id = job;
      if (showDetails) {
        StatusDelegator sd = new StatusDelegator(appConf);
        try {
          jobItem.detail = sd.run(user, job, false);
        }
        catch(Exception ex) {
          /*
           * if we could not get status for some reason, log it, and send empty status back with
           * just the ID so that caller knows to even look in the log file
           */
          LOG.info("Failed to get status detail for jobId='" + job + "'", ex);
          jobItem.detail = new QueueStatusBean(job, "Failed to retrieve status; see WebHCat logs");
        }
      }
      detailList.add(jobItem);
    }

    return detailList;
  }
}
