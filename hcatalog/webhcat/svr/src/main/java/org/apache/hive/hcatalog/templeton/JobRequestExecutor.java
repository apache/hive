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
package org.apache.hive.hcatalog.templeton;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Future;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobRequestExecutor<T> {
  private static final Logger LOG = LoggerFactory.getLogger(JobRequestExecutor.class);
  private static AppConfig appConf = Main.getAppConfigInstance();

  /*
   * Thread pool to execute job requests.
   */
  private ThreadPoolExecutor jobExecutePool = null;

  /*
   * Type of job request.
   */
  private JobRequestType requestType;

  /*
   * Config name used to find the number of concurrent requests.
   */
  private String concurrentRequestsConfigName;

  /*
   * Config name used to find the maximum time job request can be executed.
   */
  private String jobTimeoutConfigName;

  /*
   * Job request execution time out in seconds. If it is 0 then request
   * will not be timed out.
   */
  private int requestExecutionTimeoutInSec = 0;

  /*
   * Amount of time a thread can be alive in thread pool before cleaning this up. Core threads
   * will not be cleanup from thread pool.
   */
  private int threadKeepAliveTimeInHours = 1;

  /*
   * Maximum number of times a cancel request is sent to job request execution
   * thread. Future.cancel may not be able to interrupt the thread if it is
   * blocked on network calls.
   */
  private int maxTaskCancelRetryCount = 10;

  /*
   * Wait time in milliseconds before another cancel request is made.
   */
  private int maxTaskCancelRetryWaitTimeInMs = 1000;

  /*
   * A flag to indicate whether to cancel the task when exception TimeoutException or
   * InterruptedException or CancellationException raised. The default is cancel thread.
   */
  private boolean enableCancelTask = true;

  /*
   * Job Request type.
   */
  public enum JobRequestType {
    Submit,
    Status,
    List
  }

  /*
   * Creates a job request object and sets up execution environment. Creates a thread pool
   * to execute job requests.
   *
   * @param requestType
   *          Job request type
   *
   * @param concurrentRequestsConfigName
   *          Config name to be used to extract number of concurrent requests to be serviced.
   *
   * @param jobTimeoutConfigName
   *          Config name to be used to extract maximum time a task can execute a request.
   *
   * @param enableCancelTask
   *          A flag to indicate whether to cancel the task when exception TimeoutException
   *          or InterruptedException or CancellationException raised.
   *
   */
  public JobRequestExecutor(JobRequestType requestType, String concurrentRequestsConfigName,
                            String jobTimeoutConfigName, boolean enableCancelTask) {

    this.concurrentRequestsConfigName = concurrentRequestsConfigName;
    this.jobTimeoutConfigName = jobTimeoutConfigName;
    this.requestType = requestType;
    this.enableCancelTask = enableCancelTask;

    /*
     * The default number of threads will be 0. That means thread pool is not used and
     * operation is executed with the current thread.
     */
    int threads = !StringUtils.isEmpty(concurrentRequestsConfigName) ?
                                appConf.getInt(concurrentRequestsConfigName, 0) : 0;

    if (threads > 0) {
      /*
       * Create a thread pool with no queue wait time to execute the operation. This will ensure
       * that job requests are rejected if there are already maximum number of threads busy.
       */
      this.jobExecutePool = new ThreadPoolExecutor(threads, threads,
                             threadKeepAliveTimeInHours, TimeUnit.HOURS,
                             new SynchronousQueue<Runnable>());
       this.jobExecutePool.allowCoreThreadTimeOut(true);

      /*
       * Get the job request time out value. If this configuration value is set to 0
       * then job request will wait until it finishes.
       */
      if (!StringUtils.isEmpty(jobTimeoutConfigName)) {
        this.requestExecutionTimeoutInSec = appConf.getInt(jobTimeoutConfigName, 0);
      }

      LOG.info("Configured " + threads + " threads for job request type " + this.requestType
                 + " with time out " + this.requestExecutionTimeoutInSec + " s.");
    } else {
      /*
       * If threads are not configured then they will be executed in current thread itself.
       */
      LOG.info("No thread pool configured for job request type " + this.requestType);
    }
  }

  /*
   * Creates a job request object and sets up execution environment. Creates a thread pool
   * to execute job requests.
   *
   * @param requestType
   *          Job request type
   *
   * @param concurrentRequestsConfigName
   *          Config name to be used to extract number of concurrent requests to be serviced.
   *
   * @param jobTimeoutConfigName
   *          Config name to be used to extract maximum time a task can execute a request.
   *
   */
  public JobRequestExecutor(JobRequestType requestType, String concurrentRequestsConfigName,
                            String jobTimeoutConfigName) {
    this(requestType, concurrentRequestsConfigName, jobTimeoutConfigName, true);
  }

  /*
   * Returns true of thread pool is created and can be used for executing a job request.
   * Otherwise, returns false.
   */
  public boolean isThreadPoolEnabled() {
    return this.jobExecutePool != null;
  }

  /*
   * Executes job request operation. If thread pool is not created then job request is
   * executed in current thread itself.
   *
   * @param jobExecuteCallable
   *          Callable object to run the job request task.
   *
   */
  public T execute(JobCallable<T> jobExecuteCallable) throws InterruptedException,
                 TimeoutException, TooManyRequestsException, ExecutionException {
    /*
     * The callable shouldn't be null to execute. The thread pool also should be configured
     * to execute requests.
     */
    assert (jobExecuteCallable != null);
    assert (this.jobExecutePool != null);

    String type = this.requestType.toString().toLowerCase();

    String retryMessageForConcurrentRequests = "Please wait for some time before retrying "
                  + "the operation. Please refer to the config " + concurrentRequestsConfigName
                  + " to configure concurrent requests.";

    LOG.debug("Starting new " + type + " job request with time out " + this.requestExecutionTimeoutInSec
              + "seconds.");
    Future<T> future = null;

    try {
      future = this.jobExecutePool.submit(jobExecuteCallable);
    } catch (RejectedExecutionException rejectedException) {
      /*
       * Not able to find thread to execute the job request. Raise Busy exception and client
       * can retry the operation.
       */
      String tooManyRequestsExceptionMessage = "Unable to service the " + type + " job request as "
                        + "templeton service is busy with too many " + type + " job requests. "
                        + retryMessageForConcurrentRequests;

      LOG.warn(tooManyRequestsExceptionMessage);
      throw new TooManyRequestsException(tooManyRequestsExceptionMessage);
    }

    T result = null;

    try {
      result = this.requestExecutionTimeoutInSec > 0
                ? future.get(this.requestExecutionTimeoutInSec, TimeUnit.SECONDS) : future.get();
    } catch (TimeoutException e) {
      /*
       * See if the execution thread has just completed operation and result is available.
       * If result is available then return the result. Otherwise, raise exception.
       */
      if ((result = tryGetJobResultOrSetJobStateFailed(jobExecuteCallable)) == null) {
        String message = this.requestType + " job request got timed out. Please wait for some time "
                       + "before retrying the operation. Please refer to the config "
                       + jobTimeoutConfigName + " to configure job request time out.";
        LOG.warn(message);

        /*
         * Throw TimeoutException to caller.
         */
        throw new TimeoutException(message);
      }
    } catch (InterruptedException e) {
      /*
       * See if the execution thread has just completed operation and result is available.
       * If result is available then return the result. Otherwise, raise exception.
       */
      if ((result = tryGetJobResultOrSetJobStateFailed(jobExecuteCallable)) == null) {
        String message = this.requestType + " job request got interrupted. Please wait for some time "
                       + "before retrying the operation.";
        LOG.warn(message);

        /*
         * Throw TimeoutException to caller.
         */
        throw new InterruptedException(message);
      }
    } catch (CancellationException e) {
      /*
       * See if the execution thread has just completed operation and result is available.
       * If result is available then return the result. Otherwise, raise exception.
       */
      if ((result = tryGetJobResultOrSetJobStateFailed(jobExecuteCallable)) == null) {
        String message = this.requestType + " job request got cancelled and thread got interrupted. "
                       + "Please wait for some time before retrying the operation.";
        LOG.warn(message);

        throw new InterruptedException(message);
      }
    } finally {
      /*
       * If the thread is still active and needs to be cancelled then cancel it. This may
       * happen in case task got interrupted, or timed out.
       */
      if (enableCancelTask) {
        cancelExecutePoolThread(future);
      }
    }

    LOG.debug("Completed " + type + " job request.");

    return result;
  }

  /*
   * Initiate cancel request to cancel the thread execution and interrupt the thread.
   * If thread interruption is not handled by jobExecuteCallable then thread may continue
   * running to completion. The cancel call may fail for some scenarios. In that case,
   * retry the cancel call until it returns true or max retry count is reached.
   *
   * @param future
   *          Future object which has handle to cancel the thread.
   *
   */
  private void cancelExecutePoolThread(Future<T> future) {
    int retryCount = 0;
    while(retryCount < this.maxTaskCancelRetryCount && !future.isDone()) {
      LOG.info("Task is still executing the job request. Cancelling it with retry count: "
               + retryCount);
      if (future.cancel(true)) {
        /*
         * Cancelled the job request and return to client.
         */
        LOG.info("Cancel job request issued successfully.");
        return;
      }

      retryCount++;
      try {
        Thread.sleep(this.maxTaskCancelRetryWaitTimeInMs);
      } catch (InterruptedException e) {
        /*
         * Nothing to do. Just retry.
         */
      }
    }

    LOG.warn("Failed to cancel the job. isCancelled: " + future.isCancelled()
                    + " Retry count: " + retryCount);
  }

  /*
   * Tries to get the job result if job request is completed. Otherwise it sets job status
   * to FAILED such that execute thread can do necessary clean up based on FAILED state.
   */
  private T tryGetJobResultOrSetJobStateFailed(JobCallable<T> jobExecuteCallable) {
    if (!jobExecuteCallable.setJobStateFailed()) {
      LOG.info("Job is already COMPLETED. Returning the result.");
      return jobExecuteCallable.returnResult;
    } else {
      LOG.info("Job status set to FAILED. Job clean up to be done by execute thread "
              + "after job request is executed.");
      return null;
    }
  }
}
