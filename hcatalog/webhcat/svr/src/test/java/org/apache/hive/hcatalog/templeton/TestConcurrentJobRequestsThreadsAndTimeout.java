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

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeoutException;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import static org.junit.Assert.assertTrue;

/*
 * Test submission of concurrent job requests with the controlled number of concurrent
 * Requests and job request execution time outs. Verify that we get appropriate exceptions
 * and exception message.
 */
@org.junit.Ignore("HIVE-23983")
public class TestConcurrentJobRequestsThreadsAndTimeout extends ConcurrentJobRequestsTestBase {

  private static AppConfig config;
  private static QueueStatusBean statusBean;
  private static String statusTooManyRequestsExceptionMessage;
  private static String listTooManyRequestsExceptionMessage;
  private static String submitTooManyRequestsExceptionMessage;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @BeforeClass
  public static void setUp() {
    final String[] args = new String[] {};
    Main main = new Main(args);
    config = main.getAppConfigInstance();
    config.setInt(AppConfig.JOB_STATUS_MAX_THREADS, 5);
    config.setInt(AppConfig.JOB_LIST_MAX_THREADS, 5);
    config.setInt(AppConfig.JOB_SUBMIT_MAX_THREADS, 5);
    config.setInt(AppConfig.JOB_SUBMIT_TIMEOUT, 5);
    config.setInt(AppConfig.JOB_STATUS_TIMEOUT, 5);
    config.setInt(AppConfig.JOB_LIST_TIMEOUT, 5);
    config.setInt(AppConfig.JOB_TIMEOUT_TASK_RETRY_COUNT, 4);
    config.setInt(AppConfig.JOB_TIMEOUT_TASK_RETRY_INTERVAL, 1);
    statusBean = new QueueStatusBean("job_1000", "Job not found");

    statusTooManyRequestsExceptionMessage = "Unable to service the status job request as "
                                 + "templeton service is busy with too many status job requests. "
                                 + "Please wait for some time before retrying the operation. "
                                 + "Please refer to the config templeton.parallellism.job.status "
                                 + "to configure concurrent requests.";
    listTooManyRequestsExceptionMessage = "Unable to service the list job request as "
                                 + "templeton service is busy with too many list job requests. "
                                 + "Please wait for some time before retrying the operation. "
                                 + "Please refer to the config templeton.parallellism.job.list "
                                 + "to configure concurrent requests.";
    submitTooManyRequestsExceptionMessage = "Unable to service the submit job request as "
                                 + "templeton service is busy with too many submit job requests. "
                                 + "Please wait for some time before retrying the operation. "
                                 + "Please refer to the config templeton.parallellism.job.submit "
                                 + "to configure concurrent requests.";
  }

  @Test
  public void ConcurrentJobsStatusTooManyRequestsException() {
    try {
      JobRunnable jobRunnable = ConcurrentJobsStatus(6, config, false, false,
                statusJobHelper.getDelayedResonseAnswer(4, statusBean));
      verifyTooManyRequestsException(jobRunnable.exception, this.statusTooManyRequestsExceptionMessage);
    } catch (Exception e) {
      assertTrue(false);
    }
  }

  @Test
  public void ConcurrentListJobsTooManyRequestsException() {
    try {
      JobRunnable jobRunnable = ConcurrentListJobs(6, config, false, false,
                listJobHelper.getDelayedResonseAnswer(4, new ArrayList<JobItemBean>()));
      verifyTooManyRequestsException(jobRunnable.exception, this.listTooManyRequestsExceptionMessage);
    } catch (Exception e) {
      assertTrue(false);
    }
  }

  @Test
  public void ConcurrentSubmitJobsTooManyRequestsException() {
    try {
      JobRunnable jobRunnable = SubmitConcurrentJobs(6, config, false, false,
                submitJobHelper.getDelayedResonseAnswer(4, 0),
                killJobHelper.getDelayedResonseAnswer(0, statusBean), "job_1000");
      verifyTooManyRequestsException(jobRunnable.exception, this.submitTooManyRequestsExceptionMessage);
    } catch (Exception e) {
      assertTrue(false);
    }
  }

  @Test
  public void ConcurrentJobsStatusTimeOutException() {
    try {
      JobRunnable jobRunnable = ConcurrentJobsStatus(5, config, false, false,
                statusJobHelper.getDelayedResonseAnswer(6, statusBean));
      assertTrue(jobRunnable.exception != null);
      assertTrue(jobRunnable.exception instanceof TimeoutException);
      String expectedMessage = "Status job request got timed out. Please wait for some time before "
                               + "retrying the operation. Please refer to the config "
                               + "templeton.job.status.timeout to configure job request time out.";
      assertTrue(jobRunnable.exception.getMessage().contains(expectedMessage));

      /*
       * Verify that new job requests should succeed with no issues.
       */
      jobRunnable = ConcurrentJobsStatus(5, config, false, false,
                statusJobHelper.getDelayedResonseAnswer(0, statusBean));
      assertTrue(jobRunnable.exception == null);
    } catch (Exception e) {
      assertTrue(false);
    }
  }

  @Test
  public void ConcurrentListJobsTimeOutException() {
    try {
      JobRunnable jobRunnable = ConcurrentListJobs(5, config, false, false,
                listJobHelper.getDelayedResonseAnswer(6, new ArrayList<JobItemBean>()));
      assertTrue(jobRunnable.exception != null);
      assertTrue(jobRunnable.exception instanceof TimeoutException);
      String expectedMessage = "List job request got timed out. Please wait for some time before "
                               + "retrying the operation. Please refer to the config "
                               + "templeton.job.list.timeout to configure job request time out.";

      assertTrue(jobRunnable.exception.getMessage().contains(expectedMessage));

      /*
       * Verify that new job requests should succeed with no issues.
       */
      jobRunnable = ConcurrentListJobs(5, config, false, false,
                listJobHelper.getDelayedResonseAnswer(1, new ArrayList<JobItemBean>()));
      assertTrue(jobRunnable.exception == null);
    } catch (Exception e) {
      assertTrue(false);
    }
  }

  @Test
  public void ConcurrentSubmitJobsTimeOutException() {
    try {
      JobRunnable jobRunnable = SubmitConcurrentJobs(5, config, false, false,
                submitJobHelper.getDelayedResonseAnswer(6, 0),
                killJobHelper.getDelayedResonseAnswer(0, statusBean), "job_1000");
      assertTrue(jobRunnable.exception != null);
      assertTrue(jobRunnable.exception instanceof QueueException);
      String expectedMessage = "Submit job request got timed out. Please wait for some time before "
                               + "retrying the operation. Please refer to the config "
                               + "templeton.job.submit.timeout to configure job request time out.";
      assertTrue(jobRunnable.exception.getMessage().contains(expectedMessage));

      /*
       * For submit operation, tasks are not cancelled. Verify that new job request
       * should fail with TooManyRequestsException.
       */
      jobRunnable = SubmitConcurrentJobs(1, config, false, false,
                submitJobHelper.getDelayedResonseAnswer(0, 0),
                killJobHelper.getDelayedResonseAnswer(0, statusBean), "job_1000");
      verifyTooManyRequestsException(jobRunnable.exception, this.submitTooManyRequestsExceptionMessage);

     /*
      * Sleep until all threads with clean up tasks are completed.
      */
      Thread.sleep(2000);

      /*
       * Now, tasks would have passed. Verify that new job requests should succeed with no issues.
       */
      jobRunnable = SubmitConcurrentJobs(5, config, false, false,
                submitJobHelper.getDelayedResonseAnswer(0, 0),
                killJobHelper.getDelayedResonseAnswer(0, statusBean), "job_1000");
      assertTrue(jobRunnable.exception == null);
    } catch (Exception e) {
      assertTrue(false);
    }
  }

  @Test
  public void ConcurrentStatusJobsVerifyExceptions() {
    try {
      /*
       * Trigger kill threads and verify we get InterruptedException and expected Message.
       */
      int timeoutTaskDelay = 4;
      JobRunnable jobRunnable = ConcurrentJobsStatus(5, config, true, false,
                statusJobHelper.getDelayedResonseAnswer(timeoutTaskDelay, statusBean));
      assertTrue(jobRunnable.exception != null);
      assertTrue(jobRunnable.exception instanceof InterruptedException);
      String expectedMessage = "Status job request got interrupted. Please wait for some time before "
                               + "retrying the operation.";
      assertTrue(jobRunnable.exception.getMessage().contains(expectedMessage));

      /*
       * Interrupt all thread and verify we get InterruptedException and expected Message.
       */
      jobRunnable = ConcurrentJobsStatus(5, config, false, true,
                statusJobHelper.getDelayedResonseAnswer(timeoutTaskDelay, statusBean));
      assertTrue(jobRunnable.exception != null);
      assertTrue(jobRunnable.exception instanceof InterruptedException);
      assertTrue(jobRunnable.exception.getMessage().contains(expectedMessage));

      /*
       * Raise custom exception like IOException and verify expected Message.
       */
      jobRunnable = ConcurrentJobsStatus(5, config, false, false,
                                     statusJobHelper.getIOExceptionAnswer());
      assertTrue(jobRunnable.exception != null);
      assertTrue(jobRunnable.exception.getCause() instanceof IOException);

      /*
       * Now new job requests should succeed as status operation has no cancel threads.
       */
      jobRunnable = ConcurrentJobsStatus(5, config, false, false,
                statusJobHelper.getDelayedResonseAnswer(0, statusBean));
      assertTrue(jobRunnable.exception == null);
    } catch (Exception e) {
      assertTrue(false);
    }
  }

  @Test
  public void ConcurrentListJobsVerifyExceptions() {
    try {
      /*
       * Trigger kill threads and verify we get InterruptedException and expected Message.
       */
      int timeoutTaskDelay = 4;
      JobRunnable jobRunnable = ConcurrentListJobs(5, config, true, false,
                listJobHelper.getDelayedResonseAnswer(timeoutTaskDelay, new ArrayList<JobItemBean>()));
      assertTrue(jobRunnable.exception != null);
      assertTrue(jobRunnable.exception instanceof InterruptedException);
      String expectedMessage = "List job request got interrupted. Please wait for some time before "
                               + "retrying the operation.";
      assertTrue(jobRunnable.exception.getMessage().contains(expectedMessage));

      /*
       * Interrupt all thread and verify we get InterruptedException and expected Message.
       */
      jobRunnable = ConcurrentListJobs(5, config, false, true,
                listJobHelper.getDelayedResonseAnswer(timeoutTaskDelay, new ArrayList<JobItemBean>()));
      assertTrue(jobRunnable.exception != null);
      assertTrue(jobRunnable.exception instanceof InterruptedException);
      assertTrue(jobRunnable.exception.getMessage().contains(expectedMessage));

      /*
       * Raise custom exception like IOException and verify expected Message.
       */
      jobRunnable = ConcurrentListJobs(5, config, false, false,
                listJobHelper.getIOExceptionAnswer());
      assertTrue(jobRunnable.exception != null);
      assertTrue(jobRunnable.exception.getCause() instanceof IOException);

      /*
       * Now new job requests should succeed as list operation has no cancel threads.
       */
      jobRunnable = ConcurrentListJobs(5, config, false, false,
                listJobHelper.getDelayedResonseAnswer(0, new ArrayList<JobItemBean>()));
      assertTrue(jobRunnable.exception == null);
    } catch (Exception e) {
      assertTrue(false);
    }
  }

  @Test
  public void ConcurrentSubmitJobsVerifyExceptions() {
    try {
      int timeoutTaskDelay = 4;

      /*
       * Raise custom exception like IOException and verify expected Message.
       * This should not invoke cancel operation.
       */
      JobRunnable jobRunnable = SubmitConcurrentJobs(1, config, false, false,
                submitJobHelper.getIOExceptionAnswer(),
                killJobHelper.getDelayedResonseAnswer(timeoutTaskDelay, statusBean), "job_1002");
      assertTrue(jobRunnable.exception != null);
      assertTrue(jobRunnable.exception instanceof QueueException);
      assertTrue(jobRunnable.exception.getMessage().contains("IOException raised manually."));

      /*
       * Raise custom exception like IOException and verify expected Message.
       * This should not invoke cancel operation.
       */
      jobRunnable = SubmitConcurrentJobs(1, config, false, false,
                submitJobHelper.getOutOfMemoryErrorAnswer(),
                killJobHelper.getDelayedResonseAnswer(timeoutTaskDelay, statusBean), "job_1003");
      assertTrue(jobRunnable.exception != null);
      assertTrue(jobRunnable.exception instanceof QueueException);
      assertTrue(jobRunnable.exception.getMessage().contains("OutOfMemoryError raised manually."));

      /*
       * Trigger kill threads and verify that we get InterruptedException and expected
       * Message. This should raise 3 kill operations and ensure that retries keep the time out
       * occupied for 4 sec.
       */
      jobRunnable = SubmitConcurrentJobs(3, config, true, false,
                submitJobHelper.getDelayedResonseAnswer(2, 0),
                killJobHelper.getDelayedResonseAnswer(timeoutTaskDelay, statusBean), "job_1000");
      assertTrue(jobRunnable.exception != null);
      assertTrue(jobRunnable.exception instanceof QueueException);
      String expectedMessage = "Submit job request got interrupted. Please wait for some time "
                               + "before retrying the operation.";
      assertTrue(jobRunnable.exception.getMessage().contains(expectedMessage));

      /*
       * Interrupt all threads and verify we get InterruptedException and expected
       * Message. Also raise 2 kill operations and ensure that retries keep the time out
       * occupied for 4 sec.
       */
      jobRunnable = SubmitConcurrentJobs(2, config, false, true,
                submitJobHelper.getDelayedResonseAnswer(2, 0),
                killJobHelper.getDelayedResonseAnswer(0, statusBean), "job_1001");
      assertTrue(jobRunnable.exception != null);
      assertTrue(jobRunnable.exception instanceof QueueException);
      assertTrue(jobRunnable.exception.getMessage().contains(expectedMessage));

      /*
       * For submit operation, tasks are not cancelled. Verify that new job request
       * should fail with TooManyRequestsException.
       */
      jobRunnable = SubmitConcurrentJobs(1, config, false, false,
                submitJobHelper.getDelayedResonseAnswer(0, 0),
                killJobHelper.getDelayedResonseAnswer(0, statusBean), "job_1002");
      verifyTooManyRequestsException(jobRunnable.exception, this.submitTooManyRequestsExceptionMessage);

      /*
       * Sleep until all threads with clean up tasks are completed. 2 seconds completing task
       * and 1 sec grace period.
       */
      Thread.sleep((timeoutTaskDelay + 2 + 1) * 1000);

      /*
       * Now new job requests should succeed as all cancel threads would have completed.
       */
      jobRunnable = SubmitConcurrentJobs(5, config, false, false,
                submitJobHelper.getDelayedResonseAnswer(0, 0),
                killJobHelper.getDelayedResonseAnswer(0, statusBean), "job_1004");
      assertTrue(jobRunnable.exception == null);
    } catch (Exception e) {
      assertTrue(false);
    }
  }

  private void verifyTooManyRequestsException(Throwable exception, String expectedMessage) {
      assertTrue(exception != null);
      assertTrue(exception instanceof TooManyRequestsException);
      TooManyRequestsException ex = (TooManyRequestsException)exception;
      assertTrue(ex.httpCode == TooManyRequestsException.TOO_MANY_REQUESTS_429);
      assertTrue(exception.getMessage().contains(expectedMessage));
  }

}
