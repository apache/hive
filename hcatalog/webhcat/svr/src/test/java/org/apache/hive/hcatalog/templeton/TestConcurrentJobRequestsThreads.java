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

import java.util.ArrayList;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import static org.junit.Assert.assertTrue;

/*
 * Test submission of concurrent job requests with the controlled number of concurrent
 * Requests. Verify that we get busy exception and appropriate message.
 */
public class TestConcurrentJobRequestsThreads extends ConcurrentJobRequestsTestBase {

  private static AppConfig config;
  private static QueueStatusBean statusBean;

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
    statusBean = new QueueStatusBean("job_1000", "Job not found");
  }

  @Test
  public void ConcurrentJobsStatusTooManyRequestsException() {
    try {
      JobRunnable jobRunnable = ConcurrentJobsStatus(6, config, false, false,
                statusJobHelper.getDelayedResonseAnswer(4, statusBean));
      assertTrue(jobRunnable.exception != null);
      assertTrue(jobRunnable.exception instanceof TooManyRequestsException);
      TooManyRequestsException ex = (TooManyRequestsException)jobRunnable.exception;
      assertTrue(ex.httpCode == TooManyRequestsException.TOO_MANY_REQUESTS_429);
      String expectedMessage = "Unable to service the status job request as templeton service is busy "
                                 + "with too many status job requests. Please wait for some time before "
                                 + "retrying the operation. Please refer to the config "
                                 + "templeton.parallellism.job.status to configure concurrent requests.";
      assertTrue(jobRunnable.exception.getMessage().contains(expectedMessage));

      /*
       * Verify that new job requests have no issues.
       */
      jobRunnable = ConcurrentJobsStatus(5, config, false, false,
                statusJobHelper.getDelayedResonseAnswer(4, statusBean));
      assertTrue(jobRunnable.exception == null);
    } catch (Exception e) {
      assertTrue(false);
    }
  }

  @Test
  public void ConcurrentListJobsTooManyRequestsException() {
    try {
      JobRunnable jobRunnable = ConcurrentListJobs(6, config, false, false,
                listJobHelper.getDelayedResonseAnswer(4, new ArrayList<JobItemBean>()));
      assertTrue(jobRunnable.exception != null);
      assertTrue(jobRunnable.exception instanceof TooManyRequestsException);
      TooManyRequestsException ex = (TooManyRequestsException)jobRunnable.exception;
      assertTrue(ex.httpCode == TooManyRequestsException.TOO_MANY_REQUESTS_429);
      String expectedMessage = "Unable to service the list job request as templeton service is busy "
                               + "with too many list job requests. Please wait for some time before "
                               + "retrying the operation. Please refer to the config "
                               + "templeton.parallellism.job.list to configure concurrent requests.";
      assertTrue(jobRunnable.exception.getMessage().contains(expectedMessage));

      /*
       * Verify that new job requests have no issues.
       */
      jobRunnable = ConcurrentListJobs(5, config, false, false,
                listJobHelper.getDelayedResonseAnswer(4, new ArrayList<JobItemBean>()));
      assertTrue(jobRunnable.exception == null);
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
      assertTrue(jobRunnable.exception != null);
      assertTrue(jobRunnable.exception instanceof TooManyRequestsException);
      TooManyRequestsException ex = (TooManyRequestsException)jobRunnable.exception;
      assertTrue(ex.httpCode == TooManyRequestsException.TOO_MANY_REQUESTS_429);
      String expectedMessage = "Unable to service the submit job request as templeton service is busy "
                                + "with too many submit job requests. Please wait for some time before "
                                + "retrying the operation. Please refer to the config "
                                + "templeton.parallellism.job.submit to configure concurrent requests.";
      assertTrue(jobRunnable.exception.getMessage().contains(expectedMessage));

      /*
       * Verify that new job requests have no issues.
       */
      jobRunnable = SubmitConcurrentJobs(5, config, false, false,
                submitJobHelper.getDelayedResonseAnswer(4, 0),
                killJobHelper.getDelayedResonseAnswer(0, statusBean), "job_1000");
      assertTrue(jobRunnable.exception == null);
    } catch (Exception e) {
      assertTrue(false);
    }
  }
}
