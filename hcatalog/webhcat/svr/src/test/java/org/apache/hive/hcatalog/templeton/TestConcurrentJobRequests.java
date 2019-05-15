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
 * Test submission of concurrent job requests.
 */
public class TestConcurrentJobRequests extends ConcurrentJobRequestsTestBase {

  private static AppConfig config;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @BeforeClass
  public static void setUp() {
    final String[] args = new String[] {};
    Main main = new Main(args);
    config = main.getAppConfigInstance();
  }

  @Test
  public void ConcurrentJobsStatusSuccess() {
    try {
      JobRunnable jobRunnable = ConcurrentJobsStatus(6, config, false, false,
                statusJobHelper.getDelayedResonseAnswer(4, new QueueStatusBean("job_1000", "Job not found")));
      assertTrue(jobRunnable.exception == null);
    } catch (Exception e) {
      assertTrue(false);
    }
  }

  @Test
  public void ConcurrentListJobsSuccess() {
    try {
      JobRunnable jobRunnable = ConcurrentListJobs(6, config, false, false,
                listJobHelper.getDelayedResonseAnswer(4, new ArrayList<JobItemBean>()));
      assertTrue(jobRunnable.exception == null);
    } catch (Exception e) {
      assertTrue(false);
    }
  }

  @Test
  public void ConcurrentSubmitJobsSuccess() {
    try {
      JobRunnable jobRunnable = SubmitConcurrentJobs(6, config, false, false,
                submitJobHelper.getDelayedResonseAnswer(4, 0),
                killJobHelper.getDelayedResonseAnswer(4, null), "job_1000");
      assertTrue(jobRunnable.exception == null);
    } catch (Exception e) {
      assertTrue(false);
    }
  }
}
