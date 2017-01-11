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

package org.apache.hadoop.hive.ql.session;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.shims.ShimLoader;

import java.io.IOException;

/**
 * A front handle for managing job submission to Yarn-FairScheduler.
 */
public class YarnFairScheduling {
  /**
   * Determine if jobs can be configured for YARN fair scheduling.
   * @param conf - the current HiveConf configuration.
   * @return Returns true when impersonation mode is disabled and fair-scheduling is enabled.
   */
  public static boolean usingNonImpersonationModeWithFairScheduling(HiveConf conf) {
    return (conf != null)
      && (!conf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS)
      && (conf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_MAP_FAIR_SCHEDULER_QUEUE)));
  }

  /**
   * Configure the default YARN queue for the user.
   * @param conf - The current HiveConf configuration.
   * @param forUser - The user to configure scheduling for.
   * @throws IOException
   * @throws HiveException
   */
  public static void setDefaultJobQueue(HiveConf conf, String forUser) throws IOException, HiveException {
    Preconditions.checkState(usingNonImpersonationModeWithFairScheduling(conf),
      "Unable to map job to fair-scheduler because either impersonation is on or fair-scheduling is disabled.");

    ShimLoader.getSchedulerShims().refreshDefaultQueue(conf, forUser);
  }

  /**
   * Validate the current YARN queue for the current user.
   * @param conf - The current HiveConf configuration.
   * @param forUser - The user to configure scheduling for.
   * @throws IOException
   * @throws HiveException
   */
  public static void validateYarnQueue(HiveConf conf, String forUser) throws IOException, HiveException {
    Preconditions.checkState(usingNonImpersonationModeWithFairScheduling(conf),
      "Unable to map job to fair-scheduler because either impersonation is on or fair-scheduling is disabled.");

    ShimLoader.getSchedulerShims().validateQueueConfiguration(conf, forUser);
  }
}