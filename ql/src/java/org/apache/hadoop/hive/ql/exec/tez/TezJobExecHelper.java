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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.tez;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * TezJobExecHelper is a utility to safely call Tez functionality from
 * common code paths. It will check if tez is available/installed before
 * invoking the code.
 */
public class TezJobExecHelper {

  private static final Logger LOG = LoggerFactory.getLogger(TezJobExecHelper.class.getName());

  private static final Method KILL_RUNNING_TEZ_JOBS;

  static {
    Method method = null;
    try {
      Class.forName("org.apache.tez.dag.api.DAG");

      // we have tez installed
      ClassLoader classLoader = TezJobExecHelper.class.getClassLoader();

      method = classLoader
          .loadClass("org.apache.hadoop.hive.ql.exec.tez.monitoring.TezJobMonitor")
          .getDeclaredMethod("killRunningJobs");
      method.setAccessible(true);
    } catch (Exception e) {
      LOG.error("Error getting tez method", e);
    }
    KILL_RUNNING_TEZ_JOBS = method;
  }

  public static void killRunningJobs() {
    try {
      if (KILL_RUNNING_TEZ_JOBS != null) {
        KILL_RUNNING_TEZ_JOBS.invoke(null, null);
      } else {
        LOG.warn("Unable to find tez method for killing jobs");
      }
    } catch (Exception e) {
      // It is not available do nothing
      LOG.error("Could not stop tez dags: ", e);
    }
  }
}
