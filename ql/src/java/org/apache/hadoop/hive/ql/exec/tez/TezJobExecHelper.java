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

package org.apache.hadoop.hive.ql.exec.tez;

import java.lang.reflect.Method;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * TezJobExecHelper is a utility to safely call Tez functionality from
 * common code paths. It will check if tez is available/installed before
 * invoking the code.
 */
public class TezJobExecHelper {

  private static final Log LOG = LogFactory.getLog(TezJobExecHelper.class.getName());

  public static void killRunningJobs() {
    try {
      Class.forName("org.apache.tez.dag.api.DAG");

      // we have tez installed
      ClassLoader classLoader = TezJobExecHelper.class.getClassLoader();
      Method method = classLoader.loadClass("org.apache.hadoop.hive.ql.exec.tez.TezJobMonitor")
        .getMethod("killRunningJobs");
      method.invoke(null, null);
    }
    catch (Exception e) {
      // It is not available do nothing
      LOG.debug("Could not stop tez dags: ", e);
    }
  }
}
