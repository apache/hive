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

package org.apache.hadoop.hive.ql.hooks;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.DDLTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.HookUtils.InputInfo;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * The implementation of the hook that based on the input size of the query
 * submits big jobs into a fifo pool.
 */
public class FifoPoolHook {

  static final private Log LOG = LogFactory.getLog(FifoPoolHook.class.getName());
  static private boolean fifoed = false;

  static final private String failure = "FifoHook failure: ";

  public static class PreExec implements ExecuteWithHookContext {
    @Override
    public void run(HookContext hookContext) throws Exception {
      assert(hookContext.getHookType() == HookContext.HookType.PRE_EXEC_HOOK);
      SessionState sess = SessionState.get();
      Set<ReadEntity> inputs = hookContext.getInputs();
      Map<String, ContentSummary> inputToCS = hookContext.getInputPathToContentSummary();

      QueryPlan queryPlan = hookContext.getQueryPlan();
      List<Task<? extends Serializable>> rootTasks = queryPlan.getRootTasks();

      // If it is a pure DDL task,
      if (rootTasks == null) {
        return;
      }
      if (rootTasks.size() == 1) {
        Task<? extends Serializable> tsk = rootTasks.get(0);
        if (tsk instanceof DDLTask) {
          return;
        }
      }

      HiveConf conf = sess.getConf();

      // In case posthook of the previous query was not triggered,
      // we revert job tracker to clean state first.
      if (fifoed) {
        conf.set("mapred.fairscheduler.pool", "");
        fifoed = false;
      }
      // if the pool is specified already - bailout
      String poolValue = conf.get("mapred.fairscheduler.pool", null);
      if ((poolValue != null) && !poolValue.isEmpty()){
        return;
      }

      // if we are set on local mode execution (via user or auto) bail
      if ("local".equals(conf.getVar(HiveConf.ConfVars.HADOOPJT))) {
        return;
      }

      // check if we need to run at all
      if (!conf.getBoolean("fbhive.fifopool.auto", false)) {
        return;
      }

      long maxGigaBytes = conf.getLong("fbhive.fifopool.GigaBytes", 0L);
      if (maxGigaBytes == 0) {
        LOG.info (failure + "fifopool.GigaBytes = 0");
        return;
      }

      long maxBytes = maxGigaBytes * 1024 * 1024 * 1024L;

      if (maxGigaBytes < 0) {
        LOG.warn (failure + "fifopool.GigaBytes value of " + maxGigaBytes +
            "is invalid");
        return;
      }

      // Get the size of the input
      Map<String, Double> pathToTopPercentage = new HashMap<String, Double>();
      Set<ReadEntity> nonSampledInputs = new HashSet<ReadEntity>();
      boolean isThereSampling = HookUtils.checkForSamplingTasks(
          hookContext.getQueryPlan().getRootTasks(),
          pathToTopPercentage, nonSampledInputs);

      InputInfo info = HookUtils.getInputInfo(inputs, inputToCS, conf,
          isThereSampling, pathToTopPercentage, nonSampledInputs,
          Long.MAX_VALUE, maxBytes);

      if (info.getSize() > maxBytes) {
          LOG.info ("Submitting to the fifo pool since the input length of " +
                    info.getSize() + " is more than " + maxBytes);
      } else {
        LOG.info("Not submitting to the fifo pool since the input length " +
            info.getSize() + " is less than " + maxBytes);
        return;
      }

      // The job meets at least one of the requirements to be submitted into the
      // fifo pool
      String fifoPool = conf.get("fbhive.fifopool.name", "fifo");
      fifoed = true;
      conf.set("mapred.fairscheduler.pool", fifoPool);
    }
  }

  public static class PostExec implements ExecuteWithHookContext {
    @Override
    public void run(HookContext hookContext) throws Exception {
      assert(hookContext.getHookType() == HookContext.HookType.POST_EXEC_HOOK);
      SessionState ss = SessionState.get();
      this.run(ss);
    }

    public void run(SessionState sess) throws Exception {
      HiveConf conf = sess.getConf();

      if (fifoed) {
        conf.set("mapred.fairscheduler.pool", "");
        fifoed = false;
      }
    }
  }
}
