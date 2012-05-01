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

import java.util.Map;
import java.util.Set;
import java.util.Random;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.DDLTask;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Implementation of a pre execute hook that prints out a suggestion for users
 * to use TABLESAMPLE when inputs are large.
 */
public class SuggestionPrintingHook implements ExecuteWithHookContext {

  static final private Log LOG = LogFactory.getLog(SuggestionPrintingHook.class
      .getName());

  static private int timesReported = 0;

  @Override
  public void run(HookContext hookContext) throws Exception {
    assert (hookContext.getHookType() == HookContext.HookType.PRE_EXEC_HOOK);
    SessionState sess = SessionState.get();
    if (sess.getIsSilent()) {
      return;
    }
    SessionState.LogHelper console = new SessionState.LogHelper(LOG);

    QueryPlan queryPlan = hookContext.getQueryPlan();
    ArrayList<Task<? extends Serializable>> rootTasks = queryPlan.getRootTasks();

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

    // do some simple query matching to not to show the suggestion for some
    // queries.
    String command = SessionState.get().getCmd().toUpperCase().replace('\n',
        ' ').replace('\t', ' ');
    if ((timesReported > 0 && HookUtils.rollDice(0.9f)) ||
        !command.contains("SELECT ") || command.contains(" TABLESAMPLE")
        || command.contains(" JOIN ") || command.contains(" LIMIT ")) {
      return;
    }

    Set<ReadEntity> inputs = hookContext.getInputs();
    Map<String, ContentSummary> inputToCS = hookContext
        .getInputPathToContentSummary();

    HiveConf conf = sess.getConf();

    int maxGigaBytes = conf.getInt("fbhive.suggest.tablesample.gigabytes", 32);

    long maxBytes = maxGigaBytes * 1024 * 1024 * 1024L;

    if (maxGigaBytes < 0) {
      console.printError("maxGigaBytes value of " + maxGigaBytes
              + "is invalid");
      return;
    }

    long inputSize = HookUtils.getInputSize(inputs, inputToCS, conf);

    if (inputSize > maxBytes) {
      console.printInfo("");
      console
          .printInfo("***  This queries over "
              + Math.round(maxBytes / 1024D / 1024D / 1024D)
              + " GB data. Consider TABLESAMPLE: fburl.com/?key=2001210");
      console.printInfo("");
      timesReported++;
    }
  }
}
