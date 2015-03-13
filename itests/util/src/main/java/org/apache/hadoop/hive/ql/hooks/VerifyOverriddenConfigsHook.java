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

import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;

/**
 *
 * VerifyOverriddenConfigsHook.
 *
 * This hook is meant to be used for testing.  It prints the keys and values of config variables
 * which have been noted by the session state as changed by the user.  It only prints specific
 * variables as others may change and that should not affect this test.
 */
public class VerifyOverriddenConfigsHook implements ExecuteWithHookContext {

  // A config variable set via a System Property, a config variable set in the CLI,
  // a config variable not in the default List of config variables, and a config variable in the
  // default list of config variables, but which has not been overridden
  private static String[] keysArray =
    {"mapred.job.tracker", "hive.exec.post.hooks", "some.hive.config.doesnt.exit",
     "hive.exec.mode.local.auto"};
  private static List<String> keysList = Arrays.asList(keysArray);

  public void run(HookContext hookContext) {
    LogHelper console = SessionState.getConsole();
    SessionState ss = SessionState.get();

    if (console == null || ss == null) {
      return;
    }

    for (Entry<String, String> entry : ss.getOverriddenConfigurations().entrySet()) {
      if (keysList.contains(entry.getKey())) {
        console.printError("Key: " + entry.getKey() + ", Value: " + entry.getValue());
      }
    }
  }
}
