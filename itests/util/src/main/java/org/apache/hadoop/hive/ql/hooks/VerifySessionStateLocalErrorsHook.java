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

import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;

/**
 *
 * VerifySessionStateLocalErrorsHook.
 *
 * This hook is intended for testing that the localMapRedErrors variable in SessionState is
 * populated when a local map reduce job fails.  It prints the ID of the stage that failed, and
 * the lines recorded in the variable.
 */
public class VerifySessionStateLocalErrorsHook implements ExecuteWithHookContext {

  public void run(HookContext hookContext) {
    LogHelper console = SessionState.getConsole();

    for (Entry<String, List<String>> entry : SessionState.get().getLocalMapRedErrors().entrySet()) {
      console.printError("ID: " + entry.getKey());

      for (String line : entry.getValue()) {
        console.printError(line);
      }
    }
  }
}
