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
package org.apache.hadoop.hive.ql.hooks;

import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;

/**
 *
 * VerifySessionStateStackTracesHook.
 *
 * Writes the first line of each stack trace collected to the console, to either verify stack
 * traces were or were not collected.
 */
public class VerifySessionStateStackTracesHook implements ExecuteWithHookContext {

  public void run(HookContext hookContext) {
    LogHelper console = SessionState.getConsole();

    for (Entry<String, List<List<String>>> entry :
        SessionState.get().getStackTraces().entrySet()) {

      for (List<String> stackTrace : entry.getValue()) {
        // Only print the first line of the stack trace as it contains the error message, and other
        // lines may contain line numbers which are volatile
        // Also only take the string after the first two spaces, because the prefix is a date and
        // and time stamp
        console.printError(StringUtils.substringAfter(
            StringUtils.substringAfter(stackTrace.get(0), " "), " "));
      }
    }
  }
}
