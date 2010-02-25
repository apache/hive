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

package org.apache.hadoop.hive.ql.exec.errors;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Detects when a query has failed because a user's script that was specified in
 * transform returns a non-zero error code.
 *
 * Conditions to check:
 *
 * 1. "Script failed with code <some number>" is in the log
 *
 */

public class ScriptErrorHeuristic extends RegexErrorHeuristic {

  private static final String FAILED_REGEX = "Script failed with code [0-9]+";

  public ScriptErrorHeuristic() {
    setQueryRegex(".*");
    getLogRegexes().add(FAILED_REGEX);
  }

  @Override
  public ErrorAndSolution getErrorAndSolution() {
    ErrorAndSolution es = null;

    if(getQueryMatches()) {
      for(List<String> matchingLines : getRegexToLogLines().values()) {
        // There should really only be one line with "Script failed..."
        if (matchingLines.size() > 0) {
          assert(matchingLines.size() == 1);

          // Get "Script failed with code <some number>"
          Matcher m1 = Pattern.compile(FAILED_REGEX).matcher(matchingLines.get(0));
          m1.find();
          String failedStr = m1.group();

          // Get "<some number>"
          Matcher m2 = Pattern.compile("[0-9]+").matcher(failedStr);
          m2.find();
          String errorCode = m2.group();

          es = new ErrorAndSolution(
            "A user-supplied transfrom script has exited with error code " +
            errorCode + " instead of 0.",
            "Verify that the script can properly handle all the input rows " +
            "without throwing exceptions and exits properly.");
        }
      }
    }

    reset();
    return es;
  }
}
