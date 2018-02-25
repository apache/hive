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

package org.apache.hadoop.hive.ql.exec.errors;

import org.apache.hadoop.mapred.JobConf;

/**
 * Classes implementing ErrorHeuristic are able to generate a possible cause and
 * solution for Hive jobs that have failed by examining the query, task log
 * files, and the job configuration.
 *
 * A class implementing ErrorHeuristic should only detect one type of error.
 *
 */
public interface ErrorHeuristic {

  /**
   * Initialize this error heuristic. Must be called before any other methods
   * are called
   * @param query
   * @param jobConf
   */
  void init(String query, JobConf jobConf);

  /**
   * Process the given log line. It should be called for every line in the task
   * log file, in sequence.
   *
   * @param line
   */
  void processLogLine(String line);

  /**
   * Examine the hive query, job configuration, and the lines from the task log
   * seen so far though processLogLine() and generate a possible cause/solution.
   * Once this method is called, the implementing class should be reset to the
   * state before any processLogLine() calls were made.
   *
   * @return a matching error, or null if a suitable match wasn't found.
   *
   */
  ErrorAndSolution getErrorAndSolution();
}
