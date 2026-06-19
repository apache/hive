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

package org.apache.hadoop.hive.ql.plan;

import java.util.List;

/*
 * Encapsulates statistics about the duration of all reduce tasks
 * corresponding to a specific JobId.
 * The stats are computed in the HadoopJobExecHelper when the
 * job completes and then populated inside the QueryPlan for
 * each job, from where it can be later on accessed.
 * The reducer statistics consist of minimum/maximum/mean/stdv of the
 * run times of all the reduce tasks for a job. All the Run times are
 * in Milliseconds.
 */
public class ReducerTimeStatsPerJob {

  // Stores the temporal statistics in milliseconds for reducers
  // specific to a Job
  private final long minimumTime;
  private final long maximumTime;
  private final double meanTime;
  private final double standardDeviationTime;


  /*
   * Computes the temporal run time statistics of the reducers
   * for a specific JobId.
   */
  public ReducerTimeStatsPerJob(List<Integer> reducersRunTimes) {

    // If no Run times present, then set -1, indicating no values
    if (!reducersRunTimes.isEmpty()) {
      long minimumTime = reducersRunTimes.get(0);
      long maximumTime = reducersRunTimes.get(0);
      long totalTime = reducersRunTimes.get(0);
      double standardDeviationTime = 0.0;
      double meanTime = 0.0;

      for (int i = 1; i < reducersRunTimes.size(); i++) {
        if (reducersRunTimes.get(i) < minimumTime) {
          minimumTime = reducersRunTimes.get(i);
        }
        if (reducersRunTimes.get(i) > maximumTime) {
          maximumTime = reducersRunTimes.get(i);
        }
        totalTime += reducersRunTimes.get(i);
      }
      meanTime = (double) totalTime / reducersRunTimes.size();

      for (int i = 0; i < reducersRunTimes.size(); i++) {
        standardDeviationTime += Math.pow(meanTime - reducersRunTimes.get(i), 2);
      }
      standardDeviationTime /= reducersRunTimes.size();
      standardDeviationTime = Math.sqrt(standardDeviationTime);

      this.minimumTime = minimumTime;
      this.maximumTime = maximumTime;
      this.meanTime = meanTime;
      this.standardDeviationTime = standardDeviationTime;
      return;
    }
    this.minimumTime = -1;
    this.maximumTime = -1;
    this.meanTime = -1.0;
    this.standardDeviationTime = -1.0;
    return;
  }

  public long getMinimumTime() {
    return this.minimumTime;
  }

  public long getMaximumTime() {
    return this.maximumTime;
  }

  public double getMeanTime() {
    return this.meanTime;
  }

  public double getStandardDeviationTime() {
    return this.standardDeviationTime;
  }
}
