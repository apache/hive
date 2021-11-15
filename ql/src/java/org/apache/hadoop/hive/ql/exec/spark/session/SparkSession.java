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
package org.apache.hadoop.hive.ql.exec.spark.session;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.TaskQueue;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobRef;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.SparkWork;

import java.io.IOException;

public interface SparkSession {

  /**
   * Initializes a Spark session for DAG execution.
   * @param conf Hive configuration.
   */
  void open(HiveConf conf) throws HiveException;

  /**
   * Submit given <i>sparkWork</i> to SparkClient.
   * @param taskQueue
   * @param sparkWork
   * @return SparkJobRef
   */
  SparkJobRef submit(TaskQueue taskQueue, Context context, SparkWork sparkWork) throws Exception;

  /**
   * Get Spark shuffle memory per task, and total number of cores. This
   * information can be used to estimate how many reducers a task can have.
   *
   * @return an object pair, the first element is the shuffle memory per task in bytes,
   *  the second element is the number of total cores usable by the client
   */
  Pair<Long, Integer> getMemoryAndCores() throws Exception;

  /**
   * @return true if the session is open and ready to submit jobs.
   */
  boolean isOpen();

  /**
   * @return configuration.
   */
  HiveConf getConf();

  /**
   * @return session id.
   */
  String getSessionId();

  /**
   * Close session and release resources.
   */
  void close();

  /**
   * Get an HDFS dir specific to the SparkSession
   * */
  Path getHDFSSessionDir() throws IOException;

  /**
   * Callback function that is invoked by the {@link org.apache.hadoop.hive.ql.Driver} when a
   * query has completed.
   *
   * @param queryId the id of the query that completed
   */
  void onQueryCompletion(String queryId);

  /**
   * Callback function that is invoked by the {@link org.apache.hadoop.hive.ql.Driver} when a
   * query has been submitted.
   *
   * @param queryId the id of the query that completed
   */
  void onQuerySubmission(String queryId);

  /**
   * Checks if a session has timed out, and closes if the session if the timeout has occurred;
   * returns true if the session timed out, and false otherwise.
   *
   * @param sessionTimeout the session timeout
   *
   * @return true if the session timed out and was closed, false otherwise
   */
  boolean triggerTimeout(long sessionTimeout);
}
