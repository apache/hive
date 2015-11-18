/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.spark.client;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hive.spark.counter.SparkCounters;

import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Holds runtime information about the job execution context.
 *
 * An instance of this class is kept on the node hosting a remote Spark context and is made
 * available to jobs being executed via RemoteSparkContext#submit().
 */
@InterfaceAudience.Private
public interface JobContext {

  /** The shared SparkContext instance. */
  JavaSparkContext sc();

  /**
   * Monitor a job. This allows job-related information (such as metrics) to be communicated
   * back to the client.
   *
   * @return The job (unmodified).
   */
  <T> JavaFutureAction<T> monitor(
    JavaFutureAction<T> job, SparkCounters sparkCounters, Set<Integer> cachedRDDIds);

  /**
   * Return a map from client job Id to corresponding JavaFutureActions.
   */
  Map<String, List<JavaFutureAction<?>>> getMonitoredJobs();

  /**
   * Return all added jar path and timestamp which added through AddJarJob.
   */
  Map<String, Long> getAddedJars();

  /**
   * Returns a local tmp dir specific to the context
   */
  File getLocalTmpDir();

}
