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

import java.io.Serializable;
import java.net.URL;
import java.util.concurrent.Future;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;

/**
 * Defines the API for the Spark remote client.
 */
@InterfaceAudience.Private
public interface SparkClient extends Serializable {

  /**
   * Submits a job for asynchronous execution.
   *
   * @param job The job to execute.
   * @return A handle that be used to monitor the job.
   */
  <T extends Serializable> JobHandle<T> submit(Job<T> job);

  /**
   * Stops the remote context.
   *
   * Any pending jobs will be cancelled, and the remote context will be torn down.
   */
  void stop();

  /**
   * Adds a jar file to the running remote context.
   *
   * Note that the URL should be reachable by the Spark driver process. If running the driver
   * in cluster mode, it may reside on a different host, meaning "file:" URLs have to exist
   * on that node (and not on the client machine).
   *
   * @param url The location of the jar file.
   * @return A future that can be used to monitor the operation.
   */
  Future<?> addJar(URL url);

  /**
   * Adds a file to the running remote context.
   *
   * Note that the URL should be reachable by the Spark driver process. If running the driver
   * in cluster mode, it may reside on a different host, meaning "file:" URLs have to exist
   * on that node (and not on the client machine).
   *
   * @param url The location of the file.
   * @return A future that can be used to monitor the operation.
   */
  Future<?> addFile(URL url);

  /**
   * Get the count of executors.
   */
  Future<Integer> getExecutorCount();

  /**
   * Get default parallelism. For standalone mode, this can be used to get total number of cores.
   */
  Future<Integer> getDefaultParallelism();
}
