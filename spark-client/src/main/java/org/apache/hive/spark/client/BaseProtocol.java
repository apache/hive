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

import org.apache.hive.spark.client.metrics.Metrics;
import org.apache.hive.spark.client.rpc.RpcDispatcher;
import org.apache.hive.spark.counter.SparkCounters;

import com.google.common.base.Throwables;

abstract class BaseProtocol extends RpcDispatcher {

  protected static class CancelJob implements Serializable {

    final String id;

    CancelJob(String id) {
      this.id = id;
    }

    CancelJob() {
      this(null);
    }

  }

  protected static class EndSession implements Serializable {

  }

  protected static class Error implements Serializable {

    final String cause;

    Error(Throwable cause) {
      if (cause == null) {
        this.cause = "";
      } else {
        this.cause = Throwables.getStackTraceAsString(cause);
      }
    }

    Error() {
      this(null);
    }

  }

  protected static class JobMetrics implements Serializable {

    final String jobId;
    final int sparkJobId;
    final int stageId;
    final long taskId;
    final Metrics metrics;

    JobMetrics(String jobId, int sparkJobId, int stageId, long taskId, Metrics metrics) {
      this.jobId = jobId;
      this.sparkJobId = sparkJobId;
      this.stageId = stageId;
      this.taskId = taskId;
      this.metrics = metrics;
    }

    JobMetrics() {
      this(null, -1, -1, -1, null);
    }

  }

  protected static class JobRequest<T extends Serializable> implements Serializable {

    final String id;
    final Job<T> job;

    JobRequest(String id, Job<T> job) {
      this.id = id;
      this.job = job;
    }

    JobRequest() {
      this(null, null);
    }

  }

  protected static class JobResult<T extends Serializable> implements Serializable {

    final String id;
    final T result;
    final String error;
    final SparkCounters sparkCounters;

    JobResult(String id, T result, Throwable error, SparkCounters sparkCounters) {
      this.id = id;
      this.result = result;
      this.error = error != null ? Throwables.getStackTraceAsString(error) : null;
      this.sparkCounters = sparkCounters;
    }

    JobResult() {
      this(null, null, null, null);
    }

  }

  protected static class JobStarted implements Serializable {

    final String id;

    JobStarted(String id) {
      this.id = id;
    }

    JobStarted() {
      this(null);
    }

  }

  /**
   * Inform the client that a new spark job has been submitted for the client job.
   */
  protected static class JobSubmitted implements Serializable {
    final String clientJobId;
    final int sparkJobId;

    JobSubmitted(String clientJobId, int sparkJobId) {
      this.clientJobId = clientJobId;
      this.sparkJobId = sparkJobId;
    }

    JobSubmitted() {
      this(null, -1);
    }
  }

  protected static class SyncJobRequest<T extends Serializable> implements Serializable {

    final Job<T> job;

    SyncJobRequest(Job<T> job) {
      this.job = job;
    }

    SyncJobRequest() {
      this(null);
    }

  }
}
