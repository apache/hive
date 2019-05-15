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


public abstract class BaseProtocol extends RpcDispatcher {

  protected static class CancelJob implements Serializable {

    final String id;

    CancelJob(String id) {
      this.id = id;
    }

    CancelJob() {
      this(null);
    }

    @Override
    public String toString() {
      return "CancelJob{" +
              "id='" + id + '\'' +
              '}';
    }
  }

  protected static class EndSession implements Serializable {

    @Override
    public String toString() {
      return "EndSession";
    }
  }

  protected static class Error implements Serializable {

    final String cause;

    Error(String cause) {
      this.cause = cause;
    }

    Error() {
      this(null);
    }

    @Override
    public String toString() {
      return "Error{" +
              "cause='" + cause + '\'' +
              '}';
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

    @Override
    public String toString() {
      return "JobMetrics{" +
              "jobId='" + jobId + '\'' +
              ", sparkJobId=" + sparkJobId +
              ", stageId=" + stageId +
              ", taskId=" + taskId +
              ", metrics=" + metrics +
              '}';
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

    @Override
    public String toString() {
      return "JobRequest{" +
              "id='" + id + '\'' +
              ", job=" + job +
              '}';
    }
  }

  public static class JobResult<T extends Serializable> implements Serializable {

    final String id;
    final T result;
    final Throwable error;
    final SparkCounters sparkCounters;

    JobResult(String id, T result, Throwable error, SparkCounters sparkCounters) {
      this.id = id;
      this.result = result;
      this.error = error;
      this.sparkCounters = sparkCounters;
    }

    JobResult() {
      this(null, null, null, null);
    }

    @Override
    public String toString() {
      return "JobResult{" +
              "id='" + id + '\'' +
              ", result=" + result +
              ", error=" + error +
              ", sparkCounters=" + sparkCounters +
              '}';
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

    @Override
    public String toString() {
      return "JobStarted{" +
              "id='" + id + '\'' +
              '}';
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

    @Override
    public String toString() {
      return "JobSubmitted{" +
              "clientJobId='" + clientJobId + '\'' +
              ", sparkJobId=" + sparkJobId +
              '}';
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

    @Override
    public String toString() {
      return "SyncJobRequest{" +
              "job=" + job +
              '}';
    }
  }
}
