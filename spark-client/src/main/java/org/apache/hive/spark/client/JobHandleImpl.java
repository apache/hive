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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.util.concurrent.Promise;

import org.apache.hive.spark.counter.SparkCounters;

/**
 * A handle to a submitted job. Allows for monitoring and controlling of the running remote job.
 */
class JobHandleImpl<T extends Serializable> implements JobHandle<T> {

  private final AtomicBoolean cancelled;
  private final SparkClientImpl client;
  private final String jobId;
  private final MetricsCollection metrics;
  private final Promise<T> promise;
  private final List<Integer> sparkJobIds;
  private volatile SparkCounters sparkCounters;

  JobHandleImpl(SparkClientImpl client, Promise<T> promise, String jobId) {
    this.cancelled = new AtomicBoolean();
    this.client = client;
    this.jobId = jobId;
    this.promise = promise;
    this.metrics = new MetricsCollection();
    this.sparkJobIds = new CopyOnWriteArrayList<Integer>();
    this.sparkCounters = null;
  }

  /** Requests a running job to be cancelled. */
  @Override
  public boolean cancel(boolean mayInterrupt) {
    if (cancelled.compareAndSet(false, true)) {
      client.cancel(jobId);
      promise.cancel(mayInterrupt);
      return true;
    }
    return false;
  }

  @Override
  public T get() throws ExecutionException, InterruptedException {
    return promise.get();
  }

  @Override
  public T get(long timeout, TimeUnit unit)
      throws ExecutionException, InterruptedException, TimeoutException {
    return promise.get(timeout, unit);
  }

  @Override
  public boolean isCancelled() {
    return promise.isCancelled();
  }

  @Override
  public boolean isDone() {
    return promise.isDone();
  }

  /**
   * The client job ID. This is unrelated to any Spark jobs that might be triggered by the
   * submitted job.
   */
  @Override
  public String getClientJobId() {
    return jobId;
  }

  /**
   * A collection of metrics collected from the Spark jobs triggered by this job.
   *
   * To collect job metrics on the client, Spark jobs must be registered with JobContext::monitor()
   * on the remote end.
   */
  @Override
  public MetricsCollection getMetrics() {
    return metrics;
  }

  @Override
  public List<Integer> getSparkJobIds() {
    return sparkJobIds;
  }

  @Override
  public SparkCounters getSparkCounters() {
    return sparkCounters;
  }

  public void setSparkCounters(SparkCounters sparkCounters) {
    this.sparkCounters = sparkCounters;
  }

  @SuppressWarnings("unchecked")
  void setSuccess(Object result) {
    promise.setSuccess((T) result);
  }

  void setFailure(Throwable error) {
    promise.setFailure(error);
  }

  /** Last attempt resort at preventing stray jobs from accumulating in SparkClientImpl. */
  @Override
  protected void finalize() {
    if (!isDone()) {
      cancel(true);
    }
  }

}
