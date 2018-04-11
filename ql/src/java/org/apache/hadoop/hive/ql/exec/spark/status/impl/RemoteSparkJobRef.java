/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.spark.status.impl;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.spark.status.RemoteSparkJobMonitor;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobRef;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobStatus;
import org.apache.hive.spark.client.JobHandle;

import java.io.Serializable;

public class RemoteSparkJobRef implements SparkJobRef {

  private final String jobId;
  private final HiveConf hiveConf;
  private final RemoteSparkJobStatus sparkJobStatus;
  private final JobHandle<Serializable> jobHandler;

  public RemoteSparkJobRef(HiveConf hiveConf, JobHandle<Serializable> jobHandler, RemoteSparkJobStatus sparkJobStatus) {
    this.jobHandler = jobHandler;
    this.jobId = jobHandler.getClientJobId();
    this.hiveConf = hiveConf;
    this.sparkJobStatus = sparkJobStatus;
  }

  @Override
  public String getJobId() {
    return jobId;
  }

  @Override
  public SparkJobStatus getSparkJobStatus() {
    return sparkJobStatus;
  }

  @Override
  public boolean cancelJob() {
    return jobHandler.cancel(true);
  }

  @Override
  public int monitorJob() {
    RemoteSparkJobMonitor remoteSparkJobMonitor = new RemoteSparkJobMonitor(hiveConf, sparkJobStatus);
    return remoteSparkJobMonitor.startMonitor();
  }
}
