/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.spark.session;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.spark.SparkClient;
import org.apache.hadoop.hive.ql.plan.SparkWork;

import java.util.UUID;

/**
 * Simple implementation of <i>SparkSession</i> which currently just submits jobs to
 * SparkClient which is shared by all SparkSession instances.
 */
public class SparkSessionImpl implements SparkSession {
  private HiveConf conf;
  private boolean isOpen;
  private final String sessionId;

  public SparkSessionImpl() {
    sessionId = makeSessionId();
  }

  @Override
  public void open(HiveConf conf) {
    this.conf = conf;
    isOpen = true;
  }

  @Override
  public int submit(DriverContext driverContext, SparkWork sparkWork) {
    Preconditions.checkState(isOpen, "Session is not open. Can't submit jobs.");
    return SparkClient.getInstance(driverContext.getCtx().getConf())
        .execute(driverContext, sparkWork);
  }

  @Override
  public boolean isOpen() {
    return isOpen;
  }

  @Override
  public HiveConf getConf() {
    return conf;
  }

  @Override
  public String getSessionId() {
    return sessionId;
  }

  @Override
  public void close() {
    isOpen = false;
  }

  public static String makeSessionId() {
    return UUID.randomUUID().toString();
  }
}
