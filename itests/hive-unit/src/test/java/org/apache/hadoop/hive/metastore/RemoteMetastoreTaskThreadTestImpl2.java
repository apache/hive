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
package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * An implementation of MetastoreTaskThread for testing metastore leader config.
 */
public class RemoteMetastoreTaskThreadTestImpl2 implements MetastoreTaskThread {
  static final String TASK_NAME = "metastore_task_thread_test_impl_2";
  public static final Logger LOG = LoggerFactory.getLogger(RemoteMetastoreTaskThreadTestImpl2.class);
  private Configuration conf;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public long runFrequency(TimeUnit unit) {
    return conf.getTimeDuration(MetastoreHousekeepingLeaderTestBase.METASTORE_THREAD_TASK_FREQ_CONF,
            0, unit);
  }

  @Override
  public void run() {
    LOG.info("Name of thread " + Thread.currentThread().getName() + " changed to " + TASK_NAME);
    Thread.currentThread().setName(TASK_NAME);
    try {
      Thread.sleep(10000);
    } catch (InterruptedException ie) {
      LOG.error("Task " + TASK_NAME + " interrupted: " + ie.getMessage(), ie);
    }
  }
}
