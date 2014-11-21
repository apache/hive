/**
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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobRef;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.SparkWork;

public interface SparkSession {
  /**
   * Initializes a Spark session for DAG execution.
   */
  public void open(HiveConf conf) throws HiveException;

  /**
   * Submit given <i>sparkWork</i> to SparkClient
   * @param driverContext
   * @param sparkWork
   */
  public SparkJobRef submit(DriverContext driverContext, SparkWork sparkWork) throws Exception;

  /**
   * Is the session open and ready to submit jobs?
   */
  public boolean isOpen();

  /**
   * Return configuration.
   */
  public HiveConf getConf();

  /**
   * Return session id.
   */
  public String getSessionId();

  /**
   * Close session and release resources
   */
  public void close();
}
