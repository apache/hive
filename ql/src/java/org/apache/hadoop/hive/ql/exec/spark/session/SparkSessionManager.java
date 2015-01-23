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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Defines interface for managing multiple SparkSessions in Hive when multiple users
 * are executing queries simultaneously on Spark execution engine.
 */
public interface SparkSessionManager {
  /**
   * Initialize based on given configuration.
   *
   * @param hiveConf
   */
  void setup(HiveConf hiveConf) throws HiveException;

  /**
   * Get a valid SparkSession. First try to check if existing session is reusable
   * based on the given <i>conf</i>. If not release <i>existingSession</i> and return
   * a new session based on session manager criteria and <i>conf</i>.
   *
   * @param existingSession Existing session (can be null)
   * @param conf
   * @param doOpen Should the session be opened before returning?
   * @return SparkSession
   */
  SparkSession getSession(SparkSession existingSession, HiveConf conf,
      boolean doOpen) throws HiveException;

  /**
   * Return the given <i>sparkSession</i> to pool. This is used when the client
   * still holds references to session and may want to reuse it in future.
   * When client wants to reuse the session, it should pass the it <i>getSession</i> method.
   */
  void returnSession(SparkSession sparkSession) throws HiveException;

  /**
   * Close the given session and return it to pool. This is used when the client
   * no longer needs a SparkSession.
   */
  void closeSession(SparkSession sparkSession) throws HiveException;

  /**
   * Shutdown the session manager. Also closing up SparkSessions in pool.
   */
  void shutdown();
}
