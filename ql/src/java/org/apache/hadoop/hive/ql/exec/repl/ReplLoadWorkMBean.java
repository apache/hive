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
package org.apache.hadoop.hive.ql.exec.repl;

public interface ReplLoadWorkMBean {

  /**
   * Gets the name of the database which is being loaded.
   * @return the source database name.
   */
  public String getSourceDatabase();

  /**
   * Gets the name of the database on which load is happening.
   * @return the target database name.
   */
  public String getTargetDatabase();

  /**
   * Gets the Replication Type.
   * @return INCREMENTAL or BOOTSTRAP load.
   */
  public String getReplicationType();

  /**
   * Gets the name of scheduled query being run.
   * @return the name of the scheduled query.
   */
  public String getScheduledQueryName();

  /**
   * Gets the execution id of the policy.
   *
   * @return the execution id.
   */
  public Long getExecutionId();

  /**
   * Gets the dump directory.
   * @return the dump directory.
   */
  public String getDumpDirectory();

  /**
   * Gets the event id that just got processed.
   * @return last event id.
   */
  public String getCurrentEventId();

  /**
   * Gets the last event id for the load.
   * @return the last event id
   */
  public Long getLastEventId();

  /**
   * Gets the string representation of replication statistics
   * @return the replication stats.
   */
  public String getReplStats();

}
