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

package org.apache.hive.service.cli.session;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.operation.OperationManager;
import org.apache.hive.service.cli.thrift.TProtocolVersion;

/**
 * Methods that don't need to be executed under a doAs
 * context are here. Rest of them in HiveSession interface
 */
public interface HiveSessionBase {

  TProtocolVersion getProtocolVersion();

  /**
   * Set the session manager for the session
   * @param sessionManager
   */
  public void setSessionManager(SessionManager sessionManager);

  /**
   * Get the session manager for the session
   */
  public SessionManager getSessionManager();

  /**
   * Set operation manager for the session
   * @param operationManager
   */
  public void setOperationManager(OperationManager operationManager);

  public SessionHandle getSessionHandle();

  public String getUsername();

  public String getPassword();

  public HiveConf getHiveConf();

  public SessionState getSessionState();

  public String getUserName();

  public void setUserName(String userName);

  public String getIpAddress();

  public void setIpAddress(String ipAddress);
}
