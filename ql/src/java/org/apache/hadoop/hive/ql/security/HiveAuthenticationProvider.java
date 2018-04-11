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

package org.apache.hadoop.hive.ql.security;

import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * HiveAuthenticationProvider is an interface for authentication. The
 * implementation should return userNames and groupNames.
 */
public interface HiveAuthenticationProvider extends Configurable{

  public String getUserName();

  public List<String> getGroupNames();

  public void destroy() throws HiveException;

  /**
   * This function is meant to be used only for hive internal implementations of this interface.
   * SessionState is not a public interface.
   * @param ss SessionState that created this instance
   */
  public void setSessionState(SessionState ss);

}
