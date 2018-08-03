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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Authenticator that returns the userName set in SessionState. For use when authorizing with HS2
 * so that HS2 can set the user for the session through SessionState
 */
public class SessionStateUserAuthenticator implements HiveAuthenticationProvider {

  private final List<String> groupNames = new ArrayList<String>();

  protected Configuration conf;
  private SessionState sessionState;
  private List<String> groups;

  @Override
  public List<String> getGroupNames() {
    if (groups == null) {
      groups = UserGroupInformation.createRemoteUser(sessionState.getUserName()).getGroups();
    }
    return groups;
  }

  @Override
  public String getUserName() {
    return sessionState.getUserName();
  }

  @Override
  public void destroy() throws HiveException {
    return;
  }

  @Override
  public Configuration getConf() {
    return null;
  }

  @Override
  public void setConf(Configuration arg0) {
  }

  @Override
  public void setSessionState(SessionState sessionState) {
    this.sessionState = sessionState;
  }

}
