/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.hadoop.hive.ql.queryhistory;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.queryhistory.schema.QueryHistoryRecord;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.shims.Utils;

public class FromSessionStateUpdater {

  private static final FromSessionStateUpdater INSTANCE = new FromSessionStateUpdater();

  public static FromSessionStateUpdater getInstance() {
    return INSTANCE;
  }

  public void consume(SessionState sessionState, QueryHistoryRecord record) {
    record.setSessionId(sessionState.getSessionId());
    record.setEndUser(sessionState.getUserName());
    record.setClientProtocol(sessionState.getConf().getInt(SerDeUtils.LIST_SINK_OUTPUT_PROTOCOL, 0));
    try {
      boolean doAsEnabled = sessionState.getConf().getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS);
      String user = Utils.getUGI().getShortUserName();
      record.setClusterUser(doAsEnabled ? sessionState.getUserName() : user);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    record.setSessionType(sessionState.isHiveServerQuery() ? SessionType.HIVESERVER2.name() : SessionType.OTHER.name());
    record.setCurrentDatabase(sessionState.getCurrentDatabase());
    record.setClientAddress(sessionState.getUserIpAddress());
    record.setConfigurationOptionsChanged(sessionState.getOverriddenConfigurations());
  }

  // Enum to represent the type of session, currently only two types are supported
  // based on sessionState.isHiveServerQuery()
  public enum SessionType {
    HIVESERVER2, OTHER
  }
}
