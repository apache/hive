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

/**
 *
 */
package org.apache.hadoop.hive.ql.hooks;

import java.util.ArrayList;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.conf.FBHiveConf;

/**
 * Seperating out some functionality for so that Hive1 can share code.
 */
public class BaseReplicationHook {
  static final private Log LOG = LogFactory.getLog("hive.ql.hooks.BaseReplicationHook");

  protected ConnectionUrlFactory urlFactory = null;
  HiveConf conf = null;

  public static ConnectionUrlFactory getReplicationMySqlUrl() {
    HiveConf conf = new HiveConf(BaseReplicationHook.class);
    return HookUtils.getUrlFactory(conf,
        FBHiveConf.CONNECTION_FACTORY,
        FBHiveConf.REPLICATION_CONNECTION_FACTORY,
        FBHiveConf.REPLICATION_MYSQL_TIER_VAR_NAME,
        FBHiveConf.REPLICATION_HOST_DATABASE_VAR_NAME);
  }

  public BaseReplicationHook() throws Exception {
    urlFactory = getReplicationMySqlUrl();
    conf = new HiveConf(BaseReplicationHook.class);
  }

  /**
   * Simplified call used by hive1 to insert into the audit log
   *
   * @param command
   * @param commandType
   * @param inputs
   * @param outputs
   * @param userInfo
   * @throws Exception
   */
  public void run(String command, String commandType, String inputs,
      String outputs, String userInfo) throws Exception {
    ArrayList<Object> sqlParams = new ArrayList<Object>();
    sqlParams.add(StringEscapeUtils.escapeJava(commandType));
    sqlParams.add(StringEscapeUtils.escapeJava(inputs));
    sqlParams.add(outputs);
    sqlParams.add(StringEscapeUtils.escapeJava(userInfo));

    String sql = "insert into snc1_command_log set command_type = ?, " +
      "inputs = ?, outputs = ?, user_info = ?";
    if (conf == null) {
      conf = new HiveConf(BaseReplicationHook.class);
    }
    HookUtils.runInsert(conf, urlFactory, sql, sqlParams);
  }
}
