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

package org.apache.hadoop.hive.ql.hooks;

import java.util.ArrayList;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.conf.FBHiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * This hook records the approx. start and finish times of queries into a table
 * in MySQL (query_time_log). Useful for debugging. Possibly for performance
 * measurement.
 *
 * - Relies on query_id to update rows with the finish time.
 * - Old entries in this table should be cleaned out on a regular basis.
 */

/*
Example table schema:

CREATE TABLE `query_time_log` (
    `query_id` varchar(512) DEFAULT NULL,
    `start_time` timestamp NULL DEFAULT NULL,
    `finish_time` timestamp NULL DEFAULT NULL,
    `query` mediumtext,
    `query_type` varchar(32) DEFAULT NULL,
    `inputs` mediumtext,
    `outputs` mediumtext,
    `user_info` varchar(512) DEFAULT NULL,
    PRIMARY KEY (`query_id`),
    INDEX(start_time),
    INDEX(finish_time),
    INDEX(inputs(256)),
    INDEX(outputs(256))
  ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
*/

public class StartFinishHook implements PreExecute, PostExecute {



  ConnectionUrlFactory urlFactory = null;

  public StartFinishHook() throws Exception {
    HiveConf conf = new HiveConf(StartFinishHook.class);


    urlFactory = HookUtils.getUrlFactory(conf,
        FBHiveConf.CONNECTION_FACTORY,
        FBHiveConf.STARTFINISH_CONNECTION_FACTORY,
        FBHiveConf.STARTFINISH_MYSQL_TIER_VAR_NAME,
        FBHiveConf.STARTFINISH_HOST_DATABASE_VAR_NAME);
  }

  /**
   * Returns a list of strings with following values extracted from the state:
   * command, commandType, inputStr, outputStr, queryId, userInfo
   *
   * @param sess
   * @param inputs
   * @param outputs
   * @param ugi
   * @return
   */
  private static ArrayList<Object> extractValues(SessionState sess,
      Set<ReadEntity> inputs, Set<WriteEntity> outputs, UserGroupInformation ugi) {
    String command     = sess.getCmd();
    String commandType = sess.getCommandType();
    String userInfo = "";
    if (ugi != null) {
      userInfo = ugi.getUserName();
    }
    String inputStr = "";

    if (inputs != null) {
      StringBuilder inputsSB = new StringBuilder();

      boolean first = true;

      for (ReadEntity inp : inputs) {
        if (!first) {
          inputsSB.append(",");
        }
        first = false;
        inputsSB.append(inp.toString());
      }
      inputStr = inputsSB.toString();
    }

    String outputStr = "";

    if (outputs != null) {
      StringBuilder outputsSB = new StringBuilder();

      boolean first = true;

      for (WriteEntity o : outputs) {
        if (!first) {
          outputsSB.append(",");
        }
        first = false;
        outputsSB.append(o.toString());
      }
      outputStr = outputsSB.toString();
    }

    String queryId = getQueryId(sess);

    ArrayList<Object> values = new ArrayList<Object>();
    values.add(command);
    values.add(commandType);
    values.add(inputStr);
    values.add(outputStr);
    values.add(queryId);
    values.add(userInfo);

    return values;
  }

  private static String getQueryId(SessionState sess) {
    HiveConf conf = sess.getConf();
    String queryId = conf.getVar(HiveConf.ConfVars.HIVEQUERYID);
    return queryId;

  }

  /**
   * For PreExecute
   */
  @Override
  public void run(SessionState sess, Set<ReadEntity> inputs,
      Set<WriteEntity> outputs, UserGroupInformation ugi) throws Exception {
    ArrayList<Object> values = StartFinishHook.extractValues(sess,
        inputs, outputs, ugi);
    String query = "INSERT INTO query_time_log SET " +
      "query = ?, " +
      "query_type = ?, " +
      "inputs = ?, " +
      "outputs = ?, " +
      "query_id = ?, " +
      "user_info = ?, " +
      "start_time = now()";

    HiveConf conf = sess.getConf();
    // pre-hook doesn't need to retry many times and can fail faster.
    HookUtils.runInsert(conf, urlFactory, query, values, 5);
  }

  /**
   * For PostExecute
   */
  @Override
  public void run(SessionState sess, Set<ReadEntity> inputs,
      Set<WriteEntity> outputs, LineageInfo info, UserGroupInformation ugi)
  throws Exception {
    ArrayList<Object> values = StartFinishHook.extractValues(sess,
        inputs, outputs, ugi);
    // Duplicate values for update statement
    values.addAll(values);
    // The ON DUPLICATE.. ensures that start_time is preserved for normal cases
    // where start_time was recorded
    String valueString =
      "query = ?, " +
      "query_type = ?, " +
      "inputs = ?, " +
      "outputs = ?, " +
      "query_id = ?, " +
      "user_info = ?, " +
      "finish_time = now()";
    String query = "INSERT INTO query_time_log SET " + valueString +
      " ON DUPLICATE KEY UPDATE " + valueString ;

    HiveConf conf = sess.getConf();
    HookUtils.runInsert(conf, urlFactory, query, values, HookUtils
        .getSqlNumRetry(conf));
  }

}
