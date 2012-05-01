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

import java.util.Set;
import java.util.ArrayList;
import java.sql.Connection;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.hooks.PostExecute;
import org.apache.hadoop.hive.ql.hooks.LineageInfo;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Implementation of a post execute hook that prints out some more information
 * to console to allow regression tests to check correctness.
 */
public class RegressionTestHook implements PostExecute {
  static final private Log LOG = LogFactory
      .getLog("hive.ql.hooks.RegressionTestHook");

  final static String REGRESSION_TEST_PRINT_SWITCH_VAR_NAME = "fbhive.regressiontesthook.swtich";

  public RegressionTestHook() throws Exception {
  }

  public void run(SessionState sess, Set<ReadEntity> inputs,
      Set<WriteEntity> outputs, LineageInfo lInfo,
      UserGroupInformation ugi) throws Exception {
    HiveConf conf = sess.getConf();

    String hookSwitch = conf.get(REGRESSION_TEST_PRINT_SWITCH_VAR_NAME, "");

    if (!hookSwitch.equals("1")) {
      return;
    }

    String inputStr = "";

    if (inputs != null) {
      StringBuilder inputsSB = new StringBuilder();

      boolean first = true;

      for (ReadEntity inp : inputs) {
        if (!first)
          inputsSB.append(",");
        first = false;
        inputsSB.append(inp.toString());
      }
      inputStr = StringEscapeUtils.escapeJava(inputsSB.toString());
    }

    String outputStr = "";

    if (outputs != null) {
      StringBuilder outputsSB = new StringBuilder();

      boolean first = true;

      for (WriteEntity o : outputs) {
        if (!first)
          outputsSB.append(",");
        first = false;
        outputsSB.append(o.toString());
      }
      outputStr = StringEscapeUtils.escapeJava(outputsSB.toString());
    }

    String queryId = conf.getVar(HiveConf.ConfVars.HIVEQUERYID);

    System.out
        .println("++++++++++Regression Test Hook Output Start+++++++++");

    System.out.println("+++queryId:" + queryId);
    System.out.println("+++input:" + inputStr);
    System.out.println("+++output:" + outputStr);
    System.out
        .println("++++++++++Regression Test Hook Output End+++++++++");
  }
}
