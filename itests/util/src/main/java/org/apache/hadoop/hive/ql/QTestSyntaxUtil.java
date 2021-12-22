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
package org.apache.hadoop.hive.ql;

import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.processors.AddResourceProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.processors.CompileProcessor;
import org.apache.hadoop.hive.ql.processors.CryptoProcessor;
import org.apache.hadoop.hive.ql.processors.DfsProcessor;
import org.apache.hadoop.hive.ql.processors.ListResourceProcessor;
import org.apache.hadoop.hive.ql.processors.LlapCacheResourceProcessor;
import org.apache.hadoop.hive.ql.processors.LlapClusterResourceProcessor;
import org.apache.hadoop.hive.ql.processors.ReloadProcessor;
import org.apache.hadoop.hive.ql.processors.ResetProcessor;
import org.apache.hadoop.hive.ql.processors.SetProcessor;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.common.util.HiveStringUtils;

/**
 * Q File Syntax Checker Utility.
 *
 */
public class QTestSyntaxUtil {

  private QTestUtil qTestUtil;
  private HiveConf conf;
  private ParseDriver pd;

  public QTestSyntaxUtil(QTestUtil qTestUtil, HiveConf conf, ParseDriver pd) {
    this.qTestUtil = qTestUtil;
    this.conf = conf;
    this.pd = pd;
  }

  public void checkQFileSyntax(List<String> cmds) {
    String command = "";
    if (QTestSystemProperties.shouldCheckSyntax()) {
      //check syntax first
      for (String oneCmd : cmds) {
        if (StringUtils.endsWith(oneCmd, "\\")) {
          command += StringUtils.chop(oneCmd) + "\\;";
          continue;
        } else {
          if (qTestUtil.isHiveCommand(oneCmd)) {
            command = oneCmd;
          } else {
            command += oneCmd;
          }
        }
        if (StringUtils.isBlank(command)) {
          continue;
        }
        assertTrue("Syntax error in command: " + command, checkSyntax(command));
        command = "";
      }
    }
  }

  private boolean checkSyntax(String cmd) {
    ASTNode tree;
    int ret = 0;
    CliSessionState ss = (CliSessionState) SessionState.get();

    String cmdTrimmed = HiveStringUtils.removeComments(cmd).trim();
    String[] tokens = cmdTrimmed.split("\\s+");
    if (tokens[0].equalsIgnoreCase("source")) {
      return true;
    }
    if (cmdTrimmed.toLowerCase().equals("quit") || cmdTrimmed.toLowerCase().equals("exit")) {
      return true;
    }
    if (cmdTrimmed.startsWith("!")) {
      return true;
    }
    try {
      CommandProcessor proc = CommandProcessorFactory.get(tokens, (HiveConf) conf);
      if (proc instanceof IDriver) {
        try {
          tree = pd.parse(cmd, conf).getTree();
          qTestUtil.analyzeAST(tree);
        } catch (Exception e) {
          return false;
        }
      } else {
        ret = processLocalCmd(cmdTrimmed, proc, ss);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      return false;
    }
    if (ret != 0) {
      return false;
    }
    return true;
  }

  private static int processLocalCmd(String cmd, CommandProcessor proc, CliSessionState ss) {
    int ret = 0;
    if (proc != null) {
      String firstToken = cmd.trim().split("\\s+")[0];
      String cmd1 = cmd.trim().substring(firstToken.length()).trim();
      //CommandProcessorResponse res = proc.run(cmd_1);
      if (proc instanceof ResetProcessor ||
          proc instanceof CompileProcessor ||
          proc instanceof ReloadProcessor ||
          proc instanceof CryptoProcessor ||
          proc instanceof AddResourceProcessor ||
          proc instanceof ListResourceProcessor ||
          proc instanceof LlapClusterResourceProcessor ||
          proc instanceof LlapCacheResourceProcessor) {
        if (cmd1.trim().split("\\s+").length < 1) {
          ret = -1;
        }
      }
      if (proc instanceof SetProcessor) {
        if (!cmd1.contains("=")) {
          ret = -1;
        }
      }
      if (proc instanceof DfsProcessor) {
        String[] argv = cmd1.trim().split("\\s+");
        if ("-put".equals(firstToken) || "-test".equals(firstToken) ||
            "-copyFromLocal".equals(firstToken) || "-moveFromLocal".equals(firstToken)) {
          if (argv.length < 3) {
            ret = -1;
          }
        } else if ("-get".equals(firstToken) ||
            "-copyToLocal".equals(firstToken) || "-moveToLocal".equals(firstToken)) {
          if (argv.length < 3) {
            ret = -1;
          }
        } else if ("-mv".equals(firstToken) || "-cp".equals(firstToken)) {
          if (argv.length < 3) {
            ret = -1;
          }
        } else if ("-rm".equals(firstToken) || "-rmr".equals(firstToken) ||
            "-cat".equals(firstToken) || "-mkdir".equals(firstToken) ||
            "-touchz".equals(firstToken) || "-stat".equals(firstToken) ||
            "-text".equals(firstToken)) {
          if (argv.length < 2) {
            ret = -1;
          }
        }
      }
    }
    return ret;
  }

}
