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

package org.apache.hadoop.hive.ql.processors;

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.metadata.*;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * CommandProcessorFactory.
 *
 */
public final class CommandProcessorFactory {

  private CommandProcessorFactory() {
    // prevent instantiation
  }

  public static CommandProcessor getForHiveCommand(String[] cmd, HiveConf conf)
    throws SQLException {
    return getForHiveCommandInternal(cmd, conf, false);
  }

  public static CommandProcessor getForHiveCommandInternal(String[] cmd, HiveConf conf,
                                                           boolean testOnly)
    throws SQLException {
    HiveCommand hiveCommand = HiveCommand.find(cmd, testOnly);
    if (hiveCommand == null || isBlank(cmd[0])) {
      return null;
    }
    if (conf == null) {
      conf = new HiveConf();
    }
    Set<String> availableCommands = new HashSet<String>();
    for (String availableCommand : conf.getVar(HiveConf.ConfVars.HIVE_SECURITY_COMMAND_WHITELIST)
      .split(",")) {
      availableCommands.add(availableCommand.toLowerCase().trim());
    }
    if (!availableCommands.contains(cmd[0].trim().toLowerCase())) {
      throw new SQLException("Insufficient privileges to execute " + cmd[0], "42000");
    }
    if (cmd.length > 1 && "reload".equalsIgnoreCase(cmd[0])
      && "function".equalsIgnoreCase(cmd[1])) {
      // special handling for SQL "reload function"
      return null;
    }
    switch (hiveCommand) {
    case SET:
      return new SetProcessor();
    case RESET:
      return new ResetProcessor();
    case DFS:
      SessionState ss = SessionState.get();
      return new DfsProcessor(ss.getConf());
    case ADD:
      return new AddResourceProcessor();
    case LIST:
      return new ListResourceProcessor();
    case LLAP_CLUSTER:
      return new LlapClusterResourceProcessor();
    case LLAP_CACHE:
      return new LlapCacheResourceProcessor();
    case DELETE:
      return new DeleteResourceProcessor();
    case COMPILE:
      return new CompileProcessor();
    case RELOAD:
      return new ReloadProcessor();
    case CRYPTO:
      try {
        return new CryptoProcessor(SessionState.get().getHdfsEncryptionShim(), conf);
      } catch (HiveException e) {
        throw new SQLException("Fail to start the command processor due to the exception: ", e);
      }
    case ERASURE:
      try {
        return new ErasureProcessor(conf);
      } catch (IOException e) {
        throw new SQLException("Fail to start the erasure command processor due to the exception: ", e);
      }
    default:
      throw new AssertionError("Unknown HiveCommand " + hiveCommand);
    }
  }

  private static Logger LOG = LoggerFactory.getLogger(CommandProcessorFactory.class);

  public static CommandProcessor get(String[] cmd, @Nonnull HiveConf conf) throws SQLException {
    CommandProcessor result = getForHiveCommand(cmd, conf);
    if (result != null) {
      return result;
    }
    if (isBlank(cmd[0])) {
      return null;
    } else {
      return DriverFactory.newDriver(conf);
    }
  }
}
