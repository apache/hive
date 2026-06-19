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

import java.io.PrintStream;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.hive.conf.HiveVariableSource;
import org.apache.hadoop.hive.conf.VariableSubstitution;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;

/**
 * DfsProcessor.
 *
 */
public class DfsProcessor implements CommandProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(DfsProcessor.class.getName());
  private static final LogHelper console = new LogHelper(LOG);
  public static final String DFS_RESULT_HEADER = "DFS Output";

  private final FsShell dfs;
  private final Schema dfsSchema;

  public DfsProcessor(Configuration conf) {
    this(conf, false);
  }

  public DfsProcessor(Configuration conf, boolean addSchema) {
    dfs = new FsShell(conf);
    dfsSchema = new Schema();
    dfsSchema.addToFieldSchemas(new FieldSchema(DFS_RESULT_HEADER, serdeConstants.STRING_TYPE_NAME, ""));
  }

  @Override
  public CommandProcessorResponse run(String command) throws CommandProcessorException {


    try {
      SessionState ss = SessionState.get();
      command = new VariableSubstitution(new HiveVariableSource() {
        @Override
        public Map<String, String> getHiveVariable() {
          return SessionState.get().getHiveVariables();
        }
      }).substitute(ss.getConf(), command);

      String[] tokens = splitCmd(command);
      CommandProcessorResponse authErrResp =
          CommandUtil.authorizeCommand(ss, HiveOperationType.DFS, Arrays.asList(tokens));
      if(authErrResp != null){
        // there was an authorization issue
        return authErrResp;
      }

      PrintStream oldOut = System.out;

      if (ss != null && ss.out != null) {
        System.setOut(ss.out);
      }

      int ret = dfs.run(tokens);
      System.setOut(oldOut);
      if (ret != 0) {
        console.printError("Command " + command + " failed with exit code = " + ret);
        throw new CommandProcessorException(ret);
      }
      return new CommandProcessorResponse(dfsSchema, null);
    } catch (CommandProcessorException e) {
      throw e;
    } catch (Exception e) {
      console.printError("Exception raised from DFSShell.run "
          + e.getLocalizedMessage(), org.apache.hadoop.util.StringUtils
          .stringifyException(e));
      throw new CommandProcessorException(1);
    }
  }

  private String[] splitCmd(String command) throws HiveException {

    ArrayList<String> paras = new ArrayList<String>();
    int cmdLng = command.length();
    char y = 0;
    int start = 0;

    for (int i = 0; i < cmdLng; i++) {
      char x = command.charAt(i);

      switch(x) {
        case ' ':
          if (y == 0) {
            String str = command.substring(start, i).trim();
            if (!str.equals("")) {
              paras.add(str);
              start = i + 1;
            }
          }
          break;
        case '"':
          if (y == 0) {
            y = x;
            start = i + 1;
          } else if ('"' == y) {
            paras.add(command.substring(start, i).trim());
            y = 0;
            start = i + 1;
          }
          break;
        case '\'':
          if (y == 0) {
            y = x;
            start = i + 1;
          } else if ('\'' == y) {
            paras.add(command.substring(start, i).trim());
            y = 0;
            start = i + 1;
          }
          break;
        default:
          if (i == cmdLng-1 && start < cmdLng) {
            paras.add(command.substring(start, cmdLng).trim());
          }
          break;
      }
    }

    if (y != 0) {
      String message = "Syntax error on hadoop options: dfs " + command;
      console.printError(message);
      throw new HiveException(message);
    }

    return paras.toArray(new String[paras.size()]);
  }

  @Override
  public void close() throws Exception {
  }
}
