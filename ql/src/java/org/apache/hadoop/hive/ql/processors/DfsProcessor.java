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

package org.apache.hadoop.hive.ql.processors;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.hive.conf.HiveVariableSource;
import org.apache.hadoop.hive.conf.VariableSubstitution;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;

/**
 * DfsProcessor.
 *
 */
public class DfsProcessor implements CommandProcessor {

  public static final Logger LOG = LoggerFactory.getLogger(DfsProcessor.class.getName());
  public static final LogHelper console = new LogHelper(LOG);
  public static final String DFS_RESULT_HEADER = "DFS Output";

  private final FsShell dfs;
  private final Schema dfsSchema;

  public DfsProcessor(Configuration conf) {
    this(conf, false);
  }

  public DfsProcessor(Configuration conf, boolean addSchema) {
    dfs = new FsShell(conf);
    dfsSchema = new Schema();
    dfsSchema.addToFieldSchemas(new FieldSchema(DFS_RESULT_HEADER, "string", ""));
  }

  @Override
  public void init() {
  }

  @Override
  public CommandProcessorResponse run(String command) {


    try {
      SessionState ss = SessionState.get();
      command = new VariableSubstitution(new HiveVariableSource() {
        @Override
        public Map<String, String> getHiveVariable() {
          return SessionState.get().getHiveVariables();
        }
      }).substitute(ss.getConf(), command);

      String[] tokens = command.split("\\s+");
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
      if (ret != 0) {
        console.printError("Command failed with exit code = " + ret);
      }

      System.setOut(oldOut);
      return new CommandProcessorResponse(ret, null, null, dfsSchema);

    } catch (Exception e) {
      console.printError("Exception raised from DFSShell.run "
          + e.getLocalizedMessage(), org.apache.hadoop.util.StringUtils
          .stringifyException(e));
      return new CommandProcessorResponse(1);
    }
  }

}
