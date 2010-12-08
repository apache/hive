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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.hive.ql.parse.VariableSubstitution;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;

/**
 * DfsProcessor.
 *
 */
public class DfsProcessor implements CommandProcessor {

  public static final Log LOG = LogFactory.getLog(DfsProcessor.class.getName());
  public static final LogHelper console = new LogHelper(LOG);

  private final FsShell dfs;

  public DfsProcessor(Configuration conf) {
    dfs = new FsShell(conf);
  }

  public void init() {
  }

  public CommandProcessorResponse run(String command) {


    try {
      SessionState ss = SessionState.get();
      command = new VariableSubstitution().substitute(ss.getConf(),command);

      String[] tokens = command.split("\\s+");
      PrintStream oldOut = System.out;

      if (ss != null && ss.out != null) {
        System.setOut(ss.out);
      }

      int ret = dfs.run(tokens);
      if (ret != 0) {
        console.printError("Command failed with exit code = " + ret);
      }

      System.setOut(oldOut);
      return new CommandProcessorResponse(ret);

    } catch (Exception e) {
      console.printError("Exception raised from DFSShell.run "
          + e.getLocalizedMessage(), org.apache.hadoop.util.StringUtils
          .stringifyException(e));
      return new CommandProcessorResponse(1);
    }
  }

}
