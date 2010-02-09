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

import static org.apache.commons.lang.StringUtils.isBlank;

import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * CommandProcessorFactory.
 *
 */
public final class CommandProcessorFactory {

  private CommandProcessorFactory() {
    // prevent instantiation
  }

  public static CommandProcessor get(String cmd) {
    String cmdl = cmd.toLowerCase();

    if ("set".equals(cmdl)) {
      return new SetProcessor();
    } else if ("dfs".equals(cmdl)) {
      SessionState ss = SessionState.get();
      return new DfsProcessor(ss.getConf());
    } else if ("add".equals(cmdl)) {
      return new AddResourceProcessor();
    } else if ("delete".equals(cmdl)) {
      return new DeleteResourceProcessor();
    } else if (!isBlank(cmd)) {
      return new Driver();
    }
    return null;
  }

}
