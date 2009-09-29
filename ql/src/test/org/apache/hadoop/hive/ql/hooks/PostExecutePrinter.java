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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;

/**
 * Implementation of a post execute hook that simply prints out its
 * parameters to standard output.
 */
public class PostExecutePrinter implements PostExecute {

  @Override
  public void run(SessionState sess, Set<ReadEntity> inputs,
      Set<WriteEntity> outputs, UserGroupInformation ugi)
    throws Exception {

    LogHelper console = SessionState.getConsole();

    if (console == null)
      return;

    if (sess != null) {
      console.printError("POSTHOOK: query: " + sess.getCmd().trim());
      console.printError("POSTHOOK: type: " + sess.getCommandType());
    }

    for(ReadEntity re: inputs) {
      console.printError("POSTHOOK: Input: " + re.toString());
    }
    for(WriteEntity we: outputs) {
      console.printError("POSTHOOK: Output: " + we.toString());
    }
  }

}
