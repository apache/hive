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

import java.util.Arrays;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveVariableSource;
import org.apache.hadoop.hive.conf.VariableSubstitution;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DeleteResourceProcessor.
 *
 */
public class DeleteResourceProcessor implements CommandProcessor {

  public static final Logger LOG = LoggerFactory.getLogger(DeleteResourceProcessor.class.getName());
  public static final LogHelper console = new LogHelper(LOG);

  @Override
  public void init() {
  }

  @Override
  public CommandProcessorResponse run(String command) {
    SessionState ss = SessionState.get();
    command = new VariableSubstitution(new HiveVariableSource() {
      @Override
      public Map<String, String> getHiveVariable() {
        return SessionState.get().getHiveVariables();
      }
    }).substitute(ss.getConf(), command);
    String[] tokens = command.split("\\s+");

    SessionState.ResourceType t;
    if (tokens.length < 1
        || (t = SessionState.find_resource_type(tokens[0])) == null) {
      console.printError("Usage: delete ["
          + StringUtils.join(SessionState.ResourceType.values(), "|")
          + "] <value> [<value>]*");
      return new CommandProcessorResponse(1);
    }
    CommandProcessorResponse authErrResp =
        CommandUtil.authorizeCommand(ss, HiveOperationType.DELETE, Arrays.asList(tokens));
    if(authErrResp != null){
      // there was an authorization issue
      return authErrResp;
    }
    if (tokens.length >= 2) {
      ss.delete_resources(t, Arrays.asList(Arrays.copyOfRange(tokens, 1, tokens.length)));
    } else {
      ss.delete_resources(t);
    }

    return new CommandProcessorResponse(0);
  }
}
