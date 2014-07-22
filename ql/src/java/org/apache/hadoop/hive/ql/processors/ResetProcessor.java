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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.session.SessionState;

public class ResetProcessor implements CommandProcessor {

  @Override
  public void init() {
  }

  @Override
  public CommandProcessorResponse run(String command) throws CommandNeedRetryException {
    SessionState ss = SessionState.get();

    CommandProcessorResponse authErrResp =
        CommandUtil.authorizeCommand(ss, HiveOperationType.RESET, Arrays.asList(command));
    if(authErrResp != null){
      // there was an authorization issue
      return authErrResp;
    }

    if (ss.getOverriddenConfigurations().isEmpty()) {
      return new CommandProcessorResponse(0);
    }
    HiveConf conf = new HiveConf();
    for (String key : ss.getOverriddenConfigurations().keySet()) {
      String value = conf.get(key);
      if (value != null) {
        ss.getConf().set(key, value);
      }
    }
    ss.getOverriddenConfigurations().clear();
    return new CommandProcessorResponse(0);
  }
}
