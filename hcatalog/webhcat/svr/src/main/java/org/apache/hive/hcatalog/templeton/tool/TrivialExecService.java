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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.templeton.tool;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Execute a local program.  This is a singleton service that will
 * execute a programs on the local box.
 */
public class TrivialExecService {
  private static volatile TrivialExecService theSingleton;
  private static final Log LOG = LogFactory.getLog(TrivialExecService.class);

  /**
   * Retrieve the singleton.
   */
  public static synchronized TrivialExecService getInstance() {
    if (theSingleton == null)
      theSingleton = new TrivialExecService();
    return theSingleton;
  }

  public Process run(List<String> cmd, List<String> removeEnv,
             Map<String, String> environmentVariables)
    throws IOException {
    logDebugCmd(cmd, environmentVariables);
    ProcessBuilder pb = new ProcessBuilder(cmd);
    for (String key : removeEnv)
      pb.environment().remove(key);
    pb.environment().putAll(environmentVariables);
    return pb.start();
  }

  private void logDebugCmd(List<String> cmd,
    Map<String, String> environmentVariables) {
    if(!LOG.isDebugEnabled()){
      return;
    }
    LOG.debug("starting " + cmd);
    LOG.debug("With environment variables: " );
    for(Map.Entry<String, String> keyVal : environmentVariables.entrySet()){
      LOG.debug(keyVal.getKey() + "=" + keyVal.getValue());
    }
    LOG.debug("With environment variables already set: " );
    Map<String, String> env = System.getenv();
    for (String envName : env.keySet()) {
      LOG.debug(envName + "=" + env.get(envName));
    }
  }
}
