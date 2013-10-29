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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 * Execute a local program.  This is a singleton service that will
 * execute a programs on the local box.
 * 
 * Note that is is executed from LaunchMapper which is executed in 
 * different JVM from WebHCat (Templeton) server.  Thus it should not call any classes
 * not available on every node in the cluster (outside webhcat jar)
 */
final class TrivialExecService {
  //with default log4j config, this output ends up in 'syslog' of the LaunchMapper task
  private static final Log LOG = LogFactory.getLog(TrivialExecService.class);
  private static volatile TrivialExecService theSingleton;
  private static final String HADOOP_CLIENT_OPTS = "HADOOP_CLIENT_OPTS";
  /**
   * Retrieve the singleton.
   */
  public static synchronized TrivialExecService getInstance() {
    if (theSingleton == null)
      theSingleton = new TrivialExecService();
    return theSingleton;
  }
  /**
   * See {@link JobSubmissionConstants#CONTAINER_LOG4J_PROPS} file for details.
   */
  private static void hadoop2LogRedirect(ProcessBuilder processBuilder) {
    Map<String, String> env = processBuilder.environment();
    if(!env.containsKey(HADOOP_CLIENT_OPTS)) {
      return;
    }
    String hcopts = env.get(HADOOP_CLIENT_OPTS);
    if(!hcopts.contains("log4j.configuration=container-log4j.properties")) {
      return;
    }
    //TempletonControllerJob ensures that this file is in DistributedCache
    File log4jProps = new File(JobSubmissionConstants.CONTAINER_LOG4J_PROPS);
    hcopts = hcopts.replace("log4j.configuration=container-log4j.properties",
            "log4j.configuration=file://" + log4jProps.getAbsolutePath());
    //helps figure out what log4j is doing, but may confuse 
    //some jobs due to extra output to stdout
    //hcopts = hcopts + " -Dlog4j.debug=true";
    env.put(HADOOP_CLIENT_OPTS, hcopts);
  }
  public Process run(List<String> cmd, List<String> removeEnv,
             Map<String, String> environmentVariables, boolean overrideContainerLog4jProps)
    throws IOException {
    LOG.info("run(cmd, removeEnv, environmentVariables, " + overrideContainerLog4jProps + ")");
    LOG.info("Starting cmd: " + cmd);
    ProcessBuilder pb = new ProcessBuilder(cmd);
    for (String key : removeEnv) {
      if(pb.environment().containsKey(key)) {
        LOG.info("Removing env var: " + key + "=" + pb.environment().get(key));
      }
      pb.environment().remove(key);
    }
    pb.environment().putAll(environmentVariables);
    if(overrideContainerLog4jProps) {
      hadoop2LogRedirect(pb);
    }
    logDebugInfo("Starting process with env:", pb.environment());
    return pb.start();
  }
  private static void logDebugInfo(String msg, Map<String, String> props) {
    LOG.info(msg);
    List<String> keys = new ArrayList<String>();
    keys.addAll(props.keySet());
    Collections.sort(keys);
    for(String key : keys) {
      LOG.info(key + "=" + props.get(key));
    }    
  }
}
