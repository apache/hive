/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.ptest.execution;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class CleanupPhase extends Phase {

  public CleanupPhase(ImmutableList<HostExecutor> hostExecutors,
      LocalCommandFactory localCommandFactory,
      ImmutableMap<String, String> templateDefaults, Logger logger) {
    super(hostExecutors, localCommandFactory, templateDefaults, logger);
  }
  @Override
public void execute() throws Exception {
    execHosts("killall -q -9 -f java || true");
    TimeUnit.SECONDS.sleep(1);
    execLocally("rm -rf $workingDir/scratch");
    execInstances("rm -rf $localDir/$instanceName/scratch $localDir/$instanceName/logs");
  }
}