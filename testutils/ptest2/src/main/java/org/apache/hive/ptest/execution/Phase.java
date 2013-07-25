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

import java.io.IOException;
import java.util.List;

import org.apache.hive.ptest.execution.LocalCommand.CollectLogPolicy;
import org.apache.hive.ptest.execution.ssh.NonZeroExitCodeException;
import org.apache.hive.ptest.execution.ssh.RSyncResult;
import org.apache.hive.ptest.execution.ssh.RemoteCommandResult;
import org.apache.hive.ptest.execution.ssh.SSHExecutionException;
import org.apache.hive.ptest.execution.ssh.SSHResult;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public abstract class Phase {

  protected final ImmutableList<HostExecutor> hostExecutors;
  private final LocalCommandFactory localCommandFactory;
  private final ImmutableMap<String, String> templateDefaults;
  protected final Logger logger;

  public Phase(ImmutableList<HostExecutor> hostExecutors,
      LocalCommandFactory localCommandFactory,
      ImmutableMap<String, String> templateDefaults, Logger logger) {
    super();
    this.hostExecutors = hostExecutors;
    this.localCommandFactory = localCommandFactory;
    this.templateDefaults = templateDefaults;
    this.logger = logger;
  }

  public abstract void execute() throws Throwable;

  // clean prep
  protected void execLocally(String command)
      throws IOException, InterruptedException, NonZeroExitCodeException {
    CollectLogPolicy localCollector = new CollectLogPolicy(logger);
    command = Templates.getTemplateResult(command, templateDefaults);
    LocalCommand localCmd = localCommandFactory.create(localCollector, command);
    if(localCmd.getExitCode() != 0) {
      throw new NonZeroExitCodeException(String.format(
          "Command '%s' failed with exit status %d and output '%s'",
          command, localCmd.getExitCode(), localCollector.getOutput()));
    }
  }
  // prep
  protected List<RSyncResult> rsyncFromLocalToRemoteInstances(String localFile, String remoteFile)
      throws Exception {
    List<ListenableFuture<RSyncResult>> futures = Lists.newArrayList();
    for(HostExecutor hostExecutor : hostExecutors) {
      futures.addAll(hostExecutor.rsyncFromLocalToRemoteInstances(localFile, remoteFile));
    }
    return toListOfResults(futures);
  }

  // clean
  protected List<SSHResult> execHosts(String command)
      throws Exception {
    List<ListenableFuture<SSHResult>> futures = Lists.newArrayList();
    for(HostExecutor hostExecutor : hostExecutors) {
      futures.add(hostExecutor.exec(command));
    }
    return toListOfResults(futures);
  }
  // clean prep
  protected List<SSHResult> execInstances(String command)
      throws Exception {
    List<ListenableFuture<SSHResult>> futures = Lists.newArrayList();
    for(HostExecutor hostExecutor : hostExecutors) {
      futures.addAll(hostExecutor.execInstances(command));
    }
    return toListOfResults(futures);
  }
  private <T extends RemoteCommandResult> List<T> toListOfResults(List<ListenableFuture<T>> futures)
  throws Exception {
    List<T> results = Lists.newArrayList();
    for(T result : Futures.allAsList(futures).get()) {
      if(result != null) {
        if(result.getException() != null || result.getExitCode() != 0) {
          throw new SSHExecutionException(result);
        }
        results.add(result);
      }
    }
    return results;
  }
  protected ImmutableMap<String, String> getTemplateDefaults() {
    return templateDefaults;
  }
  protected ImmutableList<HostExecutor> getHostExecutors() {
    return hostExecutors;
  }
}
