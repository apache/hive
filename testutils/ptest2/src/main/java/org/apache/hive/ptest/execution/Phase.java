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
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hive.ptest.execution.LocalCommand.CollectLogPolicy;
import org.apache.hive.ptest.execution.ssh.NonZeroExitCodeException;
import org.apache.hive.ptest.execution.ssh.RemoteCommandResult;
import org.apache.hive.ptest.execution.ssh.SSHExecutionException;
import org.apache.hive.ptest.execution.ssh.SSHResult;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public abstract class Phase {

  protected final List<HostExecutor> hostExecutors;
  private final LocalCommandFactory localCommandFactory;
  private final ImmutableMap<String, String> templateDefaults;
  protected final Logger logger;

  public Phase(List<HostExecutor> hostExecutors,
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
  protected List<RemoteCommandResult> rsyncFromLocalToRemoteInstances(String localFile, String remoteFile)
      throws Exception {
    List<ListenableFuture<List<ListenableFuture<RemoteCommandResult>>>> futures = Lists.newArrayList();
    for(HostExecutor hostExecutor : hostExecutors) {
      futures.add(hostExecutor.rsyncFromLocalToRemoteInstances(localFile, remoteFile));
    }
    return flatten(futures);
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
  protected List<SSHResult> execHostsIgnoreErrors(String command)
      throws Exception {
    List<ListenableFuture<SSHResult>> futures = Lists.newArrayList();
    for(HostExecutor hostExecutor : hostExecutors) {
      futures.add(hostExecutor.execIgnoreAllErrors(command));
    }
    return toListOfResults(futures, false);
  }
  // clean prep
  protected List<RemoteCommandResult> execInstances(String command)
      throws Exception {
    List<ListenableFuture<RemoteCommandResult>> futures = Lists.newArrayList();
    for(HostExecutor hostExecutor : hostExecutors) {
      futures.addAll(hostExecutor.execInstances(command));
    }
    return toListOfResults(futures);
  }
  protected List<RemoteCommandResult> initalizeHosts()
      throws Exception {
    List<ListenableFuture<List<RemoteCommandResult>>> futures = Lists.newArrayList();
    ListeningExecutorService executor = MoreExecutors.
        listeningDecorator(Executors.newFixedThreadPool(hostExecutors.size()));
    try {
      for(final HostExecutor hostExecutor : hostExecutors) {
        futures.add(executor.submit(new Callable<List<RemoteCommandResult>>() {
          @Override
          public List<RemoteCommandResult> call() throws Exception {
            return initalizeHost(hostExecutor);
          }
        }));
      }
      List<RemoteCommandResult> results = Lists.newArrayList();
      for(ListenableFuture<List<RemoteCommandResult>> future : futures) {
        List<RemoteCommandResult> result = future.get();
        if(result != null) {
          results.addAll(result);
        }
      }
      executor.shutdown();
      return results;
    } finally {
      if(executor.isShutdown()) {
        executor.shutdownNow();
      }
    }
  }
  protected List<RemoteCommandResult> initalizeHost(HostExecutor hostExecutor)
      throws Exception {
    List<RemoteCommandResult> results = Lists.newArrayList();
    results.add(hostExecutor.exec("killall -q -9 -f java || true").get());
    TimeUnit.SECONDS.sleep(1);
    // order matters in all of these so block
    results.addAll(toListOfResults(hostExecutor.execInstances("rm -rf $localDir/$instanceName/scratch $localDir/$instanceName/logs")));
    results.addAll(toListOfResults(hostExecutor.execInstances("mkdir -p $localDir/$instanceName/logs " +
        "$localDir/$instanceName/maven " +
        "$localDir/$instanceName/scratch " +
        "$localDir/$instanceName/ivy " +
        "$localDir/$instanceName/${repositoryName}-source")));
    // order does not matter below, so go wide
    List<ListenableFuture<List<ListenableFuture<RemoteCommandResult>>>> futures = Lists.newArrayList();
    futures.add(hostExecutor.rsyncFromLocalToRemoteInstances("$workingDir/${repositoryName}-source", "$localDir/$instanceName/"));
    futures.add(hostExecutor.rsyncFromLocalToRemoteInstances("$workingDir/maven", "$localDir/$instanceName/"));
    futures.add(hostExecutor.rsyncFromLocalToRemoteInstances("$workingDir/ivy", "$localDir/$instanceName/"));
    results.addAll(flatten(futures));
    return results;
  }
  private <T extends RemoteCommandResult> List<T> flatten(List<ListenableFuture<List<ListenableFuture<T>>>> futures)
      throws Exception {
    List<T> results = Lists.newArrayList();
    for(ListenableFuture<List<ListenableFuture<T>>> future : futures) {
      List<ListenableFuture<T>> result = future.get();
      if(result != null) {
        results.addAll(toListOfResults(result));
      }
    }
    return results;
  }
  private <T extends RemoteCommandResult> List<T> toListOfResults(List<ListenableFuture<T>> futures)
      throws Exception {
    return toListOfResults(futures, true);
  }
  private <T extends RemoteCommandResult> List<T> toListOfResults(List<ListenableFuture<T>> futures,
      boolean reportErrors)
      throws Exception {
    List<T> results = Lists.newArrayList();
    for(T result : Futures.allAsList(futures).get()) {
      if(result != null) {
        if(reportErrors && (result.getException() != null || result.getExitCode() != 0)) {
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
}
