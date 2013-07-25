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
package org.apache.hive.ptest.execution.context;

import java.util.Set;

import org.apache.hive.ptest.execution.conf.Host;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class ExecutionContext {
  private final Set<Host> mHosts;
  private final String mLocalWorkingDirectory;
  private final String mPrivateKey;
  private final ExecutionContextProvider mExecutionContextProvider;
  private final Set<Host> mBadHosts;
  public ExecutionContext(ExecutionContextProvider executionContextProvider,
      Set<Host> hosts, String localWorkingDirectory, String privateKey) {
    super();
    mExecutionContextProvider = executionContextProvider;
    mHosts = hosts;
    mLocalWorkingDirectory = localWorkingDirectory;
    mPrivateKey = privateKey;
    mBadHosts = Sets.newHashSet();
  }
  public void addBadHost(Host host) {
    mBadHosts.add(host);
  }
  public void clearBadHosts() {
    mBadHosts.clear();
  }
  void addHost(Host host) {
    mHosts.add(host);
  }
  boolean removeHost(Host host) {
    return mHosts.remove(host);
  }
  public ImmutableSet<Host> getBadHosts() {
    return ImmutableSet.copyOf(mBadHosts);
  }
  public ImmutableSet<Host> getHosts() {
    return  ImmutableSet.copyOf(mHosts);
  }
  public String getLocalWorkingDirectory() {
    return mLocalWorkingDirectory;
  }
  public String getPrivateKey() {
    return mPrivateKey;
  }
  public void replaceBadHosts() throws CreateHostsFailedException {
    mExecutionContextProvider.replaceBadHosts(this);
  }
  public void terminate() {
    mExecutionContextProvider.terminate(this);
  }
}
