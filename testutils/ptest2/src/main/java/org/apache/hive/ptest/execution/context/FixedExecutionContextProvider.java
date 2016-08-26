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

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.apache.hive.ptest.execution.Dirs;
import org.apache.hive.ptest.execution.conf.Context;
import org.apache.hive.ptest.execution.conf.Host;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class FixedExecutionContextProvider implements ExecutionContextProvider {
  private static final Logger LOG = LoggerFactory
      .getLogger(FixedExecutionContextProvider.class);
  private final ExecutionContext mExecutionContext;
  private final File mWorkingDir;

  private FixedExecutionContextProvider(Set<Host> hosts, String workingDirectory, String privateKey)
      throws IOException {
    mWorkingDir = Dirs.create(new File(workingDirectory, "working"));
    mExecutionContext = new ExecutionContext(this, hosts, mWorkingDir.getAbsolutePath(), privateKey);
  }
  @Override
  public ExecutionContext createExecutionContext()
      throws CreateHostsFailedException, ServiceNotAvailableException {
    return mExecutionContext;
  }

  @Override
  public void terminate(ExecutionContext executionContext) {
    // do nothing
  }
  @Override
  public void replaceBadHosts(ExecutionContext executionContext) throws CreateHostsFailedException {
    if (!executionContext.getBadHosts().isEmpty()) {
      LOG.warn(
          "Found bad nodes on FixedExecutionContext. Cannot replace them. Degraded performance. badNodes={}",
          executionContext.getBadHosts());
    }
  }

  @Override
  public void close(){

  }

  public static class Builder implements ExecutionContextProvider.Builder {
    @Override
    public ExecutionContextProvider build(Context context, String workingDirectory) throws Exception {
      String privateKey = Preconditions.checkNotNull(context.getString(PRIVATE_KEY), PRIVATE_KEY + " is required");
      Set<Host> hosts = Sets.newHashSet();
      for(String alias : Splitter.on(" ").omitEmptyStrings().split(context.getString("hosts", ""))) {
        Context hostContext = new Context(context.getSubProperties(
            Joiner.on(".").join("host", alias, "")));
        LOG.info("Processing host {}: {}", alias, hostContext.getParameters().toString());
        hosts.add(new Host(hostContext.getString("host"), hostContext.getString("user"),
            Iterables.toArray(Splitter.on(",").trimResults().split(hostContext.getString("localDirs")), String.class),
            hostContext.getInteger("threads")));
      }
      Preconditions.checkState(hosts.size() > 0, "no hosts specified");
      ExecutionContextProvider hostProvider = new FixedExecutionContextProvider(hosts, workingDirectory, privateKey);
      return hostProvider;
    }
  }
}
