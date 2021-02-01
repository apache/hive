/*
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
package org.apache.hive.testutils.junit.extensions;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A JUnit Jupiter extension for creating and destroying TCP servers.
 *
 * @see DoNothingTCPServer
 */
public class DoNothingTCPServerExtension implements ParameterResolver, AfterEachCallback {
  
  @Override
  public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return parameterContext.getParameter().getType() == DoNothingTCPServer.class;
  }

  @Override
  public Object resolveParameter(ParameterContext parameterContext, ExtensionContext context)
      throws ParameterResolutionException {
    List<DoNothingTCPServer> servers = (List<DoNothingTCPServer>) context.getStore(ExtensionContext.Namespace.GLOBAL)
        .getOrComputeIfAbsent(context.getUniqueId(), (id) -> new ArrayList<DoNothingTCPServer>());
    try {
      DoNothingTCPServer server = new DoNothingTCPServer();
      server.start();
      servers.add(server);
      return server;
    } catch (IOException e) {
      throw new ParameterResolutionException("Problem when initialising the server; see cause for details.", e);
    }
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    List<DoNothingTCPServer> servers =
        (List<DoNothingTCPServer>) context.getStore(ExtensionContext.Namespace.GLOBAL).remove(context.getUniqueId());
    for (DoNothingTCPServer s : servers) {
      s.stop();
    }
  }

}
