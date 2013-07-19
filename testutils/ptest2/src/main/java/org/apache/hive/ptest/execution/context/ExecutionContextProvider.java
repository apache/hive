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

import org.apache.hive.ptest.execution.conf.Context;

public interface ExecutionContextProvider {
  static final String PRIVATE_KEY = "privateKey";

  public ExecutionContext createExecutionContext() throws CreateHostsFailedException, ServiceNotAvailableException;

  public void replaceBadHosts(ExecutionContext executionContext)throws CreateHostsFailedException ;

  public void terminate(ExecutionContext executionContext);

  public void close();

  public interface Builder {
    public ExecutionContextProvider build(Context context, String workingDirectory) throws Exception;
  }

}
