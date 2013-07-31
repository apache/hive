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
package org.apache.hive.ptest.execution.ssh;

import java.util.concurrent.Callable;

public abstract class AbstractSSHCommand<RESULT> implements Callable<RESULT> {
  private final String privateKey;
  private final String user;
  private final String host;
  private final int instance;
  private int exitCode = -1;
  private Exception exception;
  private String output;

  public AbstractSSHCommand(String privateKey,
      String user, String host, int instance) {
    this.privateKey = privateKey;
    this.user = user;
    this.host = host;
    this.instance = instance;
  }

  public void setException(Exception exception) {
    this.exception = exception;
  }
  public void setExitCode(int exitCode) {
    this.exitCode = exitCode;
  }
  public void setOutput(String output) {
    this.output = output;
  }
  public int getExitCode() {
    return exitCode;
  }
  public Exception getException() {
    return exception;
  }
  public String getOutput() {
    return output;
  }
  public String getPrivateKey() {
    return privateKey;
  }
  public String getUser() {
    return user;
  }
  public String getHost() {
    return host;
  }
  public int getInstance() {
    return instance;
  }
}
