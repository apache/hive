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

public class RSyncCommand extends AbstractSSHCommand<RSyncResult> {
  private final RSyncCommandExecutor executor;
  private final String localFile;
  private final String remoteFile;
  private RSyncCommand.Type type;
  public RSyncCommand(RSyncCommandExecutor executor, String privateKey,
      String user, String host, int instance,
      String localFile, String remoteFile, RSyncCommand.Type type) {
    super(privateKey, user, host, instance);
    this.executor = executor;
    this.localFile = localFile;
    this.remoteFile = remoteFile;
    this.type = type;
  }
  public RSyncCommand.Type getType() {
    return type;
  }
  public String getLocalFile() {
    return localFile;
  }
  public String getRemoteFile() {
    return remoteFile;
  }
  @Override
  public RSyncResult call() {
    executor.execute(this);
    return new RSyncResult(getUser(), getHost(), getInstance(), getLocalFile(), getRemoteFile(),
        getExitCode(), getException(), getOutput());
  }

  @Override
  public String toString() {
    return "RSyncCommand [executor=" + executor + ", localFile=" + localFile
        + ", remoteFile=" + remoteFile + ", type=" + type + ", getHost()="
        + getHost() + ", getInstance()=" + getInstance() + "]";
  }

  public static enum Type {
    FROM_LOCAL(),
    TO_LOCAL();
  }
}