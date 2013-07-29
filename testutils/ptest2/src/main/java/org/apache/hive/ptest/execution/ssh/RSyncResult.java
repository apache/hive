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

public class RSyncResult extends AbstractSSHResult {
  private final String localFile;
  private final String remoteFile;
  public RSyncResult(String user, String host, int instance,
      String localFile, String remoteFile, int exitCode,
      Exception exception, String output) {
    super(user, host, instance, exitCode, exception, output);
    this.localFile = localFile;
    this.remoteFile = remoteFile;
  }
  public String getLocalFile() {
    return localFile;
  }
  public String getRemoteFile() {
    return remoteFile;
  }
  @Override
  public String toString() {
    return "RSyncResult [localFile=" + localFile + ", remoteFile="
        + remoteFile + ", getExitCode()=" + getExitCode() + ", getException()="
        + getException() + ", getUser()=" + getUser() + ", getHost()="
        + getHost() + ", getInstance()=" + getInstance() + "]";
  }
}
