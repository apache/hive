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

package org.apache.hadoop.hive.llap.cli.status;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Representing the state of an Llap instance monitored.
 */
class LlapInstance {
  private final String hostname;
  private final String containerId;
  private String logUrl;

  // Only for live instances.
  private String statusUrl;
  private String webUrl;
  private Integer rpcPort;
  private Integer mgmtPort;
  private Integer shufflePort;

  // For completed instances
  private String diagnostics;
  private int yarnContainerExitStatus;

  // TODO HIVE-13454 Add additional information such as #executors, container size, etc

  LlapInstance(String hostname, String containerId) {
    this.hostname = hostname;
    this.containerId = containerId;
  }

  LlapInstance setLogUrl(String logUrl) {
    this.logUrl = logUrl;
    return this;
  }

  LlapInstance setStatusUrl(String statusUrl) {
    this.statusUrl = statusUrl;
    return this;
  }

  LlapInstance setWebUrl(String webUrl) {
    this.webUrl = webUrl;
    return this;
  }

  LlapInstance setRpcPort(int rpcPort) {
    this.rpcPort = rpcPort;
    return this;
  }

  LlapInstance setMgmtPort(int mgmtPort) {
    this.mgmtPort = mgmtPort;
    return this;
  }

  LlapInstance setShufflePort(int shufflePort) {
    this.shufflePort = shufflePort;
    return this;
  }

  LlapInstance setDiagnostics(String diagnostics) {
    this.diagnostics = diagnostics;
    return this;
  }

  LlapInstance setYarnContainerExitStatus(int yarnContainerExitStatus) {
    this.yarnContainerExitStatus = yarnContainerExitStatus;
    return this;
  }

  String getHostname() {
    return hostname;
  }

  String getContainerId() {
    return containerId;
  }

  String getLogUrl() {
    return logUrl;
  }

  String getStatusUrl() {
    return statusUrl;
  }

  String getWebUrl() {
    return webUrl;
  }

  Integer getRpcPort() {
    return rpcPort;
  }

  Integer getMgmtPort() {
    return mgmtPort;
  }

  Integer getShufflePort() {
    return shufflePort;
  }

  String getDiagnostics() {
    return diagnostics;
  }

  int getYarnContainerExitStatus() {
    return yarnContainerExitStatus;
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }
}
