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
 * Represents the state of the yarn application.
 */
class AmInfo {
  private String appName;
  private String appType;
  private String appId;
  private String containerId;
  private String hostname;
  private String amWebUrl;

  AmInfo setAppName(String appName) {
    this.appName = appName;
    return this;
  }

  AmInfo setAppType(String appType) {
    this.appType = appType;
    return this;
  }

  AmInfo setAppId(String appId) {
    this.appId = appId;
    return this;
  }

  AmInfo setContainerId(String containerId) {
    this.containerId = containerId;
    return this;
  }

  AmInfo setHostname(String hostname) {
    this.hostname = hostname;
    return this;
  }

  AmInfo setAmWebUrl(String amWebUrl) {
    this.amWebUrl = amWebUrl;
    return this;
  }

  String getAppName() {
    return appName;
  }

  String getAppType() {
    return appType;
  }

  String getAppId() {
    return appId;
  }

  String getContainerId() {
    return containerId;
  }

  String getHostname() {
    return hostname;
  }

  String getAmWebUrl() {
    return amWebUrl;
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }
}
