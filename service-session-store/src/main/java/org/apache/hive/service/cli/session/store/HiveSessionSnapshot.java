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

package org.apache.hive.service.cli.session.store;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class HiveSessionSnapshot {

  private final String sessionHandleId;
  private final String username;
  private final String ipAddress;
  private final String currentDatabase;
  private final Map<String, String> overriddenConfigurations;
  private final List<String> addedJars;
  private final Map<String, String> tempTableDefinitions;
  private final int protocolVersion;
  private final long creationTime;
  private final long lastAccessTime;

  @JsonCreator
  public HiveSessionSnapshot(
      @JsonProperty("sessionHandleId") String sessionHandleId,
      @JsonProperty("username") String username,
      @JsonProperty("ipAddress") String ipAddress,
      @JsonProperty("currentDatabase") String currentDatabase,
      @JsonProperty("overriddenConfigurations") Map<String, String> overriddenConfigurations,
      @JsonProperty("addedJars") List<String> addedJars,
      @JsonProperty("tempTableDefinitions") Map<String, String> tempTableDefinitions,
      @JsonProperty("protocolVersion") int protocolVersion,
      @JsonProperty("creationTime") long creationTime,
      @JsonProperty("lastAccessTime") long lastAccessTime) {
    this.sessionHandleId = sessionHandleId;
    this.username = username;
    this.ipAddress = ipAddress;
    this.currentDatabase = currentDatabase;
    this.overriddenConfigurations = overriddenConfigurations != null
        ? new HashMap<>(overriddenConfigurations) : Collections.emptyMap();
    this.addedJars = addedJars != null ? new ArrayList<>(addedJars) : Collections.emptyList();
    this.tempTableDefinitions = tempTableDefinitions != null
        ? new HashMap<>(tempTableDefinitions) : Collections.emptyMap();
    this.protocolVersion = protocolVersion;
    this.creationTime = creationTime;
    this.lastAccessTime = lastAccessTime;
  }

  @JsonProperty("sessionHandleId")
  public String getSessionHandleId() {
    return sessionHandleId;
  }

  @JsonProperty("username")
  public String getUsername() {
    return username;
  }

  @JsonProperty("ipAddress")
  public String getIpAddress() {
    return ipAddress;
  }

  @JsonProperty("currentDatabase")
  public String getCurrentDatabase() {
    return currentDatabase;
  }

  @JsonProperty("overriddenConfigurations")
  public Map<String, String> getOverriddenConfigurations() {
    return overriddenConfigurations;
  }

  @JsonProperty("addedJars")
  public List<String> getAddedJars() {
    return addedJars;
  }

  @JsonProperty("tempTableDefinitions")
  public Map<String, String> getTempTableDefinitions() {
    return tempTableDefinitions;
  }

  @JsonProperty("protocolVersion")
  public int getProtocolVersion() {
    return protocolVersion;
  }

  @JsonProperty("creationTime")
  public long getCreationTime() {
    return creationTime;
  }

  @JsonProperty("lastAccessTime")
  public long getLastAccessTime() {
    return lastAccessTime;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String sessionHandleId;
    private String username;
    private String ipAddress;
    private String currentDatabase;
    private Map<String, String> overriddenConfigurations;
    private List<String> addedJars;
    private Map<String, String> tempTableDefinitions;
    private int protocolVersion;
    private long creationTime;
    private long lastAccessTime;

    public Builder sessionHandleId(String sessionHandleId) {
      this.sessionHandleId = sessionHandleId;
      return this;
    }

    public Builder username(String username) {
      this.username = username;
      return this;
    }

    public Builder ipAddress(String ipAddress) {
      this.ipAddress = ipAddress;
      return this;
    }

    public Builder currentDatabase(String currentDatabase) {
      this.currentDatabase = currentDatabase;
      return this;
    }

    public Builder overriddenConfigurations(Map<String, String> overriddenConfigurations) {
      this.overriddenConfigurations = overriddenConfigurations;
      return this;
    }

    public Builder addedJars(List<String> addedJars) {
      this.addedJars = addedJars;
      return this;
    }

    public Builder tempTableDefinitions(Map<String, String> tempTableDefinitions) {
      this.tempTableDefinitions = tempTableDefinitions;
      return this;
    }

    public Builder protocolVersion(int protocolVersion) {
      this.protocolVersion = protocolVersion;
      return this;
    }

    public Builder creationTime(long creationTime) {
      this.creationTime = creationTime;
      return this;
    }

    public Builder lastAccessTime(long lastAccessTime) {
      this.lastAccessTime = lastAccessTime;
      return this;
    }

    public HiveSessionSnapshot build() {
      return new HiveSessionSnapshot(sessionHandleId, username, ipAddress, currentDatabase,
          overriddenConfigurations, addedJars, tempTableDefinitions,
          protocolVersion, creationTime, lastAccessTime);
    }
  }
}
