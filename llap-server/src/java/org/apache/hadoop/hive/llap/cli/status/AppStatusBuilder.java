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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Represents the status of the Llap application.
 */
class AppStatusBuilder {

  private AmInfo amInfo;
  private State state = State.UNKNOWN;
  private String diagnostics;
  private String originalConfigurationPath;
  private String generatedConfigurationPath;

  private Long appStartTime;
  private Long appFinishTime;

  private boolean runningThresholdAchieved = false;

  private Integer desiredInstances = null;
  private Integer liveInstances = null;
  private Integer launchingInstances = null;

  private final List<LlapInstance> runningInstances = new LinkedList<>();
  private final List<LlapInstance> completedInstances = new LinkedList<>();

  private final transient Map<String, LlapInstance> containerToRunningInstanceMap = new HashMap<>();
  private final transient Map<String, LlapInstance> containerToCompletedInstanceMap = new HashMap<>();

  void setAmInfo(AmInfo amInfo) {
    this.amInfo = amInfo;
  }

  AppStatusBuilder setState(State state) {
    this.state = state;
    return this;
  }

  AppStatusBuilder setDiagnostics(String diagnostics) {
    this.diagnostics = diagnostics;
    return this;
  }

  AppStatusBuilder setOriginalConfigurationPath(String originalConfigurationPath) {
    this.originalConfigurationPath = originalConfigurationPath;
    return this;
  }

  AppStatusBuilder setGeneratedConfigurationPath(String generatedConfigurationPath) {
    this.generatedConfigurationPath = generatedConfigurationPath;
    return this;
  }

  AppStatusBuilder setAppStartTime(long appStartTime) {
    this.appStartTime = appStartTime;
    return this;
  }

  AppStatusBuilder setAppFinishTime(long finishTime) {
    this.appFinishTime = finishTime;
    return this;
  }

  void setRunningThresholdAchieved(boolean runningThresholdAchieved) {
    this.runningThresholdAchieved = runningThresholdAchieved;
  }

  AppStatusBuilder setDesiredInstances(int desiredInstances) {
    this.desiredInstances = desiredInstances;
    return this;
  }

  AppStatusBuilder setLiveInstances(int liveInstances) {
    this.liveInstances = liveInstances;
    return this;
  }

  AppStatusBuilder setLaunchingInstances(int launchingInstances) {
    this.launchingInstances = launchingInstances;
    return this;
  }

  AppStatusBuilder addNewRunningLlapInstance(LlapInstance llapInstance) {
    this.runningInstances.add(llapInstance);
    this.containerToRunningInstanceMap
        .put(llapInstance.getContainerId(), llapInstance);
    return this;
  }

  LlapInstance removeAndGetRunningLlapInstanceForContainer(String containerIdString) {
    return containerToRunningInstanceMap.remove(containerIdString);
  }

  void clearRunningLlapInstances() {
    this.runningInstances.clear();
    this.containerToRunningInstanceMap.clear();
  }

  AppStatusBuilder clearAndAddPreviouslyKnownRunningInstances(List<LlapInstance> llapInstances) {
    clearRunningLlapInstances();
    for (LlapInstance llapInstance : llapInstances) {
      addNewRunningLlapInstance(llapInstance);
    }
    return this;
  }

  @JsonIgnore
  List<LlapInstance> allRunningInstances() {
    return this.runningInstances;
  }

  AppStatusBuilder addNewCompleteLlapInstance(LlapInstance llapInstance) {
    this.completedInstances.add(llapInstance);
    this.containerToCompletedInstanceMap
        .put(llapInstance.getContainerId(), llapInstance);
    return this;
  }

  LlapInstance removeAndGetCompletedLlapInstanceForContainer(String containerIdString) {
    return containerToCompletedInstanceMap.remove(containerIdString);
  }

  void clearCompletedLlapInstances() {
    this.completedInstances.clear();
    this.containerToCompletedInstanceMap.clear();
  }

  AppStatusBuilder clearAndAddPreviouslyKnownCompletedInstances(List<LlapInstance> llapInstances) {
    clearCompletedLlapInstances();
    for (LlapInstance llapInstance : llapInstances) {
      addNewCompleteLlapInstance(llapInstance);
    }
    return this;
  }

  @JsonIgnore
  List<LlapInstance> allCompletedInstances() {
    return this.completedInstances;
  }

  AmInfo getAmInfo() {
    return amInfo;
  }

  State getState() {
    return state;
  }

  String getDiagnostics() {
    return diagnostics;
  }

  String getOriginalConfigurationPath() {
    return originalConfigurationPath;
  }

  String getGeneratedConfigurationPath() {
    return generatedConfigurationPath;
  }

  Long getAppStartTime() {
    return appStartTime;
  }

  Long getAppFinishTime() {
    return appFinishTime;
  }

  boolean isRunningThresholdAchieved() {
    return runningThresholdAchieved;
  }

  Integer getDesiredInstances() {
    return desiredInstances;
  }

  Integer getLiveInstances() {
    return liveInstances;
  }

  Integer getLaunchingInstances() {
    return launchingInstances;
  }

  List<LlapInstance> getRunningInstances() {
    return runningInstances;
  }

  List<LlapInstance> getCompletedInstances() {
    return completedInstances;
  }

  @JsonIgnore
  AmInfo maybeCreateAndGetAmInfo() {
    if (amInfo == null) {
      amInfo = new AmInfo();
    }
    return amInfo;
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
  }
}
