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

import org.apache.hadoop.hive.llap.cli.LlapStatusServiceDriver;
import org.codehaus.jackson.annotate.JsonIgnore;

public class LlapStatusHelpers {
  public enum State {
    APP_NOT_FOUND, LAUNCHING,
    RUNNING_PARTIAL,
    RUNNING_ALL, COMPLETE, UNKNOWN
  }

  public static class AmInfo {
    private String appName;
    private String appType;
    private String appId;
    private String containerId;
    private String hostname;
    private String amWebUrl;

    public AmInfo setAppName(String appName) {
      this.appName = appName;
      return this;
    }

    public AmInfo setAppType(String appType) {
      this.appType = appType;
      return this;
    }

    public AmInfo setAppId(String appId) {
      this.appId = appId;
      return this;
    }

    public AmInfo setContainerId(String containerId) {
      this.containerId = containerId;
      return this;
    }

    public AmInfo setHostname(String hostname) {
      this.hostname = hostname;
      return this;
    }

    public AmInfo setAmWebUrl(String amWebUrl) {
      this.amWebUrl = amWebUrl;
      return this;
    }

    public String getAppName() {
      return appName;
    }

    public String getAppType() {
      return appType;
    }

    public String getAppId() {
      return appId;
    }

    public String getContainerId() {
      return containerId;
    }

    public String getHostname() {
      return hostname;
    }

    public String getAmWebUrl() {
      return amWebUrl;
    }

    @Override
    public String toString() {
      return "AmInfo{" +
          "appName='" + appName + '\'' +
          ", appType='" + appType + '\'' +
          ", appId='" + appId + '\'' +
          ", containerId='" + containerId + '\'' +
          ", hostname='" + hostname + '\'' +
          ", amWebUrl='" + amWebUrl + '\'' +
          '}';
    }
  }

  public static class LlapInstance {
    private final String hostname;
    private final String containerId;
    private String logUrl;

    // Only for live instances.
    private String statusUrl;
    private String webUrl;
    private Integer rpcPort;
    private Integer mgmtPort;
    private Integer  shufflePort;

    // For completed instances
    private String diagnostics;
    private int yarnContainerExitStatus;

    // TODO HIVE-13454 Add additional information such as #executors, container size, etc

    public LlapInstance(String hostname, String containerId) {
      this.hostname = hostname;
      this.containerId = containerId;
    }

    public LlapInstance setLogUrl(String logUrl) {
      this.logUrl = logUrl;
      return this;
    }

    public LlapInstance setWebUrl(String webUrl) {
      this.webUrl = webUrl;
      return this;
    }

    public LlapInstance setStatusUrl(String statusUrl) {
      this.statusUrl = statusUrl;
      return this;
    }

    public LlapInstance setRpcPort(int rpcPort) {
      this.rpcPort = rpcPort;
      return this;
    }

    public LlapInstance setMgmtPort(int mgmtPort) {
      this.mgmtPort = mgmtPort;
      return this;
    }

    public LlapInstance setShufflePort(int shufflePort) {
      this.shufflePort = shufflePort;
      return this;
    }

    public LlapInstance setDiagnostics(String diagnostics) {
      this.diagnostics = diagnostics;
      return this;
    }

    public LlapInstance setYarnContainerExitStatus(int yarnContainerExitStatus) {
      this.yarnContainerExitStatus = yarnContainerExitStatus;
      return this;
    }

    public String getHostname() {
      return hostname;
    }

    public String getLogUrl() {
      return logUrl;
    }

    public String getStatusUrl() {
      return statusUrl;
    }

    public String getContainerId() {
      return containerId;
    }

    public String getWebUrl() {
      return webUrl;
    }

    public Integer getRpcPort() {
      return rpcPort;
    }

    public Integer getMgmtPort() {
      return mgmtPort;
    }

    public Integer getShufflePort() {
      return shufflePort;
    }

    public String getDiagnostics() {
      return diagnostics;
    }

    public int getYarnContainerExitStatus() {
      return yarnContainerExitStatus;
    }

    @Override
    public String toString() {
      return "LlapInstance{" +
          "hostname='" + hostname + '\'' +
          "logUrl=" + logUrl + '\'' +
          ", containerId='" + containerId + '\'' +
          ", statusUrl='" + statusUrl + '\'' +
          ", webUrl='" + webUrl + '\'' +
          ", rpcPort=" + rpcPort +
          ", mgmtPort=" + mgmtPort +
          ", shufflePort=" + shufflePort +
          ", diagnostics=" + diagnostics +
          ", yarnContainerExitStatus=" + yarnContainerExitStatus +
          '}';
    }
  }

  public static final class AppStatusBuilder {

    private AmInfo amInfo;
    private State state = State.UNKNOWN;
    private String diagnostics;
    private String originalConfigurationPath;
    private String generatedConfigurationPath;

    private Integer desiredInstances = null;
    private Integer liveInstances = null;
    private Integer launchingInstances = null;


    private Long appStartTime;
    private Long appFinishTime;

    private boolean runningThresholdAchieved = false;

    private final List<LlapInstance> runningInstances = new LinkedList<>();
    private final List<LlapInstance> completedInstances = new LinkedList<>();

    private transient final Map<String, LlapInstance>
        containerToRunningInstanceMap = new HashMap<>();
    private transient final Map<String, LlapInstance>
        containerToCompletedInstanceMap = new HashMap<>();

    public void setAmInfo(AmInfo amInfo) {
      this.amInfo = amInfo;
    }

    public AppStatusBuilder setState(
        State state) {
      this.state = state;
      return this;
    }

    public AppStatusBuilder setDiagnostics(String diagnostics) {
      this.diagnostics = diagnostics;
      return this;
    }

    public AppStatusBuilder setOriginalConfigurationPath(String originalConfigurationPath) {
      this.originalConfigurationPath = originalConfigurationPath;
      return this;
    }

    public AppStatusBuilder setGeneratedConfigurationPath(String generatedConfigurationPath) {
      this.generatedConfigurationPath = generatedConfigurationPath;
      return this;
    }

    public AppStatusBuilder setAppStartTime(long appStartTime) {
      this.appStartTime = appStartTime;
      return this;
    }

    public AppStatusBuilder setAppFinishTime(long finishTime) {
      this.appFinishTime = finishTime;
      return this;
    }

    public void setRunningThresholdAchieved(boolean runningThresholdAchieved) {
      this.runningThresholdAchieved = runningThresholdAchieved;
    }

    public AppStatusBuilder setDesiredInstances(int desiredInstances) {
      this.desiredInstances = desiredInstances;
      return this;
    }

    public AppStatusBuilder setLiveInstances(int liveInstances) {
      this.liveInstances = liveInstances;
      return this;
    }

    public AppStatusBuilder setLaunchingInstances(int launchingInstances) {
      this.launchingInstances = launchingInstances;
      return this;
    }

    public AppStatusBuilder addNewRunningLlapInstance(LlapInstance llapInstance) {
      this.runningInstances.add(llapInstance);
      this.containerToRunningInstanceMap
          .put(llapInstance.getContainerId(), llapInstance);
      return this;
    }

    public LlapInstance removeAndGetRunningLlapInstanceForContainer(String containerIdString) {
      return containerToRunningInstanceMap.remove(containerIdString);
    }

    public void clearRunningLlapInstances() {
      this.runningInstances.clear();
      this.containerToRunningInstanceMap.clear();
    }

    public AppStatusBuilder clearAndAddPreviouslyKnownRunningInstances(List<LlapInstance> llapInstances) {
      clearRunningLlapInstances();
      for (LlapInstance llapInstance : llapInstances) {
        addNewRunningLlapInstance(llapInstance);
      }
      return this;
    }

    @JsonIgnore
    public List<LlapInstance> allRunningInstances() {
      return this.runningInstances;
    }

    public AppStatusBuilder addNewCompleteLlapInstance(LlapInstance llapInstance) {
      this.completedInstances.add(llapInstance);
      this.containerToCompletedInstanceMap
          .put(llapInstance.getContainerId(), llapInstance);
      return this;
    }

    public LlapInstance removeAndGetCompletedLlapInstanceForContainer(String containerIdString) {
      return containerToCompletedInstanceMap.remove(containerIdString);
    }

    public void clearCompletedLlapInstances() {
      this.completedInstances.clear();
      this.containerToCompletedInstanceMap.clear();
    }

    public AppStatusBuilder clearAndAddPreviouslyKnownCompletedInstances(List<LlapInstance> llapInstances) {
      clearCompletedLlapInstances();
      for (LlapInstance llapInstance : llapInstances) {
        addNewCompleteLlapInstance(llapInstance);
      }
      return this;
    }

    @JsonIgnore
    public List<LlapInstance> allCompletedInstances() {
      return this.completedInstances;
    }

    public AmInfo getAmInfo() {
      return amInfo;
    }

    public State getState() {
      return state;
    }

    public String getDiagnostics() {
      return diagnostics;
    }

    public String getOriginalConfigurationPath() {
      return originalConfigurationPath;
    }

    public String getGeneratedConfigurationPath() {
      return generatedConfigurationPath;
    }

    public Integer getDesiredInstances() {
      return desiredInstances;
    }

    public Integer getLiveInstances() {
      return liveInstances;
    }

    public Integer getLaunchingInstances() {
      return launchingInstances;
    }

    public Long getAppStartTime() {
      return appStartTime;
    }

    public Long getAppFinishTime() {
      return appFinishTime;
    }

    public boolean isRunningThresholdAchieved() {
      return runningThresholdAchieved;
    }

    public List<LlapInstance> getRunningInstances() {
      return runningInstances;
    }

    public List<LlapInstance> getCompletedInstances() {
      return completedInstances;
    }

    @JsonIgnore
    public AmInfo maybeCreateAndGetAmInfo() {
      if (amInfo == null) {
        amInfo = new AmInfo();
      }
      return amInfo;
    }

    @Override
    public String toString() {
      return "AppStatusBuilder{" +
          "amInfo=" + amInfo +
          ", state=" + state +
          ", diagnostics=" + diagnostics +
          ", originalConfigurationPath='" + originalConfigurationPath + '\'' +
          ", generatedConfigurationPath='" + generatedConfigurationPath + '\'' +
          ", desiredInstances=" + desiredInstances +
          ", liveInstances=" + liveInstances +
          ", launchingInstances=" + launchingInstances +
          ", appStartTime=" + appStartTime +
          ", appFinishTime=" + appFinishTime +
          ", runningThresholdAchieved=" + runningThresholdAchieved +
          ", runningInstances=" + runningInstances +
          ", completedInstances=" + completedInstances +
          ", containerToRunningInstanceMap=" + containerToRunningInstanceMap +
          '}';
    }
  }
}
