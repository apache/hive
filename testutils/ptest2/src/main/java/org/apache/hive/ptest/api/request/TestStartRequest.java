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
package org.apache.hive.ptest.api.request;

public class TestStartRequest {
  private String profile;
  private String testHandle;
  private String patchURL;
  private String jiraName;
  private boolean clearLibraryCache;

  public TestStartRequest() {

  }
  public TestStartRequest(String profile, String testHandle,
      String jiraName, String patchURL, boolean clearLibraryCache) {
    this.profile = profile;
    this.testHandle = testHandle;
    this.jiraName = jiraName;
    this.patchURL = patchURL;
    this.clearLibraryCache = clearLibraryCache;
  }
  public String getProfile() {
    return profile;
  }
  public void setProfile(String profile) {
    this.profile = profile;
  }
  public String getPatchURL() {
    return patchURL;
  }
  public void setPatchURL(String patchURL) {
    this.patchURL = patchURL;
  }
  public boolean isClearLibraryCache() {
    return clearLibraryCache;
  }
  public void setClearLibraryCache(boolean clearLibraryCache) {
    this.clearLibraryCache = clearLibraryCache;
  }
  public String getJiraName() {
    return jiraName;
  }
  public void setJiraName(String jiraName) {
    this.jiraName = jiraName;
  }

  public String getTestHandle() {
    return testHandle;
  }
  public void setTestHandle(String testHandle) {
    this.testHandle = testHandle;
  }
  @Override
  public String toString() {
    return "TestStartRequest [profile=" + profile + ", testHandle="
        + testHandle + ", patchURL=" + patchURL + ", jiraName=" + jiraName
        + ", clearLibraryCache=" + clearLibraryCache + "]";
  }
}
