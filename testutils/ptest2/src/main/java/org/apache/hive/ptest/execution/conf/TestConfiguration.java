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
package org.apache.hive.ptest.execution.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

public class TestConfiguration {
  public static final String REPOSITORY = "repository";
  public static final String REPOSITORY_NAME = "repositoryName";
  public static final String BRANCH = "branch";
  public static final String JAVA_HOME = "javaHome";
  public static final String JAVA_HOME_TEST = "javaHomeForTests";
  public static final String ANT_ENV_OPTS = "antEnvOpts";
  public static final String ANT_TEST_ARGS = "antTestArgs";
  public static final String ANT_TEST_TARGET = "antTestTarget";

  private static final String REPOSITORY_TYPE = "repositoryType";
  private static final String GIT = "git";
  private static final String SVN = "svn";
  private static final String ANT = "ant";
  private static final String MAVEN = "maven";
  private static final String MAVEN_ENV_OPTS = "mavenEnvOpts";
  private static final String MAVEN_ARGS = "mavenArgs";
  private static final String MAVEN_BUILD_ARGS = "mavenBuildArgs";
  private static final String MAVEN_TEST_ARGS = "mavenTestArgs";
  private static final String ANT_ARGS = "antArgs";
  private static final String JIRA_URL = "jiraUrl";
  private static final String JIRA_USER = "jiraUser";
  private static final String JIRA_PASSWORD = "jiraPassword";
  private static final String JENKINS_URL = "jenkinsURL";
  private static final String TEST_CASE_PROPERTY_NAME = "testCasePropertyName";
  private static final String BUILD_TOOL = "buildTool";
  
  private final Context context;
  private String antArgs;
  private String antTestArgs;
  private String antEnvOpts;
  private String antTestTarget;
  private String mavenArgs;
  private String mavenBuildArgs;
  private String mavenTestArgs;
  private String mavenEnvOpts;
  private String repositoryType;
  private String repository;
  private String repositoryName;
  private String patch;
  private String javaHome;
  private String javaHomeForTests;
  private String branch;
  private final String jenkinsURL;
  private final String jiraUrl;
  private final String jiraUser;
  private final String jiraPassword;
  private final String testCasePropertyName;
  private final String buildTool;
  
  private String jiraName;
  private boolean clearLibraryCache;

  @VisibleForTesting
  public TestConfiguration(Context context, Logger logger)
      throws IOException {
    this.context = context;
    repositoryType = context.getString(REPOSITORY_TYPE, GIT).trim();
    repository =  Preconditions.checkNotNull(context.getString(REPOSITORY), REPOSITORY).trim();
    repositoryName =  Preconditions.checkNotNull(context.getString(REPOSITORY_NAME), REPOSITORY_NAME).trim();
    if(GIT.equals(repositoryType)) {
      branch =  Preconditions.checkNotNull(context.getString(BRANCH), BRANCH).trim();
    } else if(SVN.equals(repositoryType)) {
      branch = Strings.nullToEmpty(null);
    } else {
      throw new IllegalArgumentException("Unkown repository type '" + repositoryType + "'");
    }
    buildTool = context.getString(BUILD_TOOL, ANT).trim();
    if(!(MAVEN.endsWith(buildTool) || ANT.equals(buildTool))) {
      throw new IllegalArgumentException("Unkown build tool type '" + buildTool + "'");
    }
    antArgs =  context.getString(ANT_ARGS, "").trim();
    antTestArgs =  context.getString(ANT_TEST_ARGS, "").trim();
    antEnvOpts =  context.getString(ANT_ENV_OPTS, "").trim();
    antTestTarget = context.getString(ANT_TEST_TARGET, "test").trim();
    mavenArgs = context.getString(MAVEN_ARGS, "").trim();
    mavenBuildArgs = context.getString(MAVEN_BUILD_ARGS, "").trim();
    mavenTestArgs =  context.getString(MAVEN_TEST_ARGS, "").trim();
    mavenEnvOpts =  context.getString(MAVEN_ENV_OPTS, "").trim();
    javaHome =  context.getString(JAVA_HOME, "").trim();
    javaHomeForTests = context.getString(JAVA_HOME_TEST, "").trim();
    patch = Strings.nullToEmpty(null);
    jiraName = Strings.nullToEmpty(null);
    jiraUrl = context.getString(JIRA_URL, "").trim();
    jiraUser = context.getString(JIRA_USER, "").trim();
    jiraPassword = context.getString(JIRA_PASSWORD, "").trim();
    jenkinsURL = context.getString(JENKINS_URL, "https://builds.apache.org/job").trim();
    testCasePropertyName = context.getString(TEST_CASE_PROPERTY_NAME, "testcase").trim();
  }
  public Context getContext() {
    return context;
  }
  public String getJenkinsURL() {
    return jenkinsURL;
  }
  public String getJiraName() {
    return jiraName;
  }
  public void setJiraName(String jiraName) {
    this.jiraName = Strings.nullToEmpty(jiraName);
  }
  public boolean isClearLibraryCache() {
    return clearLibraryCache;
  }
  public void setClearLibraryCache(boolean clearLibraryCache) {
    this.clearLibraryCache = clearLibraryCache;
  }
  public String getBuildTool() {
    return buildTool;
  }
  public String getJiraUrl() {
    return jiraUrl;
  }
  public String getJiraUser() {
    return jiraUser;
  }
  public String getJiraPassword() {
    return jiraPassword;
  }
  public String getRepositoryType() {
    return repositoryType;
  }
  public String getRepositoryName() {
    return repositoryName;
  }
  public String getRepository() {
    return repository;
  }
  public String getBranch() {
    return branch;
  }
  public String getAntArgs() {
    return antArgs;
  }
  public String getAntTestArgs() {
    return antTestArgs;
  }
  public String getAntEnvOpts() {
    return antEnvOpts;
  }
  public String getAntTestTarget() {
    return antTestTarget;
  }
  public String getMavenArgs() {
    return mavenArgs;
  }
  public String getMavenBuildArgs() {
    return mavenBuildArgs;
  }
  public String getMavenTestArgs() {
    return mavenTestArgs;
  }
  public String getMavenEnvOpts() {
    return mavenEnvOpts;
  }
  public String getJavaHome() {
    return javaHome;
  }
  public String getJavaHomeForTests() {
    return javaHomeForTests;
  }
  public String getPatch() {
    return patch;
  }
  public String getTestCasePropertyName() {
    return testCasePropertyName;
  }
  public void setPatch(String patch) {
    this.patch = Strings.nullToEmpty(patch);
  }
  public void setRepository(String repository) {
    this.repository = Strings.nullToEmpty(repository);
  }
  public void setRepositoryName(String repositoryName) {
    this.repositoryName = Strings.nullToEmpty(repositoryName);
  }
  public void setBranch(String branch) {
    this.branch = Strings.nullToEmpty(branch);
  }
  public void setJavaHome(String javaHome) {
    this.javaHome = Strings.nullToEmpty(javaHome);
  }
  public void setJavaHomeForTests(String javaHomeForTests) {
      this.javaHomeForTests = javaHomeForTests;
  }
  public void setAntArgs(String antArgs) {
    this.antArgs = Strings.nullToEmpty(antArgs);
  }
  public void setAntTestArgs(String antTestArgs) {
    this.antTestArgs = antTestArgs;
  }
  public void setAntEnvOpts(String antEnvOpts) {
    this.antEnvOpts = Strings.nullToEmpty(antEnvOpts);
  }
  public void setAntTestTarget(String antTestTarget) {
    this.antTestTarget = Strings.nullToEmpty(antTestTarget);
  }
  public void setMavenArgs(String mavenArgs) {
    this.mavenArgs = Strings.nullToEmpty(mavenArgs);
  }
  public void setMavenTestArgs(String mavenTestArgs) {
    this.mavenTestArgs = mavenTestArgs;
  }
  public void setMavenEnvOpts(String mavenEnvOpts) {
    this.mavenEnvOpts = Strings.nullToEmpty(mavenEnvOpts);
  }
  @Override
  public String toString() {
    return "TestConfiguration [antArgs=" + antArgs + ", antTestArgs="
        + antTestArgs + ", antEnvOpts=" + antEnvOpts + ", antTestTarget="
        + antTestTarget + ", mavenArgs=" + mavenArgs + ", mavenTestArgs="
        + mavenTestArgs + ", mavenEnvOpts=" + mavenEnvOpts
        + ", repositoryType=" + repositoryType + ", repository=" + repository
        + ", repositoryName=" + repositoryName + ", patch=" + patch
        + ", javaHome=" + javaHome + ", javaHomeForTests=" + javaHomeForTests
        + ", branch=" + branch + ", jenkinsURL=" + jenkinsURL + ", jiraUrl="
        + jiraUrl + ", jiraUser=" + jiraUser + ", jiraPassword=" + jiraPassword
        + ", testCasePropertyName=" + testCasePropertyName + ", buildTool="
        + buildTool + ", jiraName=" + jiraName + ", clearLibraryCache="
        + clearLibraryCache + "]";
  }
  public static TestConfiguration fromInputStream(InputStream inputStream, Logger logger)
      throws IOException {
    Properties properties = new Properties();
    properties.load(inputStream);
    Context context = new Context(Maps.fromProperties(properties));
    return new TestConfiguration(context, logger);
  }
  public static TestConfiguration fromFile(String file, Logger logger) throws IOException {
    return fromFile(new File(file), logger);
  }
  public static TestConfiguration fromFile(File file, Logger logger) throws IOException {
    InputStream in = new FileInputStream(file);
    try {
      return fromInputStream(in, logger);
    } finally {
      in.close();
    }
  }
}
